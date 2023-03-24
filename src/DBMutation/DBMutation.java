package src.DBMutation;

import Graph.*;
import src.DBMutation.Generator.GeneratorController;
import org.json.JSONObject;
import org.json.JSONTokener;
import src.Other.DBConnection;
import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

public class DBMutation {
	private static final Scanner input = new Scanner(System.in).useDelimiter("\n");
	private static String query = "";
	private static Statement st = null;
	private static ResultSet rs = null;
	private final Graph graph;
	private final List<String> validTables;
	private final int insertThreshold = 350; //Max number of records per insert into table query (performance)
	private boolean augmenting = false;

	//Percentages for randomness in generators
	private int patternPerc, alterPerc, combinePerc, noMutationPerc, nullablePerc; //Percentages for randomness in generators

	//Percentages to mutation on augmentation
	private int percRandomValues, percCombineRecords;

	private final GeneratorController generatorController;

	public DBMutation(String DBMS, String sv, String user, String password, String db) {
		initPercentages();
		try {
			DBConnection.setConn(DBMS, sv, user, password, db);
			graph = new Graph(DBMS, DBConnection.getConn(), db);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		validTables = new ArrayList<>(graph.listTables());
		int i = 0;
		while(!validTables.isEmpty() && i < validTables.size()) {
			if (graph.getRecordNumberInTable(validTables.get(i)) == 0 ||
					graph.getPrimaryKeyNumberInTable(validTables.get(i)) == graph.getColumnNumberInTable(validTables.get(i))) {
				validTables.remove(i);
			} else ++i;
		}
		generatorController = new GeneratorController(db, graph, patternPerc, alterPerc, combinePerc, noMutationPerc, nullablePerc);

	}

	public boolean mutate() {
		boolean run = true;
		Pattern augmentationCommand = null;
		Pattern generator = null;
		try {
			DBConnection.getConn().createStatement().execute("Use " + graph.getName());
		} catch (SQLException e) {
			System.out.println("There was a problem with the database");
			return false;
		}

		while (run) {
			boolean codeError = false;
			System.out.println("Comandi:\n" +
					"\tAugmentation <AugmentationPercentage> <MutationPercentage>\n\t\t" +
					"Augments the database by<AugmentationPercentage>% and \n\t\tit will mutate <MutationPercentage>% of these\n" +
					"\t\t<MutationPercentage> cant be more than 100");
			System.out.println("or\n\tSelect a table:");
			for(String s : validTables)
				System.out.println("\t" + s);
			System.out.println("Type \"back\" to end the execution");

			String userInput = input.nextLine();

			if (userInput.equalsIgnoreCase("back")) break;
			boolean mutatingColumns = true;

			if (validTables.contains(userInput)) {
				String table = userInput;
				try {
					generatorController.addGenerator(table, DBConnection.getConn());
				}
				catch (Exception e) {
					mutatingColumns = false;
					System.out.println("There was a problem with the table " + table);
				}

				while(mutatingColumns) {
					System.out.println("Select a non-key column on which to perform mutations\n" +
							"or\nType \"GenerateRecords <incrementPercentage>\" to increment of number of records by <incrementPercentage>% for " + table + ":\n" +
							"Current number of records: " + graph.getRecordNumberInTable(table) + ";\n" +
							"ScrambleRecords <percentageOfRecords>: Shuffles non-key values of the percentage required. percentageOfRecords must be greater than 100 and positive;\n" +
							"ShiftRecords <percentualeRecords>: Shuffles non-key values of the percentage required. percentageOfRecords must be greater than 100 and positive.\n" +
							"Type \"back\" to return to table selection");
					int k = 0;
					List<String> validColumns = new ArrayList<>();
					System.out.println("\nSelectable columns:");
					for (Column c : graph.getColumnsInTable(table)) {
						if (!c.isPrimaryKey()) {
							System.out.print((k % 3 == 0 ? "\n" : "") + c.getName() + "; ");
							++k;
							validColumns.add(c.getName());
						}
					}
					System.out.println();
					userInput = input.nextLine();

					if (userInput.equalsIgnoreCase("back")) break;

					if (validColumns.contains(userInput)) {
						Column column = graph.searchColumnInTable(userInput, table);
						userInput = "";

						if (!column.isNullable()) {
							System.out.println("The only selectable mutation is: Modify: Modifies the values of " + column.getName() + " of n records;\nWould you like to perform it??");
							if (answer())
								modify(column, table);
						} else {
							boolean choosing = true;
							while (choosing) {
								boolean done = false;

								System.out.println("Select the mutation you would like to perform:\n" +
										"\tModify:  Modifies the values of " + column.getName() + " of n records;\n" +
										"\tBlank:  Fills null or empty values with new values;" +
										"Type \"back\" to return to column selection");

								userInput = input.nextLine();
								switch (userInput.toLowerCase()) {
									case "modify" -> {
										modify(column, table);
										done = true;
									}
									case "blank" -> {
										fillBlanks(column, table);
										done = true;
									}
									case "back" -> choosing = false;

									default -> System.out.println("You have to select one of the mutations");
								}
								if (done) {
									System.out.println("Would you like to perform other mutations to " + column.getName() + "?");
									if (!answer())
										choosing = false;
								}
							}
						}
					} else {
						if (userInput.contains("Generate")) {
							if (generator == null)
								generator = Pattern.compile("GenerateRecords \\d{1,3}");
							if (generator.matcher(userInput).matches()) {
								String[] split = userInput.split(" ");
								int howMany = Integer.parseInt(split[1]);
								try {
									generateRandomRecords(table, howMany);
								} catch (Exception e) {
									System.out.println("Error: " + e.getMessage());
									System.out.println("Mutation GenerateRecords stopped");
								}
							} else
								System.out.println("Error: Incorrect command GenerateRecords.");

						} else if (userInput.contains("ScrambleRecords")){
							Pattern scramble = Pattern.compile("ScrambleRecords \\d{1,3}");
							if (scramble.matcher(userInput).matches()) {
								String[] split = userInput.split(" ");
								int howMany = Integer.parseInt(split[1]);
								try {
									scrambleRecords(table, null, howMany);
								} catch (Exception e) {
									System.out.println("Error: " + e.getMessage());
									System.out.println("Mutation Scramble have failed");
								}
							}
						} else if (userInput.contains("ShiftRecords")) {
							Pattern shift = Pattern.compile("ShiftRecords \\d{1,3}");
							if (shift.matcher(userInput).matches()) {
								String[] split = userInput.split(" ");
								int howMany = Integer.parseInt(split[1]);
								try {
									shiftRecords(table, null, howMany);
								} catch (Exception e) {
									System.out.println("Error: " + e.getMessage());
									System.out.println("Mutation Shift have failed");
								}
							}
						} else System.out.println("Error: Column not present");
					}

					if (userInput.equalsIgnoreCase("back")) continue;

					System.out.println("Would you like to perform other mutations on the table " + table + "?");
					if (!answer())
						mutatingColumns = false;
				}
			} else codeError = true;

			if (userInput.equalsIgnoreCase("back")) continue;

			if (userInput.contains("Augmentation")) {
				if (augmentationCommand == null)
					augmentationCommand = Pattern.compile("Augmentation \\d{1,3} \\d{1,3}");
				if (augmentationCommand.matcher(userInput).matches()) {
					String[] split = userInput.split(" ");
					int percCopy = Integer.parseInt(split[1]);
					int percMutate = Integer.parseInt(split[2]);
					System.out.println(percCopy + "; " + percMutate);
					List<String> tables = new ArrayList<>(graph.sortTopological());
					List<String> failedTables = new ArrayList<>();
					if (percMutate >= 0 && percMutate <= 100) {
						if (percCopy > 0) {
							for (String t : tables) {
								try {
									generatorController.addGenerator(t, DBConnection.getConn());
								} catch (Exception e) {
									failedTables.add(t);
								}
							}
							tables.removeAll(failedTables);
							if (!tables.isEmpty()) {
								augmenting = true;
								while (percCopy > 0) {
									int copy = Math.min(percCopy, 100);
									percCopy -= copy;
									for (String table : tables) {
										try {
											augmentation(table, copy, percMutate);
										} catch (Exception e) {
											failedTables.add(table);
										}
									}
								}
								augmenting = false;
								for (String t : failedTables)
									System.out.println("Augmentation may have not been completed for table " + t);
							} else
								System.out.println("Augmentation failed");
						}
					}
					codeError = false;
				} else {
					System.out.println("Errore: Incorrect comnand Augmentation.");
					codeError = false;
				}
			}

			if (codeError)
				System.out.println("Error: unrecognized command or table");

			System.out.println("Would you like to perform more mutations?");
			if (!answer()) run = false;
		}
		System.out.println("End mutate");
		return true;
	}

	private String[][] records = null;
	private int recordsAdded;
	public void augmentation(String table, int percCopy, int percMutate) throws Exception {
		Random r = new Random();
		int howMany = Math.max(1, percCopy * graph.getRecordNumberInTable(table) / 100);
		int howManyToMutate = percMutate * howMany / 100;
		if (graph.getRecordNumberInTable(table) > 0) {

			if (howManyToMutate > 0)
				records = new String[howManyToMutate][graph.getColumnNumberInTable(table)];

			recordsAdded = 0;
			copyRecords(table, percCopy, percMutate);
			if (howManyToMutate > 0) {
				//Roulette selection:
				// 			[0, percCombineRecords) : scramble or shift will be used
				//			[percCombineRecords -> 100] : randomModify will be used
				if (graph.getColumnNumberInTable(table) == graph.getPrimaryKeyNumberInTable(table)
						|| r.nextInt(100) < percCombineRecords) {
					if (r.nextInt(100) % 2 == 0)
						scrambleRecords(table, records, howManyToMutate);
					else
						shiftRecords(table, records, howManyToMutate);

				} else { //percRandomValues
					int columnsToModifySize = graph.getColumnNumberInTable(table) - graph.getPrimaryKeyNumberInTable(table);
					if (columnsToModifySize != 0) {
						Column[] columnsToModify = new Column[columnsToModifySize];
						int count = 0;
						for (Column c : graph.getColumnsInTable(table)) {
							if (!c.isPrimaryKey()) {
								columnsToModify[count] = c;
								++count;
							}
						}
						try {
							randomModify(columnsToModify, records, table, howManyToMutate);
						} catch (Exception e) {
							System.out.println("Mutation modify stopped. There was a problem in generation");
							throw e;
						}
					}
				}
			}
		} else {
			try {
				generateRandomRecords(table, percCopy);
			} catch (Exception e) {
				System.out.println("Error: " + e.getMessage());
				throw e;
			}
		}
		records = null;
		recordsAdded = 0;
	}

	/**
	 * Copies percCopy records into the database. Of the copied records, percMutate will be stored in a matrix to be
	 * mutated later during augmentation.
	 * @param table	table to copy records from and copy records to
	 * @param percCopy	percentage of records to copy
	 * @param percMutate percentage of records to mutate
	 * @throws Exception Any Exception that could be thrown
	 */
	private void copyRecords(String table, int percCopy, int percMutate) throws Exception{

		int howMany = Math.max(1, percCopy * graph.getRecordNumberInTable(table)/100);
		int howManyToMutate = howMany * percMutate/100;
		String temp = "tempForMutate" + table;
		//Takes the records needed
		query = "SELECT * FROM " + table +" ORDER BY RAND() LIMIT " + howMany;
		Statement st = DBConnection.getConn().createStatement();
		ResultSet rs = st.executeQuery(query);
		List<Column> columns = graph.getColumnsInTable(table);

		//Prepare stringbuilder for insertion queries
		int count = 0;
		String queryFirstPart = createInsertionQueryPlaceholder(columns, table);
		StringBuilder queryCopy = null;
		String queryTempFirstPart = null;
		StringBuilder queryTemp = null;

		//prepare a matrix to contain copied records if needed
		//It wont be instanciated if all records will be mutated
		String[][] recordsToCopy = null;
		int remainingRecordsToCopy = 0;
		int toDivideCopy = 0;
		int recordsCopied = 0;
		if (howMany > howManyToMutate) {
			int toCopy = howMany - howManyToMutate;
			double divider = Math.max(1.0, (double)toCopy/insertThreshold);
			toDivideCopy = (int)(toCopy / divider);
			recordsToCopy = new String[toDivideCopy][columns.size()];
			remainingRecordsToCopy = toCopy - toDivideCopy;
			queryCopy =  new StringBuilder(queryFirstPart);
		}

		//Selects a random primary key not autoincrement. if it remains null then its only autoincrement or no keys
		//No checks for keys will be made
		Column randomPk = null;
		for(Column col : graph.getPrimaryKeysInTable(table)) {
			if (!col.isAutoIncrementing()) {
				randomPk = col;
				break;
			}
		}

		//Creates a temporary table where to insert records to mutate, if there is any primary key not-autoincrement
		//Since records to mutate will be stored in the global "records" variable, it could be
		//slow to check all keys inside it so records will be added in this table to
		//optimize duplicate keys research with the new ones using the DBMS.
		int toDivideMutate = howManyToMutate;
		int remainingRecordsToMutate = howManyToMutate;
		if (randomPk != null && howManyToMutate > 0) {
			double divider = Math.max(1.0, (double) howManyToMutate/insertThreshold);
			toDivideMutate = (int)(howManyToMutate / divider);
			queryTempFirstPart = "INSERT INTO " + temp +" " +  queryFirstPart.substring(queryFirstPart.indexOf("("));
			queryTemp = new StringBuilder(queryTempFirstPart);
			DBConnection.getConn().createStatement().execute("CREATE TABLE " + table +" LIKE " + table);
		}

		boolean checkMutated = true;
		int startToSearchFrom = 0;
		while(rs.next()) {
			String[] values = new String[columns.size()];

			//Takes each values of fields and copy them. If its a primary key then it already generate a new one
			for(Column c : columns) {
				int columnIndex = columns.indexOf(c);
				if (c.isAutoIncrementing())
					values[columnIndex] = "AUTO";
				else if (c.isPrimaryKey())
					values[columnIndex] = generatorController.generateValue(table, c.getName(), false);//checkEscapes(gen.generateValue(c.getName(), false));
				else
					values[columnIndex] = rs.getString(c.getName());

				if (values[columnIndex] != null)
					values[columnIndex] = checkEscapes(values[columns.indexOf(c)]);
				else values[columnIndex] = "null";
			}

			//Rows 400-481: check if the generated keys of the current record copied are already in the database, in the records variable
			//or in the recordsToCopy variable
			//Search for a record with the same value as the random primary key. If it find it then check all the keys for that record
			//if all keys match they will be all generated again
			boolean alreadyGenerated;
			int countFails = 0;
			do {
				alreadyGenerated = false;
				//Null -> autoIncrement -> no checks needed
				if (randomPk != null) {

					//If the keys are not in temp table or the original table
					if (!areKeysAlreadyIn(values, table, table) && (checkMutated || !areKeysAlreadyIn(values, table, temp))) {

						int columnIndex = columns.indexOf(randomPk); 	//Index of the field in the matrix
						int toMutateStartingIndex = startToSearchFrom;	//starting index for search in records variable. the index changes after a chunk of the records are added to the tenporary table
						int toCopyStartingIndex = 0;					//starting index for search in recordsToCopy variable
						boolean finished = false;
						do {
							int recordIndex = -1;
							boolean foundInCopied = false;
							//check the new key in records
							for (int i = toMutateStartingIndex; i < recordsAdded; ++i) {
								if (records[i][columnIndex].equals(values[columnIndex])) {
									if (graph.getPrimaryKeyNumberInTable(table) == 1)
										alreadyGenerated = true;
									else {
										recordIndex = i;
										toMutateStartingIndex = i + 1;
									}
									break;
								}
								if (i == recordsAdded - 1) toMutateStartingIndex = recordsAdded;
							}
							//if its not in records then it checks the new key in recordsToCopy
							if (toMutateStartingIndex == recordsAdded) {
								for (int i = toCopyStartingIndex; i < recordsCopied; ++i) {
									if (recordsToCopy[i][columnIndex].equals(values[columnIndex])) {
										if (graph.getPrimaryKeyNumberInTable(table) == 1)
											alreadyGenerated = true;
										else {
											recordIndex = i;
											toCopyStartingIndex = i + 1;
											foundInCopied = true;
										}
										break;
									}
								}
							}

							//recordsIndex == -1 -> keys are unique
							if (recordIndex == -1) finished = true;
							else {
								//A key had the same value: it will check all the other keys of the record found
								List<Column> pKeys = graph.getPrimaryKeysInTable(table);

								//This gets "break" if the keys are unique
								for (int i = 0; i < pKeys.size(); ++i) {
									Column p = pKeys.get(i);
									if (i != columnIndex && !p.isAutoIncrementing()) {
										int secondColumnIndex = columns.indexOf(p);
										if (foundInCopied) {
											if (!recordsToCopy[recordIndex][secondColumnIndex].equals(values[secondColumnIndex]))
												break;
										} else if (!records[recordIndex][secondColumnIndex].equals(values[secondColumnIndex])) {
											break;
										}
									}
									if (i == pKeys.size() - 1) {
										//The new keys are already present somewhere
										alreadyGenerated = true;
										finished = true;
									}
								}
							}

						} while (!finished);

					} else
						alreadyGenerated = true;
				}
				if (alreadyGenerated) {

					//Generates new key and repeats the checks
					for(Column p : graph.getPrimaryKeysInTable(table)) {
						int columnIndex = columns.indexOf(p);
						String val = generatorController.generateValue(table, p.getName(), countFails > 100);
						val = checkEscapes(val);
						values[columnIndex] = val;
					}
					++countFails;
				}
			} while (alreadyGenerated);

			//adds records to mutate in the matrix and in the query for the temp table. If the query has toDivideMutate records
			//then they are added
			if (count < howManyToMutate){
				records[recordsAdded] = values;
				++recordsAdded;
				++count;
				if (randomPk != null) {
					queryTemp.append("(");
					for(Column c : columns) {
						if (!c.isAutoIncrementing()) {
							if (values[columns.indexOf(c)] == null) values[columns.indexOf(c)] = "null";
							if (values[columns.indexOf(c)].contains("CONVERT") || values[columns.indexOf(c)].equalsIgnoreCase("null"))
								queryTemp.append(values[columns.indexOf(c)]).append(",");
							else
								queryTemp.append("'").append(values[columns.indexOf(c)]).append("',");
						}
					}
					queryTemp = new StringBuilder(queryTemp.substring(0, queryTemp.length() - 1) + "),");
					if (recordsAdded % (toDivideMutate) == 0) {
						System.out.println(count);
						queryTemp = new StringBuilder(queryTemp.substring(0, queryTemp.length() - 1));
						DBConnection.getConn().createStatement().execute(queryTemp.toString());
						queryTemp = new StringBuilder(queryTempFirstPart);
						checkMutated = false;
						startToSearchFrom = recordsAdded;
						if (remainingRecordsToMutate < toDivideMutate)
							toDivideMutate = remainingRecordsToMutate;
						remainingRecordsToMutate -= toDivideMutate;
					}
				}
			} else {
				//Same for the recordsToMutate if count > howManyToMutate
				recordsToCopy[recordsCopied] = values;
				++recordsCopied;
				++count;
				queryCopy.append("(");
				for(Column c : columns) {
					if (!c.isAutoIncrementing()) {
						if (values[columns.indexOf(c)] == null) values[columns.indexOf(c)] = "null";
						if (values[columns.indexOf(c)].contains("CONVERT") || values[columns.indexOf(c)].equalsIgnoreCase("null"))
							queryCopy.append(values[columns.indexOf(c)]).append(",");
						else
							queryCopy.append("'").append(values[columns.indexOf(c)]).append("',");
					}
				}
				queryCopy = new StringBuilder(queryCopy.substring(0, queryCopy.length() - 1) + "),");
				if (recordsCopied == toDivideCopy) {
					queryCopy = new StringBuilder(queryCopy.substring(0, queryCopy.length() - 1));
					//System.out.println("copying " + recordsCopied + " records");
					DBConnection.getConn().createStatement().execute(queryCopy.toString());
					queryCopy = new StringBuilder(queryFirstPart);
					recordsCopied = 0;
					if (remainingRecordsToCopy < toDivideCopy)
						toDivideCopy = remainingRecordsToCopy;
					recordsToCopy = new String[toDivideCopy][columns.size()];
					remainingRecordsToCopy -= toDivideCopy;
				}
			}

			//updates fields totals and foreignvalues of generators
			for(Column c : columns) {
				if (!values[columns.indexOf(c)].equalsIgnoreCase("null"))
					generatorController.updateTotal(table, c.getName());

				if (c.isPrimaryKey() && !c.isAutoIncrementing())
					generatorController.updateForeignValues(table, c.getName(), values[columns.indexOf(c)]);
			}
		}
		try {
			if (howManyToMutate > 0 && randomPk != null)
				DBConnection.getConn().createStatement().execute("DROP TABLE " + temp);
		} catch (Exception e) {
			System.out.println("Error: Temporary table \"" + temp +"\" was not dropped");
		}
	}

	/**
	 * Shuffles records values. For each column, it selects a record to start to shuffle from and moves all values around until the last one
	 * that is moved to the starting position.
	 * @param table	table to scramble records to
	 * @param values records to scramble if it's called from Augmentation mutation
	 * @param howMany how many records to scramble
	 * @throws Exception any exception
	 */

	public void scrambleRecords(String table, String[][] values, int howMany) throws Exception{
		if (values == null)
			values = getNRecordsFrom(table, howMany);

		boolean scrambled = true;
		if (!(graph.getPrimaryKeyNumberInTable(table) == graph.getColumnNumberInTable(table))) {
			List<Column> columns = graph.getColumnsInTable(table);
			//Listof the indexes
			List<Integer> indexes = new ArrayList<>();
			for(int i = 0; i < howMany; ++i)
				indexes.add(i);

			Random r = new Random();
			for (Column c : columns) {
				if (!c.isPrimaryKey()) {
					//list to use as a "stack" to select where to move the next value
					List<Integer> alreadyScrambled = new ArrayList<>(indexes);

					//selects starting position
					int i = alreadyScrambled.remove(r.nextInt(alreadyScrambled.size()));
					int columnIndex = columns.indexOf(c);
					String prev = values[i][columnIndex];
					while (alreadyScrambled.size() > 0) {
						//selects where to move the value and what value gets moved next
						int j = alreadyScrambled.remove(r.nextInt(alreadyScrambled.size()));
						String temp = values[j][columnIndex];
						values[j][columnIndex] = prev;
						prev = temp;
					}
					//the last value gets moved to the starting position
					values[i][columnIndex] = prev;
				}
			}
		} else scrambled = false;

		//inserts the scrambled records into the db
		if (scrambled) {
			try{
				if (augmenting)
					insertRecords(table, values, howMany);
				else
					replaceRecords(table, values, howMany);
			} catch(Exception e) {
				System.out.println("Insertion or update stopped on mutation Scramble");
				throw e;
			}
		}
	}

	/**
	 * Shuffles records values. For each column, it will decide to move the entire column up or down and shift all values by
	 * how many records it wants.
	 * @param table
	 * @param values records to shuffle if it is called from Augmentation mutation
	 * @param howMany how many records to shift values
	 * @throws Exception Any Exception
	 */
	public void shiftRecords(String table, String[][] values, int howMany) throws Exception{
		if (howMany < 2) return;
		if (values == null)
			values = getNRecordsFrom(table, howMany);

		boolean shifting = true;
		//It won't shift primary keys
		if (!(graph.getPrimaryKeyNumberInTable(table) == graph.getColumnNumberInTable(table))) {
			List<Column> columns = graph.getColumnsInTable(table);
			Random r = new Random();
			int prev = 0;
			for (Column c : columns) {
				if (!c.isPrimaryKey()) {
					int number;
					do {
						//chooses a random value differnt from the previous, to avoid the same combination of values
						number = r.nextInt(howMany);
						if (r.nextInt() % 2 == 1) number = -number;
					} while (number == prev);
					prev = number;
					int columnIndex = columns.indexOf(c);
					String[] temp = new String[howMany];
					//for each record, moves by number positions the values in the column
					for (int i = 0; i < howMany; ++i) {
						if (!c.isAutoIncrementing()) {
							int j = (i + number) % howMany;
							temp[j] = values[i][columnIndex];
						}
					}
					//assign the columns value to each record
					for (int i = 0; i < howMany; ++i) {
						values[i][columnIndex] = temp[i];
					}
				}
			}
		} else shifting = false;

		if (shifting) {
			//inserts the scrambled records into the db
			try {
				if (augmenting)
					insertRecords(table, values, howMany);
				else
					replaceRecords(table, values, howMany);
			} catch(Exception e) {
				System.out.println("Insertion or update stopped on mutation Shift");
				throw e;
			}
		}
	}

	/**
	 * Inserts records into the Database.
	 * @param table table to insert records to
	 * @param values records to insert
	 * @param howMany how many records to insert
	 * @throws SQLException if the query goes wrong
	 */
	private void insertRecords(String table, String[][] values, int howMany) throws SQLException {
		int threshold = Math.min(howMany, insertThreshold);
		int remaining = howMany;
		List<Column> columns = graph.getColumnsInTable(table);
		String startInsertionQuery = createInsertionQueryPlaceholder(columns, table);
		StringBuilder insertionQuery = new StringBuilder(startInsertionQuery);
		for (int i = 0; i < howMany; ++i) {
			insertionQuery.append("(");
			for(Column c : columns) {
				int columnIndex = columns.indexOf(c);
				if (!c.isAutoIncrementing()) {
					if (values[i][columnIndex] == null) values[i][columnIndex] = "null";
					if (values[i][columnIndex].contains("CONVERT") || values[i][columnIndex].equalsIgnoreCase("null"))
						insertionQuery.append(values[i][columnIndex]).append(",");
					else
						insertionQuery.append("'").append(values[i][columnIndex]).append("',");
				}
			}
			insertionQuery = new StringBuilder(insertionQuery.substring(0, insertionQuery.length() - 1) + "),");

			if (i % (threshold - 1) == 0) {
				remaining -= threshold;
				if (remaining < threshold) threshold = remaining;
				insertionQuery = new StringBuilder(insertionQuery.substring(0, insertionQuery.length() - 1));
				DBConnection.getConn().createStatement().execute(insertionQuery.toString());
				insertionQuery = new StringBuilder(startInsertionQuery);
			}
		}
	}

	/**
	 * replaces records by inserting them with the "ON DUPLICATE KEY UPDATE"
	 * Since it's not known if it is used "innoDB" or "MyISAM" as the storage engine for the table, if the key is autoincrement
	 * then it will be used an Update for each record: <a href="https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html">For InnoDB, new key will be added and not updated</a>
	 * @param table table to update
	 * @param values records to update
	 * @param howMany how many records to update
	 * @throws SQLException
	 */
	private void replaceRecords(String table, String[][] values, int howMany) throws SQLException {

		int threshold = Math.min(howMany, insertThreshold);
		int remaining = howMany;


		List<Column> columns = graph.getColumnsInTable(table);
		boolean allAI = false;
		//checks if all primary key are autoincrementing
		for (Column c : columns) {
			if (c.isPrimaryKey() && c.isAutoIncrementing())  {
				allAI = true;
				break;
			}
		}
		if (!allAI) {
			//uses insert into on duplicate key update as the insertRecords method
			String startInsertionQuery = createInsertionQueryPlaceholder(columns, table);
			StringBuilder insertionQuery = new StringBuilder(startInsertionQuery);
			StringBuilder duplicateUpdate = new StringBuilder("ON DUPLICATE KEY UPDATE ");
			boolean duplicateUpdateDone = false;
			for (int i = 0; i < howMany; ++i) {
				insertionQuery.append("(");
				for (Column c : columns) {
					int columnIndex = columns.indexOf(c);
					if (!c.isAutoIncrementing()) {
						if (values[i][columnIndex] == null) values[i][columnIndex] = "null";
						if (values[i][columnIndex].contains("CONVERT") || values[i][columnIndex].equalsIgnoreCase("null"))
							insertionQuery.append(values[i][columnIndex]).append(",");
						else
							insertionQuery.append("'").append(values[i][columnIndex]).append("',");

					}


				}
				insertionQuery = new StringBuilder(insertionQuery.substring(0, insertionQuery.length() - 1) + "),");

				if ((i + 1) % threshold == 0) {

					if (remaining < threshold)
						threshold = remaining;
					remaining -= threshold;
					insertionQuery = new StringBuilder(insertionQuery.substring(0, insertionQuery.length() - 1));
					if (!duplicateUpdateDone) {
						for (Column c : columns) {
							if (!c.isPrimaryKey())
								duplicateUpdate.append(c.getName()).append(" = VALUES(").append(c.getName()).append("),");
						}
						duplicateUpdate = new StringBuilder(duplicateUpdate.substring(0, duplicateUpdate.length() - 1)).append(" ");
						duplicateUpdateDone = true;
					}
					insertionQuery.append(duplicateUpdate);

					DBConnection.getConn().createStatement().execute(insertionQuery.toString());
					insertionQuery = new StringBuilder(startInsertionQuery);

				}
			}
		} else {
			//Uses an update for each record
			for (int i = 0; i < howMany; ++i) {
				query = "UPDATE " + table + " set ";
				for (Column c : columns) {
					if (!c.isPrimaryKey()) {
						int columnIndex = columns.indexOf(c);
						query += c.getName() + "=";
						if (values[i][columnIndex].contains("CONVERT") || values[i][columnIndex].equals("null"))
							query += values[i][columnIndex];
						else
							query += "'" + values[i][columnIndex] + "'";
						query += ",";
					}
				}
				query = query.substring(0, query.length() - 1) + " WHERE ";
				List<Column> columnsToCycle = graph.getPrimaryKeyNumberInTable(table) == 0 ? columns : graph.getPrimaryKeysInTable(table);
				for (Column c : columnsToCycle) {
					int columnIndex = columns.indexOf(c);
					query += c.getName() + "=";
					if (values[i][columnIndex].contains("CONVERT") || values[i][columnIndex].equals("null"))
						query += values[i][columnIndex];
					else
						query += "'" + values[i][columnIndex] + "'";
					query += " AND ";
				}
				query = query.substring(0, query.length() - 5);
				DBConnection.getConn().createStatement().executeUpdate(query);
			}
		}
	}

	/**
	 * Copies N records from a table
	 * @param table table to get records
	 * @param howMany how many records
	 * @return string matrix of copied records
	 * @throws Exception
	 */
	private String[][] getNRecordsFrom(String table, int howMany) throws Exception {
		List<Column> columns = graph.getColumnsInTable(table);
		String[][] values = new String[howMany][columns.size()];
		Statement st = DBConnection.getConn().createStatement();
		ResultSet rs = st.executeQuery("SELECT * FROM " + table + " ORDER BY RAND() LIMIT " + howMany);
		int i = 0;
		while(rs.next()) {
			for(Column c : columns) {
					values[i][columns.indexOf(c)] = rs.getString(c.getName());
				if (values[i][columns.indexOf(c)] != null)
					values[i][columns.indexOf(c)] = checkEscapes(values[i][columns.indexOf(c)]);
				else values[i][columns.indexOf(c)] = "null";
			}
			++i;
		}
		return values;
	}

	/**
	 * Creates an insertion query without values like: "INSERT INTO table (fields to be inserted) VALUES"
	 * @param columns columns to insert
	 * @param table	table to insert into
	 * @return insertion query without values
	 */
	private String createInsertionQueryPlaceholder(List<Column> columns, String table) {
		StringBuilder insertQuery = new StringBuilder("INSERT INTO " + table + " (");
		for (Column column : columns) {
			if (!column.isAutoIncrementing())
				insertQuery.append(column.getName()).append(",");
		}
		insertQuery = new StringBuilder(insertQuery.substring(0, insertQuery.length() - 1) + ") VALUES");

		return insertQuery.toString();
	}

	/**
	 * Method that handles the blank mutation
	 * @param column column to mutate
	 * @param table table the column is in
	 */
	public void fillBlanks(Column column, String table) {
		int n_blank = -1;
		switch(column.getDatatype()) {
			case Types.CHAR, Types.NCHAR, Types.LONGNVARCHAR, Types.VARCHAR, Types.NVARCHAR ->
				query = "SELECT COUNT(*) FROM " + table + "" +
						" WHERE " + column.getName() + " IS NULL OR " + column.getName() + "=''";
			default ->
				query = "SELECT COUNT(*) FROM " + table + " WHERE " + column.getName() + " IS NULL";
		}
		try {
			st = DBConnection.getConn().createStatement();
			rs = st.executeQuery(query);
			if(rs.next())
				n_blank = rs.getInt(1);
		}catch(SQLException se) {
			System.out.println("Error: "+se.getMessage());
			System.out.println("Mutation blank aborted");
			n_blank = -1;
		}finally {
			DBConnection.closeSt(st);
			DBConnection.closeRs(rs);
		}
		if(n_blank == 0) {
			System.out.println("There isnt a null or empty value in: "+column.getName());
		}else if(n_blank > 0) {
			fillBlanksWithRandom(column,table, n_blank);
		}
	}

	/**
	 * Fills null or empty values with new values
	 * @param column column to mutate
	 * @param table table the column is in
	 * @param n_blank how many blank values there is in column
	 */
	private void fillBlanksWithRandom(Column column, String table, int n_blank) {
		Random r = new Random();
		int remaining = n_blank;
		String pKeys = graph.getPrimaryKeysStringInTable(table, ',', "", "");
		int n = 0;
		String val = "";
		while(remaining > 0) {
			try {
				n = r.nextInt(remaining) + 1;
				val = "";
				remaining -= n;

				val = generatorController.generateValue(table, column.getName(), false);
				query = "UPDATE " + table + " SET " + column.getName() + "= '" + val + "'" +
						" WHERE " + (graph.isPrimaryKeyComposedInTable(table) ? "(" + pKeys + ")" : pKeys) + " IN (" +
						"SELECT " + pKeys + " FROM ( " +
						"SELECT " + pKeys + " FROM " + table + " " +
						" WHERE " + column.getName() + " IS NULL OR " + column.getName() + "= ''" +
						"ORDER BY RAND() LIMIT " + n +") t)";

				Statement st = DBConnection.getConn().createStatement();
				st.executeUpdate(query);

			} catch(Exception e) {
				System.out.println("Error: " + e.getMessage());
				System.out.println("Mutation blank stopped");
			}
		}
	}

	/**
	 * Method that guides the user to the modify mutations
	 * @param column column to mutate
	 * @param table table the column is in
	 */
	private void modify(Column column, String table) {
		switch(column.getDatatype()) {
			case Types.NUMERIC, Types.INTEGER, Types.BIGINT, Types.SMALLINT, Types.DECIMAL,
					Types.FLOAT, Types.REAL, Types.TINYINT -> {
				if (column instanceof ForeignKeyColumn){
					System.out.println("Its a foreign key. The only mutation avaliable is: modify.");
					int howMany = askHowMany() * graph.getRecordNumberInTable(table)/100;
					try {
						randomModify(new Column[]{column}, null, table, howMany);
					} catch (Exception e) {
						System.out.println("Error: " + e.getMessage());
						System.out.println("Mutation modify aborted. There was a problem in generation");
					}
				} else {
					System.out.println("Select the type of mutation: (operation; random)");
					String type = input.nextLine();
					if (type.equalsIgnoreCase("operation"))
						randomOperation(column, table);
					else if (type.equalsIgnoreCase("random")) {
						int howMany = askHowMany() * graph.getRecordNumberInTable(table)/100;
						try {
							randomModify(new Column[]{column}, null, table, howMany);
						} catch (Exception e) {
							System.out.println("Error: " + e.getMessage());
							System.out.println("Mutation modify aborted. There was a problem in generation");
						}
					} else
						System.out.println("Invalid mutation");
				}
			}
			default -> {
				System.out.println((column instanceof ForeignKeyColumn ? "It's a foreign key: " : "") + "Avaliable mutation: random");
				int howMany = askHowMany() * graph.getRecordNumberInTable(table)/100;
				try {
					randomModify(new Column[]{column}, null, table, howMany);
				} catch (Exception e) {
					System.out.println("Error: " + e.getMessage());
					System.out.println("Mutation modify aborted. There was a problem in generation");
				}
			}
		}
	}

	/**
	 * Generates new values for each column selected.
	 * @param columnsToModify columns that are selected
	 * @param values records to modify if it's called from Augmentation mutation
	 * @param table table to mutate
	 * @param howMany how many records to mutate
	 * @throws Exception
	 */
	public void randomModify(Column[] columnsToModify, String[][] values, String table, int howMany) throws Exception{
		if (values == null)
			values = getNRecordsFrom(table, howMany);
		List<Column> columns = graph.getColumnsInTable(table);
		for(int i = 0; i < howMany; ++i) {
			for (Column column : columnsToModify) {
				if (!column.isAutoIncrementing() && !column.isPrimaryKey()) {
					int columnIndex = columns.indexOf(column);
					try {
						values[i][columnIndex] = generatorController.generateValue(table, column.getName(), false);//checkEscapes(gen.generateValue(column.getName(), false));
					} catch (Exception e) {
						if (column.isNullable()) values[i][columnIndex] = "null";
						else throw e;
					}
				}
			}
		}
		try {
			if (augmenting)
				insertRecords(table, values, howMany);
			else
				replaceRecords(table, values, howMany);
		} catch(Exception e) {
			System.out.println("Error: " + e.getMessage());
			System.out.println("Insertion or update stopped on mutation modify");
		}
	}

	/**
	 * Performs random addition or subractions to values in a column
	 * @param column column to mutate
	 * @param table table the column is in
	 */
	public void randomOperation(Column column, String table) {
		//Operazioni successive nella stessa esecuzione potrebbero avvenire sugli stessi records
		//Finche si è in una certa istanza di mutation, i lowerbound e upperbound dei campi numerici è sempre lo stesso
		Random r = new Random();
		int howMany = askHowMany();
		int total = howMany;
		char[] operations = {'+', '-'};
		int fails = 0;
		String pKeys = "";
		boolean b = false;
		if (graph.getPrimaryKeyNumberInTable(table) == 0) {
			b = true;
			for (Column c : graph.getColumnsInTable(table)){
				if (c.isNullable())
					pKeys += c.getName();
			}

		}else
			pKeys = graph.getPrimaryKeysStringInTable(table, ',', "", "");
		char operation =operations[r.nextInt(operations.length)];
		long operationBound = generatorController.getOperationBound(table, column.getName(), operation);
		double randomBound = (double) Math.abs(operationBound)/2;
		char conditionSign = switch(operation) {
			case '+' -> '<';
			case '-' -> '>';
			default -> throw new IllegalStateException("Unexpected value: " + operation);
		};
		try {
			while (howMany > 0) {
				double random = r.nextDouble(randomBound);
				int n = r.nextInt(howMany) + 1;
				//this might fail if all field are nullable and there is no primary key
				String cond = column.getName() + operation + random + conditionSign + operationBound;
				query = "UPDATE " + table + " SET " + column.getName() + "=" + column.getName() + operation + random + " " +
						"WHERE " + (graph.isPrimaryKeyComposedInTable(table) || b ? "(" + pKeys + ")" : pKeys) + " IN (" +
						"SELECT " + pKeys + " FROM ( " +
						"SELECT " + pKeys + " FROM " + table + " " +
						"WHERE " + cond +
						" ORDER BY RAND() LIMIT " + n +") t)";
				st = DBConnection.getConn().createStatement();
				st.executeUpdate(query);
				if (st.getUpdateCount() < n) {
					n = st.getUpdateCount();
				}
				if (n > 0) {
					howMany -= n;
					fails = 0;
				} else
					++fails;

				if (fails > 100) {
					System.out.println("No more values can be consistently updated: values mutated: " + (total - howMany));
					break;
				}
			}
		}catch(SQLException se) {
			System.out.println("Error: " + se.getMessage());
			System.out.println("Mutation stopped because of an error from the database");
		}finally {
			DBConnection.closeSt(st);
		}
	}

	/**
	 * Asks how much percentage of records to mutate.
	 * @return
	 */
	private int askHowMany(){
		System.out.println("Select the percentage of records that you want to mutate: ");
		int howMany ;
		do{
			try {
				howMany = Integer.parseInt(input.nextLine());
				if (howMany < 0 || (howMany > 100)) {
					System.out.println("Devi inserire un numero minore di 100 e non negativo!");
					howMany = -1;
				}
			} catch(NumberFormatException e){
				System.out.println("Devi inserire un numero!");
				howMany = -1;
			}
		} while(howMany == -1);
		return howMany;
	}

	/**
	 * Generates new records for the table
	 * @param table table to generate new records
	 * @param howManyPerc percentage of how many records to generate
	 * @throws Exception
	 */
	public void generateRandomRecords(String table, int howManyPerc) throws Exception {
		int threshold = insertThreshold;
		List<Column> columns = graph.getColumnsInTable(table);
		int howMany = Math.max(1, graph.getRecordNumberInTable(table) * howManyPerc / 100);
		String[] values = new String[columns.size()]; //Index of list are parallel to values array
		String[][] newRecords = new String[threshold][columns.size()];
		int newRecordsRowIndex = 0;

		//Generates the fields
		boolean onlyAIOrNoKeys = false;
		Column randomPk = null;
		for (int i = 0; i < howMany; ++i) {
			for(Column c : columns) {
				if (!onlyAIOrNoKeys && c.isPrimaryKey() && !c.isAutoIncrementing()) {
					randomPk = c;
					onlyAIOrNoKeys = true;
				}
				try {
					String val = generatorController.generateValue(table, c.getName(), false);
					values[columns.indexOf(c)] = val;
				} catch (Exception e) {
					if (c.isNullable()) values[columns.indexOf(c)] = "null";
					else throw e;
				}
			}

			//check for keys in matrix like copyrecords does
			boolean alreadyGenerated;
			int countFails = 0;
			do {
				alreadyGenerated = false;
				if (!onlyAIOrNoKeys) {
					if (!areKeysAlreadyIn(values, table, table)) {
						int columnIndex = columns.indexOf(randomPk);
						int toMutateStartingIndex = 0;
						boolean finished = false;
						do {
							int recordIndex = -1;

							for (int index = toMutateStartingIndex; index < newRecordsRowIndex; ++index) {

								if (records[index][columnIndex].equals(values[columnIndex])) {
									if (graph.getPrimaryKeyNumberInTable(table) == 1)
										alreadyGenerated = true;
									else {
										recordIndex = index;
										toMutateStartingIndex = index + 1;
									}
									break;
								}
								if (index == recordsAdded - 1) toMutateStartingIndex = recordsAdded;
							}
							if (recordIndex == -1) finished = true;
							else {
								List<Column> pKeys = graph.getPrimaryKeysInTable(table);
								for (int j = 0; j < pKeys.size(); ++j) {
									Column p = pKeys.get(j);
									if (j != columnIndex && !p.isAutoIncrementing()) {
										int secondColumnIndex = columns.indexOf(p);
										if (!newRecords[recordIndex][secondColumnIndex].equals(values[secondColumnIndex])) {
											break;
										}
									}
									if (j == pKeys.size() - 1) {
										alreadyGenerated = true;
										finished = true;
									}
								}
							}
						} while (!finished);

					} else
						alreadyGenerated = true;
				}
				if (alreadyGenerated) {
					for(Column p : graph.getPrimaryKeysInTable(table)) {
						int columnIndex = columns.indexOf(p);
						String val = generatorController.generateValue(table, p.getName(), countFails > 100);
						val = checkEscapes(val);
						values[columnIndex] = val;
					}
					++countFails;
				}
			} while (alreadyGenerated);
			newRecords[newRecordsRowIndex] = values;
			++newRecordsRowIndex;
			if (newRecordsRowIndex == threshold) {
				insertRecords(table, newRecords, newRecordsRowIndex);
				newRecords = new String[threshold][columns.size()];
				newRecordsRowIndex = 0;
			}
			for(Column c : columns) {
				if (!values[columns.indexOf(c)].equalsIgnoreCase("null"))
					generatorController.updateTotal(table, c.getName());
			}
		}
		graph.setRecordNumberInTable(table, graph.getRecordNumberInTable(table) + howMany);

	}

	/**
	 * Escapes the ' character to avoid problems in queries
	 * @param val
	 * @return
	 */
	private String checkEscapes(String val) {

		if (val.contains("CONVERT")) return val;
		String returnVal ="";
		if (val.contains("'")){
			for(int i = 0; i < val.length(); ++i){

				if (val.charAt(i) == '\''){
					returnVal +="\\" + val.charAt(i);
				} else returnVal += val.charAt(i);
			}

		} else return val;

		return returnVal;
	}

	/**
	 * Asks yes or no to the user
	 * @return true if yes, false if no
	 */
	private boolean answer() {
		String input ="";
		while(true) {
			input = DBMutation.input.nextLine();
			if(input.equalsIgnoreCase("yes")) return true;
			if (input.equalsIgnoreCase("no")) return false;

			System.out.println("You have to answer yes or no");
		}
	}
	/** Performs a query that checks whether the generated values of the primary keys are already present
	 *
	 * @param values record values
	 * @param table	 reference table for graph
	 * @param tableToCheck table to actually check the keys
	 * @return	true if the key combination is already in the table, false otherwise
	 * @throws Exception sqlesxeption
	 */
	private boolean areKeysAlreadyIn(String[] values, String table, String tableToCheck) {

		if (graph.getPrimaryKeyNumberInTable(table) == 0)
			return false;
		List<Column> columns = graph.getColumnsInTable(table);
		List<Column> primaryKeys = graph.getPrimaryKeysInTable(table);
		String primaryKeysQuery = "SELECT * FROM " + tableToCheck + " ";
		StringBuilder whereConditions = new StringBuilder("WHERE ");
		for (Column primaryKey : primaryKeys) {

			if (primaryKey.isAutoIncrementing()) {
				if (!graph.isPrimaryKeyComposedInTable(table))
					return false;
			} else {
				if (values[columns.indexOf(primaryKey)].equalsIgnoreCase("null"))
					whereConditions.append(primaryKey.getName()).append(" is ").append(values[columns.indexOf(primaryKey)]).append(" and ");
				else
					whereConditions.append(primaryKey.getName()).append("='").append(values[columns.indexOf(primaryKey)]).append("' and ");
			}
		}

		whereConditions = new StringBuilder(whereConditions.substring(0, whereConditions.length() - 5));

		primaryKeysQuery += whereConditions;
		try {
			ResultSet check = DBConnection.getConn().createStatement().executeQuery(primaryKeysQuery);
			return check.next();
		} catch (Exception e) {
			return true;
		}
	}

	public void initPercentages() {
		File file = new File("generator_files/percentages.json");
		if (!file.exists()) {
			alterPerc = 34;
			combinePerc = 33;
			noMutationPerc = 33;
			patternPerc = 50;
			nullablePerc = 20;
			percRandomValues = 50;
			percCombineRecords = 50;
			try {
				if (file.createNewFile()) {
					//Crea il file con le percentuali default
					JSONObject obj = new JSONObject();

					obj.put("alterPercentage", alterPerc);
					obj.put("combinePercentage", combinePerc);
					obj.put("noMutationPercentage", noMutationPerc);
					obj.put("patternPercentage", patternPerc);
					obj.put("nullablePercentage", nullablePerc);
					obj.put("percentageRandomValues", percRandomValues);
					obj.put("percentageCombineRecords", percCombineRecords);
					FileWriter fr = new FileWriter(file);
					fr.write(obj.toString(2));
					fr.close();
				}
			}catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			try {
				JSONObject j = new JSONObject(new JSONTokener(new FileReader(file)));
				alterPerc = Math.abs(j.getInt("alterPercentage"));
				combinePerc = Math.abs(j.getInt("combinePercentage"));
				noMutationPerc = Math.abs(j.getInt("noMutationPercentage"));
				if (alterPerc + combinePerc + noMutationPerc != 100) {
					throw new IllegalArgumentException("Generator percentages of randomness don't sum up to 100");
				}

				patternPerc = Math.abs(j.getInt("patternPercentage"));
				if (patternPerc > 100) patternPerc = 100;
				nullablePerc = Math.abs(j.getInt("nullablePercentage"));
				if (nullablePerc > 100) nullablePerc = 100;

				percRandomValues = Math.abs(j.getInt("percentageRandomValues"));
				percCombineRecords = Math.abs(j.getInt("percentageCombineRecords"));
				if (percCombineRecords + percRandomValues != 100) {
					throw new IllegalArgumentException("Augmentation percentages don't sum up to 100");
				}
			} catch (IOException e){
				e.printStackTrace();
				alterPerc = 34;
				combinePerc = 33;
				noMutationPerc = 33;
				patternPerc = 50;
				nullablePerc = 20;
				percRandomValues = 50;
				percCombineRecords = 50;
			} catch(IllegalArgumentException e2) {
				System.out.println(e2.getMessage());
				if (e2.getMessage().contains("Generator")) {
					alterPerc = 34;
					combinePerc = 33;
					noMutationPerc = 33;
				} else {
					percRandomValues = 50;
					percCombineRecords = 50;
				}
			}
		}
	}
}
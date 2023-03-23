package src.DBMutation;

import Graph.Column;
import Graph.ForeignKeyColumn;
import Graph.Graph;

import src.DBMutation.Generator.Generator;
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
	private static String risp="";
	private static String query="";
	private static Statement st = null;
	private static ResultSet rs = null;
	private final Graph graph;
	//private static Connection connection;
	private final List<String> validTables;
	private final int insertThreshold = 350;
	private boolean augmenting = false;
	private Map<String, Generator> generators;


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

			validTables = new ArrayList(graph.listTables());
			int i = 0;
			while(!validTables.isEmpty() && i < validTables.size()) {
				if (graph.getRecordNumberInTable(validTables.get(i)) == 0 ||
						graph.getPrimaryKeyNumberInTable(validTables.get(i)) == graph.getColumnNumberInTable(validTables.get(i))) {
					validTables.remove(i);
				} else ++i;
			}
			generatorController = new GeneratorController(db, graph, patternPerc, alterPerc, combinePerc, noMutationPerc, nullablePerc);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	//Metodo indipendente da DBSplit
	public DBMutation(Graph DBGraph){
		initPercentages();
		graph = DBGraph;
		validTables = graph.listTables();
		int i = 0;
		while(!validTables.isEmpty() && i < validTables.size()) {
			if (graph.getRecordNumberInTable(validTables.get(i)) == 0 ||
					graph.getPrimaryKeyNumberInTable(validTables.get(i)) == graph.getColumnNumberInTable(validTables.get(i))) {
				validTables.remove(i);
			} else ++i;
		}
		generatorController = new GeneratorController(graph.getName(), graph, patternPerc, alterPerc, combinePerc, noMutationPerc, nullablePerc);
	}
	public void mutate() {
		boolean run = true;
		Pattern augmentationCommand = null;
		Pattern generator = null;
		try {
			DBConnection.getConn().createStatement().execute("Use " + graph.getName());
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(1);
		}
		while (run) {
			boolean codeError = false;
			System.out.println("Comandi:\n" +
					"\tAugmentation <PercentualeAugmentation> <PercentualeMutation>\n\t\t" +
					"Effettua augmentation del DB del <PercentualeAugmentation>% e di questi\n\t\tne muta <PercentualeMutation>%\n" +
					"\t\t<PercentualeMutation> non deve essere > 100");
			System.out.println("Oppure\n\tSeleziona una Tabella:");
			for(String s : validTables)
				System.out.println("\t" + s);
			String userInput = input.nextLine();
			if (validTables.contains(userInput)) {
				String table = userInput;
				boolean mutatingColumns = true;
				while(mutatingColumns) {
					System.out.println("Seleziona la colonna non key per effettuare le mutazioni\n" +
							"oppure\ndigita \"GenerateRecords <percentualeAumento>\" per aumentare il numero di records del <percentualeAumento>% per " + table + ":\n" +
							"Numero records attuale: " + graph.getRecordNumberInTable(table) + ";\n" +
							"ScrambleRecords <percentualeRecords>: Mischia i valori di ogni campo non chiave per la percentuale di records di richiesta. percentualeRecords < 100 e positivo;\n" +
							"ShiftRecords <percentualeRecords>: Mischia i valori di ogni campo non chiave per la percentuale di records di richiesta. percentualeRecords < 100 e positivo;\n" +
							"Digita \"back\" per tornare alla selezione della tabella");

					int k = 0;
					List<String> validColumns = new ArrayList<>();
					System.out.println("\nColonne selezionabili:");
					for (Column c : graph.getColumnsInTable(table)) {
						if (!c.isPrimaryKey()) {
							System.out.print((k % 3 == 0 ? "\n" : "") + c.getName() + "; ");
							++k;
							validColumns.add(c.getName());
						}
					}
					userInput = input.nextLine();

					if (userInput.equalsIgnoreCase("back")) break;

					if (validColumns.contains(userInput)) {
						Column column = graph.searchColumnInTable(userInput, table);
						userInput = "";
						if (!column.isNullable()) {
							System.out.println("Unica operazione eseguibile: Modify: Modifica il valore di " + column.getName() + " di n records;\nVuoi eseguirla?");
							if (answer())
								modify(column, table);
						} else {
							boolean choosing = true;
							while (choosing) {
								boolean done = false;
								generatorController.addGenerator(table, DBConnection.getConn());
								System.out.println("Seleziona operazione da effettuare:\n" +
										"\tModify: Modifica il valore di " + column.getName() + " di n records;\n" +
										"\tBlank:  Riempie i valori nulli o vuoti con valori non nulli, se presenti;" +
										"Digita \"back\" per tornare alla selezione della colonna");

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

									default -> System.out.println("Devi inserire una delle due operazioni");
								}
								if (done) {
									System.out.println("Vuoi effettuare altre operazioni su " + column.getName() + "?");
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

								generatorController.addGenerator(table, DBConnection.getConn());
								int howMany = Integer.parseInt(split[1]);
								generateRandomRecords(table, howMany);
							} else
								System.out.println("Errore: Comando Generator errato.");

						} else if (userInput.contains("ScrambleRecords")){
							Pattern scramble = Pattern.compile("ScrambleRecords \\d{1,3}");
							if (scramble.matcher(userInput).matches()) {
								String[] split = userInput.split(" ");
								int howMany = Integer.parseInt(split[1]);
								scrambleRecords(table, null, howMany);
							}
						} else if (userInput.contains("ShiftRecords")) {
							Pattern shift = Pattern.compile("ShiftRecords \\d{1,3}");
							if (shift.matcher(userInput).matches()) {
								String[] split = userInput.split(" ");
								int howMany = Integer.parseInt(split[1]);
								scrambleRecords(table, null, howMany);
							}
						} else System.out.println("Errore: colonna non presente");
					}

					if (userInput.equalsIgnoreCase("back")) continue;

					System.out.println("Vuoi effettuare altre operazioni sulla tabella " + table + "?");
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
					if (percMutate >= 0 && percMutate <= 100) {
						if (percCopy > 0) {
							for (String t : graph.listTables())
								generatorController.addGenerator(t, DBConnection.getConn());

							while (percCopy > 0) {
								int copy = Math.min(percCopy, 100);
								percCopy -= copy;
								augmentation(copy, percMutate);
							}

						}
					}
					codeError = false;
				} else {
					System.out.println("Errore: Comando Augmentation errato.");
					codeError = false;
				}
			}

			if (codeError)
				System.out.println("Errore: tabella o comando non riconosciuti");

			System.out.println("Vuoi effettaure altro?");
			if (!answer()) run = false;
		}
		System.out.println("Fine");
	}

	public void mutateOld() {
		String table = "";
		String operation = "";
		risp="si";
		try {
			DBConnection.getConn().createStatement().execute("Use " + graph.getName());
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(1);
		}

		while(risp.equalsIgnoreCase("si")) {

			//Chiede di quale tabella effettuare le mutazioni
			System.out.println("Comandi:\n" +
					"\tAugmentation <PercentualeAugmentation> <PercentualeMutation>\n\t\t" +
					"Effettua augmentation del DB del <PercentualeAugmentation>% e di questi\n\t\tne muta <PercentualeMutation>%\n" +
					"\t\t<PercentualeMutation> non deve essere > 100");
			System.out.println("Oppure\n\tSeleziona una Tabella:");
			for(String s : validTables) {
				System.out.println("\t" + s);
			}
			table = input.nextLine();
			if (!validTables.contains(table)){

				if (Pattern.compile("Augmentation \\d{1,3} \\d{1,3}").matcher(table).matches()) {

					String[] split = table.split(" ");
					int percCopy = Integer.parseInt(split[1]);
					int percMutate = Integer.parseInt(split[2]);
					System.out.println(percCopy + "; " + percMutate);
					if (percMutate >= 0 && percMutate <= 100) {
						if (percCopy > 0) {
							for (String t : graph.listTables())
								generatorController.addGenerator(t, DBConnection.getConn());

							while (percCopy > 0) {
								int copy = Math.min(percCopy, 100);
								percCopy -= copy;
								augmentation(percCopy, percMutate);
							}
						}
					} else {
						System.out.println("Valore di PercMutate Errato: " + percMutate);
						System.out.println("Riprovare? (si/no)");
						risp = risposta();
					}


				} else {
					System.out.println("Tabella vuota perche non presente in lista, Tabella con solo primaryKeys, tabella non esistente oppure comando augmentation errato.");
					System.out.println("Riprovare? (si/no)");
					risp = risposta();
				}
			} else {
				//Se è una tabella valida continua, altrimenti chiede di nuovo il nome della tabella
				while (risp.equalsIgnoreCase("si")) {

					//Chiede su quale colonna effetture mutazioni, oppure di usare il comando per generare records su questa tabella
					System.out.println("Seleziona la colonna non key per effettuare le mutazioni\n" +
							"oppure\ndigita \"GenerateRecords <percentualeAumento>\" per aumentare il numero di records del <percentualeAumento>% per " + table + ":\n" +
							"Numero records attuale: " + graph.getRecordNumberInTable(table));
					int k = 0;
					for (Column c : graph.getColumnsInTable(table)) {
						if (!c.isPrimaryKey()) {
							System.out.print((k % 3 == 0 ? "\n" : "") + c.getName() + "; ");
							++k;
						}
					}
					String name = input.nextLine();
					Column column = graph.searchColumnInTable(name, table);

					//Controlla se vuole usare il comando per generare records
					if (Pattern.compile("GenerateRecords \\d{1,3}").matcher(name).matches()) {
						String[] split = name.split(" ");
						if (split.length == 2) {
							addGen(table);
							int howMany = Integer.parseInt(split[1]);
							generateRandomRecords(table, howMany);

						} else {
							System.out.println("Comando GenerateRecords errato");
						}

						//Altrimenti controlla se la colonna esiste e non è primary key
					} else if (column != null && !column.isPrimaryKey()) {
						//scelta operazione (blank-modify)
						//Se la colonna non può essere null offre come unica operazione modify
						if (!column.isNullable()) {
							System.out.println("Unica operazione possibile: Modify.\nVuoi eseguirla?");
							operation = risposta();
							if (operation.equalsIgnoreCase("si"))
								operation = "modify";
							else
								operation = null;
						} else {
							System.out.println("Inserici quale operazione eseguire (blank - modify):");
							operation = input.nextLine();
						}

						//se operation ha una risposta al suo interno crea il generatore associato alla tabella
						//se non esiste
						if (operation != null) {
							addGen(table);
							if (operation.equalsIgnoreCase("blank")) {
								fillBlanks(column, table);
							} else if (operation.equalsIgnoreCase("modify")) {
								modify(column, table);

							} else {
								System.out.println("Errore, operazione inserita non valida");
							}
						}
					} else {
						System.out.println("Colonna inserita o non presente, o è primary Key");
					}
					System.out.println("Eseguire altre modifiche alla tabella: " + table + "?(si/no)");
					risp = risposta();
				}
				risp = "si";

			}

		}
		System.out.println("Fine modifiche");
	}

	private boolean b = false;
	private String[][] records = null;
	private int recordsAdded;

	public void augmentation(int percCopy, int percMutate) {
		augmenting = true;
		Random r = new Random();
		try {
			List<String> tables = graph.sortTopological();
			for(int i = 0; i < tables.size(); ++i) {
				String table = tables.get(tables.size() - i - 1);

				//System.out.println("updating foreign key values");
				//generators.get(table).updateForeignValues();
				int howMany = Math.max(1, percCopy * graph.getRecordNumberInTable(table) / 100);

				int howManyToMutate = percMutate * howMany / 100;
				long time = System.currentTimeMillis();
				if (graph.getRecordNumberInTable(table) > 0) {
					System.out.println("Augmenting " + table);

					if (howManyToMutate > 0)
						records = new String[howManyToMutate][graph.getColumnNumberInTable(table)];

					recordsAdded = 0;
					copyRecords(table, percCopy, percMutate);
					System.out.println("copied " + howMany);
					long copyTimer = (System.currentTimeMillis() - time);
					System.out.println(copyTimer);

					//graph.setNOfRecordsByTable(table, graph.getNOfRecordsByTable(table) + howMany - howManyToMutate);
					if (howManyToMutate > 0) {
						//Selezione a roulette:
						// 			0 -> percCombineRecords : si usa scramble o shift
						//			percCombineRecords -> 100 (lunghezza range = percRandomValues)
						long combineTime = System.currentTimeMillis();
						if (graph.getColumnNumberInTable(table) == graph.getPrimaryKeyNumberInTable(table)
								|| r.nextInt(100) < percCombineRecords) {
							System.out.println("combining ");
							if (r.nextInt(100) % 2 == 0) {
								scrambleRecords(table, records, howManyToMutate);
							} else {
								shiftRecords(table, records, howManyToMutate);
							}

						} else { //percRandomValues
							System.out.println("randoming ");
							int columnsToModifySize = graph.getColumnNumberInTable(table) - graph.getPrimaryKeyNumberInTable(table);
							if (columnsToModifySize != 0) {
								Column[] columnsToModify = new Column[columnsToModifySize];
								int count = 0;
								for (Column c : graph.getColumnsInTable(table)) {
									if (!c.isPrimaryKey()) {
										columnsToModify[count] = c;
										++count;
										//System.out.println(c.getName() + ": " + howManyToMutate);
										//randomModify(c, table, "temp", howManyToMutate);
									}
								}
								randomModify(columnsToModify, records, table, "temp", howManyToMutate);


							} else {

							}
						}
					}
					System.out.println("Mutation finished");
					//graph.setNOfRecordsByTable(table, graph.getNOfRecordsByTable(table) + howManyToMutate);
					System.out.println();
				} else {
					generateRandomRecords(table, percCopy);
				}

				records = null;
				recordsAdded = 0;
			}
		} catch(Exception e) {
			e.printStackTrace();
			System.out.println(query);
			System.out.println("Errore: " + e.getMessage());

			augmenting = false;
		}
		augmenting = false;
	}

	//TODO check se nuove primarykeys sono generabili
	//Copia percCopy records modificandone le chiavi:
	//di questi il percMutate% viene messo in una tabella temporanea, il resto aggiunti alla tabella originale
	private void copyRecords(String table, int percCopy, int percMutate) throws Exception {
		//Calcola quanti records copiare e quanti di questi mutare
		int howMany = Math.max(1, percCopy * graph.getRecordNumberInTable(table)/100);
		int howManyToMutate = howMany * percMutate/100;

		//Crea una tabella temporanea con struttura identica a quella dalla quale si stanno prendendo i records
		//dove inserire i records da mutare e la inserisce al grafo
		query = "SELECT * FROM " + table +" ORDER BY RAND() LIMIT " + howMany;
		Statement st = DBConnection.getConn().createStatement();
		ResultSet rs = st.executeQuery(query);
		List<Column> columns = graph.getColumnsInTable(table);

		int count = 0;
		String queryFirstPart = createInsertionQuery(columns, null, table);
		queryFirstPart = queryFirstPart.substring(0, queryFirstPart.indexOf("VALUES") + 6);
		StringBuilder queryCopy = null;
		String queryTempFirstPart = null;
		StringBuilder queryTemp = null;


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

		Column randomPk = null;
		for(Column col : graph.getPrimaryKeysInTable(table)) {
			if (!col.isAutoIncrementing()) {
				randomPk = col;
				break;
			}
		}

		int toDivideMutate = howManyToMutate;
		int remainingRecordsToMutate = howManyToMutate;
		if (randomPk != null && howManyToMutate > 0) {
			double divider = Math.max(1.0, (double) howManyToMutate /insertThreshold);
			toDivideMutate = (int)(howManyToMutate / divider);
			queryTempFirstPart = "INSERT INTO tempForMutate " +  queryFirstPart.substring(queryFirstPart.indexOf("("));
			queryTemp = new StringBuilder(queryTempFirstPart);
			DBConnection.getConn().createStatement().execute("CREATE TABLE tempForMutate LIKE " + table);
		}

		boolean bol = true;
		boolean checkMutated = true;
		int startToSearchFrom = 0;
		while(rs.next()) {
			String[] values = new String[columns.size()];

			//Prende ogni campo e inserisce in un array i suoi valori
			for(Column c : columns) {
				int columnIndex = columns.indexOf(c);
				if (c.isAutoIncrementing()) {
					values[columnIndex] = "AUTO";
				}
				else if (c.isPrimaryKey()) {
					values[columnIndex] = generatorController.generateValue(table, c.getName(), false);//checkEscapes(gen.generateValue(c.getName(), false));
					//System.out.println("Generata: " + values[columnIndex]);
				} else
					values[columnIndex] = rs.getString(c.getName());

				if (values[columnIndex] != null)
					values[columnIndex] = checkEscapes(values[columns.indexOf(c)]);
				else values[columnIndex] = "null";

			}

			//Controlla che le chiavi primarie che ha non siano già presenti ne nella tabella
			//ne nelle chiavi già generate e non ancora inserite
			boolean alreadyGenerated;
			int countFails = 0;
			do {
				alreadyGenerated = false;
				if (randomPk != null) {
					//TODO togliere la seconda condizione
					if (!areKeysAlreadyIn(values, table, table) && (checkMutated || !areKeysAlreadyIn(values, table, "tempForMutate"))) {

						int columnIndex = columns.indexOf(randomPk);
						int toMutateStartingIndex = startToSearchFrom;
						int toCopyStartingIndex = 0;
						boolean finished = false;
						do {
							int recordIndex = -1;
							boolean foundInCopied = false;

							for (int i = toMutateStartingIndex; i < recordsAdded; ++i) {

								if (records[i][columnIndex].equals(values[columnIndex])) {
									//System.out.println("Ho trovato " + values[columnIndex] + " per " + c.getName() + " in ciclo per records");
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

							if (toMutateStartingIndex == recordsAdded) {
								for (int i = toCopyStartingIndex; i < recordsCopied; ++i) {
									if (recordsToCopy[i][columnIndex].equals(values[columnIndex])) {
										//System.out.println("Ho trovato " + values[columnIndex] + " per " + c.getName() + " in ciclo per recordsCopy");
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
							if (recordIndex == -1) finished = true;
							else {
								List<Column> pKeys = graph.getPrimaryKeysInTable(table);
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
										alreadyGenerated = true;
										finished = true;
									}
								}
							}

						} while (!finished);

					} else {
						alreadyGenerated = true;
						//System.out.println("gia nel db");
					}
				}
				if (alreadyGenerated) {
					//System.out.println("Rip lo sto rigenerando\n");
					for(Column p : graph.getPrimaryKeysInTable(table)) {
						int columnIndex = columns.indexOf(p);
						String val = generatorController.generateValue(table, p.getName(), countFails > 100);
						val = checkEscapes(val);
						//System.out.println(values[columnIndex] + " -> " +val);
						values[columnIndex] = val;
					}
					++countFails;
				}
			} while (alreadyGenerated);

			if (count < howManyToMutate){
				records[recordsAdded] = values;
				++recordsAdded;
				++count;
				//System.out.println("Added in mutation");
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
					try {
						if (recordsAdded % (toDivideMutate) == 0) {
							System.out.println(count);
							queryTemp = new StringBuilder(queryTemp.substring(0, queryTemp.length() - 1));
							//System.out.println("temping " + toDivideMutate + " records");
							DBConnection.getConn().createStatement().execute(queryTemp.toString());
							queryTemp = new StringBuilder(queryTempFirstPart);
							checkMutated = false;
							startToSearchFrom = recordsAdded;
							if (remainingRecordsToMutate < toDivideMutate)
								toDivideMutate = remainingRecordsToMutate;
							remainingRecordsToMutate -= toDivideMutate;
						}
					}catch(Exception e) {
						System.out.println("exception prints");
						System.out.println(queryCopy);
						System.out.println(queryTemp);
						System.out.println(checkMutated);
						System.out.println(startToSearchFrom);
						System.out.println(toDivideMutate);
						System.out.println(remainingRecordsToMutate);
						System.out.println(checkMutated);
						System.out.println(recordsAdded);
						System.out.println("-------\n");
						e.printStackTrace();
						throw e;
					}
				}


			} else {
				recordsToCopy[recordsCopied] = values;
				++recordsCopied;
				++count;
				//System.out.println("Added in copy");
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
					bol = false;
					queryCopy = new StringBuilder(queryFirstPart);
					recordsCopied = 0;
					if (remainingRecordsToCopy < toDivideCopy)
						toDivideCopy = remainingRecordsToCopy;
					recordsToCopy = new String[toDivideCopy][columns.size()];
					remainingRecordsToCopy -= toDivideCopy;
				}

			}

			//Aggiorna il numero dei records nel generatore
			for(Column c : columns) {
				if (!values[columns.indexOf(c)].equalsIgnoreCase("null"))
					generatorController.updateTotal(table, c.getName());

				if (c.isPrimaryKey() && !c.isAutoIncrementing())
					generatorController.updateForeignValues(table, c.getName(), values[columns.indexOf(c)]);
			}

		}

		try {
			if (howManyToMutate > 0 && randomPk != null)
				DBConnection.getConn().createStatement().execute("DROP TABLE tempForMutate");


		} catch (Exception e) {

			System.out.println(queryCopy);
			System.out.println(queryTemp);
			System.out.println(checkMutated);
			System.out.println(startToSearchFrom);
			e.printStackTrace();

		}
	}

	private void checkNewKeys(String[] values, String[][] toMutate, int toMutateMax, String[][] toCopy, int toCopyMax, String table, int startToSearchFrom, boolean checkMutated, boolean bol) throws Exception{
		List<Column> columns = graph.getColumnsInTable(table);
		List<Column> pKeys = graph.getPrimaryKeysInTable(table);
		Column randomPk = null;
		for (Column p : pKeys) {
			if (!p.isAutoIncrementing()) {
				randomPk = p;
				break;
			}
		}
		boolean alreadyGenerated;
		int countFails = 0;
		do {
			alreadyGenerated = false;
			if (randomPk != null) {
				//TODO togliere la seconda condizione
				if (!areKeysAlreadyIn(values, table, table) && (checkMutated || !areKeysAlreadyIn(values, table, "tempForMutate"))) {

					int columnIndex = columns.indexOf(randomPk);
					int toMutateStartingIndex = startToSearchFrom;
					int toCopyStartingIndex = 0;
					boolean finished = false;
					do {
						int recordIndex = -1;
						boolean foundInCopied = false;

						for (int i = toMutateStartingIndex; i < toMutateMax; ++i) {

							if (toMutate[i][columnIndex].equals(values[columnIndex])) {
								//System.out.println("Ho trovato " + values[columnIndex] + " per " + c.getName() + " in ciclo per records");
								if (graph.getPrimaryKeyNumberInTable(table) == 1)
									alreadyGenerated = true;
								else {
									recordIndex = i;
									toMutateStartingIndex = i + 1;
								}
								break;
							}
							if (i == toMutateMax - 1) toMutateStartingIndex = toMutateMax;
						}

						if (toMutateStartingIndex == toMutateMax) {
							for (int i = toCopyStartingIndex; i < toCopyMax; ++i) {
								if (toCopy[i][columnIndex].equals(values[columnIndex])) {
									//System.out.println("Ho trovato " + values[columnIndex] + " per " + c.getName() + " in ciclo per recordsCopy");
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
						if (recordIndex == -1) finished = true;
						else {

							for (int i = 0; i < pKeys.size(); ++i) {
								Column p = pKeys.get(i);
								if (i != columnIndex && !p.isAutoIncrementing()) {
									int secondColumnIndex = columns.indexOf(p);
									if (foundInCopied) {
										if (!toCopy[recordIndex][secondColumnIndex].equals(values[secondColumnIndex]))
											break;
									} else if (!toMutate[recordIndex][secondColumnIndex].equals(values[secondColumnIndex])) {
										break;
									}
								}
								if (i == pKeys.size() - 1) {
									alreadyGenerated = true;
									finished = true;
								}
							}
						}

					} while (!finished);

				} else {
					alreadyGenerated = true;
					//System.out.println("gia nel db");
				}
			}
			if (alreadyGenerated) {
				//System.out.println("Rip lo sto rigenerando\n");
				for(Column p : graph.getPrimaryKeysInTable(table)) {
					int columnIndex = columns.indexOf(p);
					String val = generatorController.generateValue(table, p.getName(), countFails > 100);
					val = checkEscapes(val);
					//System.out.println(values[columnIndex] + " -> " +val);
					values[columnIndex] = val;
				}
				++countFails;
			}
		} while (alreadyGenerated);
	}

	//TODO dividere scramble e shift in metodi per operare su singole colonne
	public void scrambleRecords(String table, String[][] values, int howMany) {
		//int howMany = Math.max(2, graph.getNOfRecordsByTable(temp) * perc / 100);
		long time = System.currentTimeMillis();
		if (values == null)
			values = getNRecordsFrom(table, table, howMany);

		boolean scrambled = true;
		if (!(graph.getPrimaryKeyNumberInTable(table) == graph.getColumnNumberInTable(table))) {

			assert values != null;
			List<Column> columns = graph.getColumnsInTable(table);
			//Lista che contiene gli indici che poi verranno presi a caso
			List<Integer> indexes = new ArrayList<>();
			for(int i = 0; i < howMany; ++i) {
				indexes.add(i);
			}
			Random r = new Random();

			for (Column c : columns) {
				if (!c.isPrimaryKey()) {
					//Lista che funge da "stack" per gli indici da rimuovere
					List<Integer> alreadyScrambled = new ArrayList<>(indexes);

					//scelgo da dove partire
					int i = alreadyScrambled.remove(r.nextInt(alreadyScrambled.size()));
					int columnIndex = columns.indexOf(c);
					String prev = values[i][columnIndex];
					while (alreadyScrambled.size() > 0) {
						//scelgo il prossimo da cambiare
						int j = alreadyScrambled.remove(r.nextInt(alreadyScrambled.size()));
						String temp = values[j][columnIndex];
						//System.out.print("\n" + prev + " al posto di " + temp);
						values[j][columnIndex] = prev;
						prev = temp;
					}
					//System.out.print("\n" + prev + " al posto di " + values[i][columnIndex] + "\n");
					values[i][columnIndex] = prev;
				}
			}
		} else scrambled = false;
		//cancella i records che ho scrambleato e li reinserisce con i nuovi valori
		long scrambleTimer = (System.currentTimeMillis() - time);
		if (scrambled) {
			if (augmenting)
				insertRecords(table, values, howMany);
			else
				replaceRecords(table, table, values, howMany);
		}
	}

	public void shiftRecords(String table, String[][] values, int howMany) {

		long time = System.currentTimeMillis();
		if (howMany < 2) return;
		if (values == null)
			values = getNRecordsFrom(table, table, howMany);

		//int howMany = Math.max(2, graph.getNOfRecordsByTable(table) * perc / 100);
		System.out.println("shifting " + howMany);
		boolean shifting = true;
		if (!(graph.getPrimaryKeyNumberInTable(table) == graph.getColumnNumberInTable(table))) {
			assert values != null;
			List<Column> columns = graph.getColumnsInTable(table);

			Random r = new Random();
			int prev = 0;
			for (Column c : columns) {
				if (!c.isPrimaryKey()) {
					int number;
					do {
						number = r.nextInt(howMany);
					} while (number == prev);
					prev = number;
					//System.out.println(c.getName() + " shifted by " + number);
					int columnIndex = columns.indexOf(c);
					String[] temp = new String[howMany];
					for (int i = 0; i < howMany; ++i) {
						if (!c.isAutoIncrementing()) {
							int j = (i + number) % howMany;
							temp[j] = values[i][columnIndex];
						}
					}
					//Riassegna i valori in values nelle nuove posizioni
					for (int i = 0; i < howMany; ++i) {
						values[i][columnIndex] = temp[i];
					}
				}
			}
		} else shifting = false;

		long shiftTimer = (System.currentTimeMillis() - time);

		if (shifting) {
			//Cancella i records che ora ha shiftato e reinserisce con valori modificati
			if (augmenting)
				insertRecords(table, values, howMany);
			else
				replaceRecords(table, table, values, howMany);
		}

	}

	private void insertRecords(String table, String[][] values, int howMany) {
		int threshold = Math.min(howMany, insertThreshold);
		int remaining = howMany;
		String p = table.substring(4);
		List<Column> columns = graph.getColumnsInTable(table.contains("temp") ? p : table);
		String startInsertionQuery = createInsertionQuery(columns, null, table);
		startInsertionQuery = startInsertionQuery.substring(0, startInsertionQuery.indexOf("VALUES") + 6);
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
				try {
					System.out.println(insertionQuery);
					DBConnection.getConn().createStatement().execute(insertionQuery.toString());
					insertionQuery = new StringBuilder(startInsertionQuery);
				} catch (Exception e) {
					System.out.println(insertionQuery);
					e.printStackTrace();
				}
			}
		}
	}

	private void replaceRecords(String table, String tableToAffect, String[][] values, int howMany) {
		String p = table.substring(4);
		long time = System.currentTimeMillis();

		int threshold = Math.min(howMany, insertThreshold);
		int remaining = howMany;


		List<Column> columns = graph.getColumnsInTable(table.contains("temp") ? p : table);
		boolean allAI = false;
		for (Column c : columns) {
			if (c.isPrimaryKey() && c.isAutoIncrementing())  {
				allAI = true;
				break;
			}
		}
		if (!allAI) {
			String startInsertionQuery = createInsertionQuery(columns, null, table);
			startInsertionQuery = startInsertionQuery.substring(0, startInsertionQuery.indexOf("VALUES") + 6);
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
				try {
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
						insertionQuery.append("" + duplicateUpdate);
						try {
							DBConnection.getConn().createStatement().execute(insertionQuery.toString());
							insertionQuery = new StringBuilder(startInsertionQuery);
						} catch (Exception e) {
							System.out.println(insertionQuery);
							e.printStackTrace();
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println();

				}
			}
		} else {

			try {
				for (int i = 0; i < howMany; ++i) {
					query = "UPDATE " + tableToAffect + " set ";
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
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(query);
				System.out.println();
			}
		}
	}

	private String[][] getNRecordsFrom(String table, String tableToGet, int howMany) {
		List<Column> columns = graph.getColumnsInTable(table);
		String[][] values = new String[howMany][columns.size()];

		try {
			Statement st = DBConnection.getConn().createStatement();
			ResultSet rs = st.executeQuery("SELECT * FROM " + tableToGet + " ORDER BY RAND() LIMIT " + howMany);
			int i = 0;
			while(rs.next()) {
				for(Column c : columns) {
					if (c.isAutoIncrementing())
						values[i][columns.indexOf(c)] = rs.getString(c.getName());
					else
						values[i][columns.indexOf(c)] = rs.getString(c.getName());
					if (values[i][columns.indexOf(c)] != null)
						values[i][columns.indexOf(c)] = checkEscapes(values[i][columns.indexOf(c)]);
					else values[i][columns.indexOf(c)] = "null";
				}
				++i;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return values;
	}
	private String createInsertionQuery(List<Column> columns, String[] values, String table) {
		StringBuilder insertQuery = new StringBuilder("INSERT INTO " + table + " (");
		StringBuilder vals = new StringBuilder("VALUES(");
		for (int j = 0; j < columns.size(); ++j) {
			//QUesto boolean stabilisce se inserire nella query di inserimento il valore autoincrementing
			boolean insertAutoIncrementing = (values == null && columns.get(j).isAutoIncrementing()) || (columns.get(j).isAutoIncrementing() && values != null && values[j].contains("AUTO"));

			if (!insertAutoIncrementing){

				insertQuery.append(columns.get(j).getName()).append(",");
				if (values != null) {
					try {
						if (values[j].contains("CONVERT") || values[j].equalsIgnoreCase("null"))
							vals.append(values[j]).append(",");
						else
							vals.append("'").append(values[j]).append("',");
					} catch (Exception e) {
						e.printStackTrace();
						System.out.println();
					}
				}
			}
		}
		if (values != null)
			vals = new StringBuilder(vals.substring(0, vals.length() - 1) + ");");
		insertQuery = new StringBuilder(insertQuery.substring(0, insertQuery.length() - 1) + ") " + vals);

		return insertQuery.toString();
	}
	/*Metodo per il riempimento dei blank
	 * - conta il numero di blank presenti nella colonna (se non presenti finisce l'esecuzione)
	 * - scelta tipologia di riempimento
	 * - invocazione del rispettivo metodo
	 */
	public void fillBlanks(Column column, String table) {
		int n_blank = -1;
		switch(column.getDatatype()) {
			case Types.CHAR, Types.NCHAR, Types.LONGNVARCHAR, Types.VARCHAR, Types.NVARCHAR -> {
				query = "SELECT COUNT(*) FROM " + table + " WHERE " + column.getName() + " IS NULL OR " + column.getName() + "=''";
			}

			default -> {
				query = "SELECT COUNT(*) FROM " + table + " WHERE " + column.getName() + " IS NULL";
			}
		}
		try {
			st = DBConnection.getConn().createStatement();
			rs = st.executeQuery(query);
			if(rs.next()) {
				n_blank = rs.getInt(1);
			}
		}catch(SQLException se) {
			System.out.println("Error: "+se.getMessage());
		}finally {
			DBConnection.closeSt(st);
			DBConnection.closeRs(rs);
		}
		if(n_blank == 0) {
			System.out.println("Non sono presenti spazi vuoti in: "+column.getName());
		}else if(n_blank > 0) {
			fillBlanksWithRandom(column,table, n_blank);
		}
		System.out.println("Fine, blank");
	}

	private void fillBlanksWithRandom(Column column, String table, int n_blank) {
		Random r = new Random();
		int remaining = n_blank;
		//Generator gen = generators.get(table);
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
				System.out.println("Operazione eseguita, " + n + " blank riempiti con: " + val);

			} catch(Exception e) {
				e.printStackTrace();
				System.out.println("Operazione: riempimento " + n + " blank con " + val + " fallita");
				System.exit(1);
			}
		}



	}

	/*Metodo per la modifica di valori di una colonna
	 * - scelta, in base al tipo di colonna, il tipo di modifica
	 * - invocazione del rispettivo metodo
	 */
	private void modify(Column column, String table) {
		switch(column.getDatatype()) {
			case Types.NUMERIC, Types.INTEGER, Types.BIGINT, Types.SMALLINT, Types.DECIMAL,
					Types.FLOAT, Types.REAL, Types.TINYINT -> {
				if (column instanceof ForeignKeyColumn){
					System.out.println("E' foreignKey: Modifica possibile: random");
					System.out.println("Quanti valori vuoi modificare?");
					int howMany = askHowMany() * graph.getRecordNumberInTable(table)/100;
					randomModify(new Column[]{column}, null, table, table, howMany);
				} else {
					System.out.println("Inserisci tipo di modifica (operazione - random)");
					String type = input.nextLine();
					if (type.equalsIgnoreCase("operazione")) {
						randomOperation(column, table);
					} else if (type.equalsIgnoreCase("random")) {
						System.out.println("Quanti valori vuoi modificare?");
						int howMany = askHowMany() * graph.getRecordNumberInTable(table)/100;
						randomModify(new Column[]{column}, null, table, table, howMany);
					} else {
						System.out.println("Modifica inserita non valida");
					}
				}
			}
			default -> {
				System.out.println((column instanceof ForeignKeyColumn ? "E' foreignkey: " : "") + "Modifica possibile: random");
				System.out.println("Quanti valori vuoi modificare?");
				int howMany = askHowMany() * graph.getRecordNumberInTable(table)/100;
				//randomModify(column, table, table, howMany);
				randomModify(new Column[]{column}, null, table, table, howMany);
			}
		}

		System.out.println("Fine modify.");
	}

	//TODO Rework this again
	public void randomModify(Column[] columnsToModify, String[][] values, String table, String tableToUpdate, int howMany) {
		long time = System.currentTimeMillis();
		if (values == null)
			values = getNRecordsFrom(table, tableToUpdate, howMany);
		List<Column> columns = graph.getColumnsInTable(table);
		try {
			for(int i = 0; i < howMany; ++i) {
				for (Column column : columnsToModify) {
					if (!column.isAutoIncrementing() && !column.isPrimaryKey()) {
						int columnIndex = columns.indexOf(column);
						values[i][columnIndex] = generatorController.generateValue(table, column.getName(), false);//checkEscapes(gen.generateValue(column.getName(), false));
					}
				}
			}
			if (augmenting)
				insertRecords(table, values, howMany);
			else
				replaceRecords(table, tableToUpdate, values, howMany);

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(query);
			System.out.println();
		}

		/*
		//Generator gen = generators.get(table);
		String query ="";
		long replaceTime = 0;
		try {
			int count = columnsToModify.length;
			long timeRep = 0;
			for(int i = 0; i < howMany; ++i){
				if (!augmenting)
					query = "UPDATE " + tableToUpdate + " set ";
				for (Column column : columnsToModify) {

					if (!column.isAutoIncrementing() && !column.isPrimaryKey()) {
						int columnIndex = columns.indexOf(column);
						values[i][columnIndex] = generatorController.generateValue(table, column.getName(), false);//checkEscapes(gen.generateValue(column.getName(), false));

						if (!augmenting) {
							timeRep = System.currentTimeMillis();
							query += column.getName() + "=";
							if (values[i][columnIndex].contains("CONVERT") || values[i][columnIndex].equals("null"))
								query += values[i][columnIndex];
							else
								query += "'" + values[i][columnIndex] + "'";
							query += ",";
							if (i == 0)
								count++;
							replaceTime += System.currentTimeMillis() - timeRep;
						}

					}
				}

				if (!augmenting) {
					timeRep = System.currentTimeMillis();
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
					connection.createStatement().executeUpdate(query);
					replaceTime += System.currentTimeMillis() - timeRep;
				}
			}

			long randomTime = System.currentTimeMillis() - time - replaceTime;
			log.addTimer("random", randomTime);
			log.addTimer("scramble", 0);
			log.addTimer("shift", 0);
			log.mutationPrint("random: " + randomTime + "ms\t" + (howMany * count) + " values (" + howMany + " x " + count + " colonne non chiave)\n");
			if (augmenting) {
				insertRecords(table, values, howMany);
			} else {
				log.addTimer("replace", replaceTime);
				log.mutationPrint("Time for replace: " + replaceTime);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(query);
			System.out.println();
		}
		 */
	}

	/*Metodo per la modifica: OPERAZIONE
	 * - chiede all'utente l'operazione aritmetica da eseguire
	 * - chiede all'utente condizione, che permette di scegliere
	 *   i record su cui effettuare l'operazione aritmetica
	 * - effettua l'operazione
	 */
	public void randomOperation(Column column, String table) {
		//Operazioni successive nella stessa esecuzione potrebbero avvenire sugli stessi records
		//Finche si è in una certa istanza di mutation, i lowerbound e upperbound dei campi numerici è sempre lo stesso
		System.out.println("Quante operazioni vuoi effettuare?");
		Random r = new Random();
		int howMany = askHowMany();
		int h = howMany;
		char[] operations = {'+', '-'};

		int tried = 0;
		int success = 0;
		String pKeys = graph.getPrimaryKeysStringInTable(table, ',', "", "");
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
				String cond = column.getName() + operation + random + conditionSign + operationBound;
				query = "UPDATE " + table + " SET " + column.getName() + "=" + column.getName() + operation + random + " " +
						"WHERE " + (graph.isPrimaryKeyComposedInTable(table) ? "(" + pKeys + ")" : pKeys) + " IN (" +
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
					++success;
					howMany -= n;
					System.out.println("Operazione " + operation + " eseguita su " + n + " records con il valore " + random);
				} else {
					//Questa cosa è da togliere
					howMany = 0;
					System.out.println("Non ci sono più elementi che rispettino la condizione " + cond);
				}
				++tried;
			}
		}catch(SQLException se) {
			System.out.println("Error: " +se.getMessage());
		}finally {
			DBConnection.closeSt(st);
		}
		System.out.println(success + "/" + tried + " for " + h + " records");
	}

	private int askHowMany(){
		System.out.println("Seleziona percentuale di mutazione: ");

		int howMany = -1;
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

	public void generateRandomRecords(String table, int howManyPerc) {
		int threshold = insertThreshold;
		List<Column> columns = graph.getColumnsInTable(table);
		int howMany = Math.max(1, graph.getRecordNumberInTable(table) * howManyPerc / 100);
		System.out.println("howmanyrecordstoadd: "+ howMany);
		//Index of list are parallel to values array
		String[] values = new String[columns.size()];
		String[][] newRecords = new String[threshold][columns.size()];
		int newRecordsRowIndex = 0;
		//Generator gen = generators.get(table);
		try {
			//Generates the fields
			boolean onlyAIOrNoKeys = false;
			Column randomPk = null;
			for (int i = 0; i < howMany; ++i) {
				for(Column c : columns){
					if (!onlyAIOrNoKeys && c.isPrimaryKey() && !c.isAutoIncrementing()) {
						randomPk = c;
						onlyAIOrNoKeys = true;
					}
					System.out.println("Generating for " + c.getName());
					String val = generatorController.generateValue(table, c.getName(), false);//gen.generateValue(c.getName(), false);
					values[columns.indexOf(c)] = val;
					System.out.println("Generated " + values[columns.indexOf(c)] + "\n");
				}

				//check for keys in matrix and table
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
								boolean foundInCopied = false;

								for (int index = toMutateStartingIndex; index < newRecordsRowIndex; ++index) {

									if (records[index][columnIndex].equals(values[columnIndex])) {
										//System.out.println("Ho trovato " + values[columnIndex] + " per " + c.getName() + " in ciclo per records");
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

						} else {
							alreadyGenerated = true;
							//System.out.println("gia nel db");
						}
					}
					if (alreadyGenerated) {
						//System.out.println("Rip lo sto rigenerando\n");
						for(Column p : graph.getPrimaryKeysInTable(table)) {
							int columnIndex = columns.indexOf(p);
							String val = generatorController.generateValue(table, p.getName(), countFails > 100);
							val = checkEscapes(val);
							//System.out.println(values[columnIndex] + " -> " +val);
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

		} catch (Exception e){
			e.printStackTrace();
		}
	}

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


	/*Metodo per riepimento: RANDOM
	 * - prende un valore random presente nel database (della colonna su cui si sta operando)
	 * - riempie i blank con quel valore
	 */
	public static String getRandomRecordFrom(String column, String table){
		return "SELECT "+column+ " FROM " + table+ "" +
				" WHERE "+column+" IS NOT NULL AND " +
				""+table+"!='' ORDER BY RAND() LIMIT 1";
	}



	//Metodo che restituisce la risposta dell'utente (si/no)
	private static String risposta() {
		boolean run=true;
		String input ="";
		while(run) {
			input = DBMutation.input.nextLine();
			if(input.equalsIgnoreCase("si")||input.equalsIgnoreCase("no")) {
				run=false;
			}else {
				System.out.println("Attenzione, inserire si o no.");
			}
		}
		return input;
	}

	private boolean answer() {
		boolean run = true;
		String input ="";
		while(run) {
			input = DBMutation.input.nextLine();
			if(input.equalsIgnoreCase("si")) return true;
			if (input.equalsIgnoreCase("no")) return false;

			System.out.println("Attenzione: inserire si o no");
		}
		throw new IllegalStateException("Unreachable code");
	}
	/** Effettua una query che controlla se i valori generati delle chiavi primarie sono già presenti
	 *
	 * @param values Valori delle chiavi primarie
	 * @param table	 Tabella
	 * @return	True se la combinazione di chiavi non è presente nella tabella
	 * @throws Exception Ogni SQLException
	 */
	private boolean areKeysAlreadyIn(String[] values, String table, String tableToCheck) throws Exception{
		try {
			if (graph.getPrimaryKeyNumberInTable(table) == 0)
				return false;
			List<Column> columns = graph.getColumnsInTable(table);
			List<Column> primaryKeys = graph.getPrimaryKeysInTable(table);
			String primaryKeysQuery = "SELECT * FROM " + tableToCheck + " ";
			String whereConditions = "WHERE ";
			for (Column primaryKey : primaryKeys) {
				//Se il valore dato alla primaryKey è AUTO_INCREMENT allora la colonna non deve essere considerata
				//Nella query. Se quella è l'unica colonna
				if (primaryKey.isAutoIncrementing()) {
					//TODO se tutte le colonne sono autoincrement ma non so se è un caso possibile
					if (!graph.isPrimaryKeyComposedInTable(table))
						return false;
				} else {
					//Altrimenti costruisce la where clause per fare la query
					if (values[columns.indexOf(primaryKey)].equalsIgnoreCase("null"))
						whereConditions += primaryKey.getName() + " is " + values[columns.indexOf(primaryKey)] + " and ";
					else
						whereConditions += primaryKey.getName() + "='" + values[columns.indexOf(primaryKey)] + "' and ";
				}
			}

			//Remove the last and
			whereConditions = whereConditions.substring(0, whereConditions.length() - 5);

			//Completo ed eseguo la query
			primaryKeysQuery += whereConditions;
			ResultSet check = DBConnection.getConn().createStatement().executeQuery(primaryKeysQuery);
			return check.next();
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
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
	public void addGen(String table) {
		generatorController.addGenerator(table, DBConnection.getConn());
	}

	@Deprecated
	public long searchMin(String table, int howManyPerQ) {
		System.out.println("Searching...");
		long totalInsertTime = 0;
		try {
			ResultSet rs = null;
			DBConnection.getConn().createStatement().execute("CREATE TABLE temp" + table + " like "+ table);
			List<Column> columns = graph.getColumnsInTable(table);
			String queryFirstPart = createInsertionQuery(columns, null, "temp" + table);
			queryFirstPart = queryFirstPart.substring(0, queryFirstPart.indexOf("VALUES") + 6);
			String queryCopy = queryFirstPart;

			rs = DBConnection.getConn().createStatement().executeQuery("SELECT * FROM " + table);


			int count = 0;
			int remaining = graph.getRecordNumberInTable(table);
			while (rs.next()) {
				String[] values = new String[columns.size()];

				//Generator gen = generators.get(table);

				//Prende ogni campo e inserisce in un array i suoi valori
				for(Column c : columns) {
					int columnIndex = columns.indexOf(c);
					values[columnIndex] = rs.getString(c.getName());

					if (values[columnIndex] != null)
						values[columnIndex] = checkEscapes(values[columns.indexOf(c)]);
					else values[columnIndex] = "null";
				}
				queryCopy += "(";
				for(Column c : columns) {
					if (!c.isAutoIncrementing()) {
						if (values[columns.indexOf(c)] == null) values[columns.indexOf(c)] = "null";
						if (values[columns.indexOf(c)].contains("CONVERT") || values[columns.indexOf(c)].equalsIgnoreCase("null"))
							queryCopy += values[columns.indexOf(c)] + ",";
						else
							queryCopy += "'" + values[columns.indexOf(c)] + "',";
					}
				}
				queryCopy = queryCopy.substring(0, queryCopy.length() - 1) + "),";
				++count;
				if (count == howManyPerQ) {
					queryCopy = queryCopy.substring(0, queryCopy.length() - 1);
					//System.out.println("copying " + recordsCopied + " records");

					DBConnection.getConn().createStatement().execute(queryCopy);

					queryCopy = queryFirstPart;
					remaining-=count;
					if (remaining < howManyPerQ)
						howManyPerQ = remaining;
					count = 0;
				}


			}
			DBConnection.getConn().createStatement().execute("DROP TABLE temp"+table);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return totalInsertTime;
	}
}
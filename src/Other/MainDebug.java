package src.Other;

import Graph.Graph;
import Graph.Graph.nodeType;
import src.DBSBaseline.DBSplit_Baseline;
import src.DBSFromHigher.DBSplit_from_higher;
import src.DBSKnapsack.DBSplit_Knapsack;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

public class MainDebug {

	private static final Scanner input = new Scanner(System.in).useDelimiter("\n");
	private static final Scanner input2 = new Scanner(System.in);
	public static String sv ="";
	public static String user = "";
	public static String password = "";
	private static int percent = 0;
	private static int percentOverlapping = 0;
	private static String splitType = "";
	public static String DB1; 
	public static String DB2;
	public static String DB; //original;
	public static String DBMS; //used for connecting to DB via JDBC (e.g., oracle, mysql, sqlserver)
	private static String path = "";
	private static String[] dbsArray;
	private static int[] overlappingArray;
	private static int[] splitArray;
	private static int executeWhat;
	static Graph graph;

	public static Map<String, TableReport> report;
	public static String fileName;
    static String ID;
    static int total_records = 0;
    static int expected_overlapping = 0;
    static double algorithm_total_time = 0;
    static double algorithm_total_knapsack = 0;
    static double algorithm_total_variation = 0;
    static double algorithm_overlapping = 0;
    static double baseline_total_time = 0;
    static double baseline_total_variation = 0;
    static double baseline_overlapping = 0;
    static int algorithm_perfectMatch_DB1 = 0;
    static int baseline_perfectMatch_DB1 = 0;
    static int algorithm_perfectMatch_DB2 = 0;
    static int baseline_perfectMatch_DB2 = 0;
	static int algorithm_perfectMatch_overlapping = 0;
	static int baseline_perfectMatch_overlapping = 0;
	private static int algorithm_variation_DB1;
	private static int algorithm_variation_DB2;
	private static double baseline_variation_DB1;
	private static double baseline_variation_DB2;

	public static void main(String[] args) {
		System.out.println("Running DBSplit...\n");
		System.out.println("Arguments: <server> <user> <password> <input-db> <output-db1> <output-db2> <perc-split>"+
				"<split-type> [<perc-overlapping>] <dbms> <execution [baseline-knapsack-both]>\n");
		System.out.println("Example: localhost:3306 user001 password001 testDB half1DB half2DB 30 " +
				"overlapping 60 mysql baseline\n");
		boolean run = true;

		while (run) {
			//if 0 debug, if 1 config mode, else user run
			int mode = 0;
			if (mode == 0) {
                DBMS = "sqlite";
                splitArray = new int[]{60};
                splitType = "disjoint";
                overlappingArray = new int[]{30}; //goes to 0 if disjoint
				executeWhat = 1;
				run = false;

                switch (DBMS.toLowerCase()) {
                    case "sqlite" -> {
                        sv = "";
                        user = "";
                        password = "";
                        dbsArray = new String[]{"/home/ilan/Downloads/pokemon_db.db"};
                        DB1 = "try1.db";
                        DB2 = "try2.db";
                    }
                    case "mysql" -> {
                        sv = "localhost:3306";
                        user = "root";
                        password = "Password_123";
						dbsArray = new String[]{"pokemon_db"};
                        DB1 = "DB_1";
                        DB2 = "DB_2";
                    }
                }

            } else if (mode == 1) {
                getConfig();

			} else if (args.length == 10 && args[7].equalsIgnoreCase("disjoint")
					|| args.length == 11 && args[7].equalsIgnoreCase("disjoint")) {
				int argIndex = 0;
				sv = args[argIndex++];
				System.out.println(sv);

				user = args[argIndex++];
				System.out.println(user);

				password = args[argIndex++];
				System.out.println(password);

				DB = args[argIndex++];
				System.out.println(DB);

				DB1 = args[argIndex++];
				System.out.println(DB1);
				DB2 = args[argIndex++];
				System.out.println(DB2);

				percent = Integer.parseInt(args[argIndex++]);
				System.out.println(percent);

				splitType = args[argIndex++];
				System.out.println(splitType);
				if (splitType.equals("overlapping")) {
					percentOverlapping = Integer.parseInt(args[argIndex++]);
					System.out.println(percentOverlapping);
				}

				DBMS = args[argIndex++];
				System.out.println(DBMS);

				executeWhat = args[argIndex].equalsIgnoreCase("both") ? 0 :
						args[argIndex].equalsIgnoreCase("knapsack") ? 1 : 2;

				switch (executeWhat) {
					case 0: System.out.println("both");
					case 1: System.out.println("knapsack");
					case 2: System.out.println("baseline");
				}
			} else {
				System.out.print("Server name (usually 'localhost:3306' for MySQL or 'SQLEXPRESS' for SQLServer, leave empty for SQLite): ");
				sv = input.nextLine();

				System.out.print("User (usually 'root' for MySQL, leave empty for SQLite): ");
				user = input.nextLine();

				System.out.print("Password (usually '' for MySQL, leave empty for SQLite): ");
				password = input.nextLine();

				System.out.print("Input Database (path to file.db/file.sqlite for SQLite): ");
				DB = input.nextLine();

				System.out.print("Output Database 1� half (path to file.db/file.sqlite for SQLite): ");
				DB1 = input.nextLine();
				System.out.print("Output Database 2� half (path to file.db/file.sqlite for SQLite): ");
				DB2 = input.nextLine();

				System.out.print("Split %: ");
				percent = input2.nextInt();

				System.out.print("Split type (disjoint or overlapping): ");
				splitType = input.nextLine();
				if (splitType.equalsIgnoreCase("overlapping")) {
					System.out.print("Records overlapping % (0% means disjoint, 100% means equal): ");
					percentOverlapping = input2.nextInt();
				}

				System.out.print("DBMS: ");
				DBMS = input.nextLine();

				System.out.print("Execution type (knapsack or baseline or both): ");
				executeWhat = switch (input.nextLine().toLowerCase()) {
					case "both" -> 0;
					case "knapsack" -> 1;
					default -> 2;
				};
			}

			long startingTime;
            ID = new SimpleDateFormat("ddMMyyyyHHmmssSSS").format(new Date());
			{
				File directory = new File(path+"Reports_" + ID);
				if (!directory.exists()) directory.mkdirs();
				for (String dir : new String[]{"DBonly", "txt files", "csv files"}) {
					directory = new File(path+"Reports_" + ID + "/"+ dir);
					if (!directory.exists()) directory.mkdirs();
				}
			}

			for (String db : dbsArray) {
				startingTime = System.currentTimeMillis();
				DB = db;
				fileName = DB;
				if (DBMS.equalsIgnoreCase("sqlite")) {
					fileName = fileName.replace("\\", "-").replace(".", "_")
							.replace("/", "-");
				}
				try {
					graph = new Graph(DBMS, sv, user, password, DB);
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
				for (int i : splitArray) {
					percent = i;

					for (int j : overlappingArray) {
						if (j == 0) {
							splitType = "disjoint";
							percentOverlapping = 0;
						} else {
							splitType = "overlapping";
							percentOverlapping = j; //goes to 0 if disjoint
						}
						System.out.println("Started execution for "+ DB +", split%: "+ i +"%" +
								", overlapping%: "+j+"%. (ID:"+ID+")");

						//Creates report
						report = new HashMap<>();
						for (String s : graph.listTables()) {
							int totalRecords = graph.getRecordNumberInTable(s);
							double expectedInDB2 = (double) totalRecords * (percent) / 100;
							double expectedInDB1 = totalRecords - expectedInDB2;
							int expectedOverlapping = (int) Math.round((Math.min(expectedInDB2, expectedInDB1) * percentOverlapping) / 100);
							nodeType type = graph.getTableType(s);
							report.put(s, new TableReport(s, totalRecords, type, expectedInDB1, expectedInDB2, expectedOverlapping,
									percentOverlapping, percent));
						}
						long execTime;
						//esecuzione
						//0 is yes, 1 is KS, 2 is Baseline
						switch (executeWhat) {
							case 0 -> {
								execTime = System.currentTimeMillis();
								DBSplit_from_higher.DBSplit(DBMS, sv, user, password, DB, DB1, DB2, percent, splitType, percentOverlapping, graph, true);
								System.out.println("\tDBSKnapsack completed in " + roundDecimal(((double) System.currentTimeMillis() - execTime) / 1000, 1000) + "s.");
								execTime = System.currentTimeMillis();
								DBSplit_Baseline.DBSplit(DBMS, sv, user, password, DB, DB1, DB2, percent, splitType, percentOverlapping, graph, true);
								System.out.println("\tBaseline completed in " + roundDecimal(((double) System.currentTimeMillis() - execTime) / 1000, 1000) + "s.");
							}
							case 1 -> {
								execTime = System.currentTimeMillis();
								DBSplit_from_higher.DBSplit(DBMS, sv, user, password, DB, DB1, DB2, percent, splitType, percentOverlapping, graph, true);
								System.out.println("\tDBSFromHigher completed in " + roundDecimal(((double) System.currentTimeMillis() - execTime) / 1000, 1000) + "s.");
							}
							case 2 -> {
								execTime = System.currentTimeMillis();
								DBSplit_Baseline.DBSplit(DBMS, sv, user, password, DB, DB1, DB2, percent, splitType, percentOverlapping, graph, true);
								System.out.println("\tBaseline completed in " + roundDecimal(((double) System.currentTimeMillis() - execTime) / 1000, 1000) + "s.");
							}
						}
						
						writeToFileCSV_slim(path+"Reports_" + ID + "/");
						writeToFileReadable(path+"Reports_" + ID + "/");
						System.out.println("Completed execution for "+ DB +", split% :"+ i +"%, " +
								"overlapping% :"+j+"%, in " +
								roundDecimal(((double) System.currentTimeMillis()-startingTime)/1000,1000)+ "s.\n. (ID:"+ID+")");
					}
				}
				getDBDataFromCsv( path+"Reports_" + ID + "/", "report_" + fileName + "(" + ID + ").csv", overlappingArray.length);
			}
			run = false;
		}
	}

    private static void getValues() {
        total_records = 0;
        expected_overlapping = 0;
        algorithm_total_time = 0;
        algorithm_total_knapsack = 0;
		baseline_total_time = 0;

		algorithm_total_variation = 0;
		algorithm_variation_DB1 = 0;
		algorithm_variation_DB2 = 0;

        algorithm_overlapping = 0;
		algorithm_perfectMatch_DB1 = 0;
		algorithm_perfectMatch_DB2 = 0;
		algorithm_perfectMatch_overlapping = 0;

		baseline_total_variation = 0;
		baseline_variation_DB1 = 0;
		baseline_variation_DB2 = 0;

        baseline_overlapping = 0;
        baseline_perfectMatch_DB1 = 0;
        baseline_perfectMatch_DB2 = 0;
		baseline_perfectMatch_overlapping = 0;

        for (String s: report.keySet()) {
            TableReport tr = report.get(s);
            total_records += tr.getTotalRecords();
            expected_overlapping += tr.getExpected_overlapping();
            algorithm_total_time += tr.getAlgorithm_runningTime();
            algorithm_total_knapsack += tr.getAlgorithm_knapsackTime();
            algorithm_total_variation += tr.getAlgorithm_variationToExpected_DB1() +
                    tr.getAlgorithm_variationToExpected_DB2();
			algorithm_variation_DB1 += tr.getAlgorithm_variationToExpected_DB1();
			algorithm_variation_DB2 += tr.getAlgorithm_variationToExpected_DB2();
			baseline_variation_DB1 += tr.getBaseline_variationToExpected_DB1();
			baseline_variation_DB2 += tr.getBaseline_variationToExpected_DB2();
            algorithm_overlapping += tr.getAlgorithm_overlappingRecords();
            baseline_total_time += tr.getBaseline_runningTime();
            baseline_total_variation += tr.getBaseline_variationToExpected_DB1() +
                    tr.getBaseline_variationToExpected_DB2();
            baseline_overlapping += tr.getBaseline_overlappingRecords();
            if (tr.getAlgorithm_variationToExpected_DB1()==0) ++algorithm_perfectMatch_DB1;
            if (tr.getAlgorithm_variationToExpected_DB2()==0) ++algorithm_perfectMatch_DB2;
			if (tr.getBaseline_variationToExpected_DB1()==0) ++baseline_perfectMatch_DB1;
			if (tr.getBaseline_variationToExpected_DB2()==0) ++baseline_perfectMatch_DB2;
			if (tr.getAlgorithm_overlappingRecords()- tr.getExpected_overlapping() ==0) ++algorithm_perfectMatch_overlapping;
			if (tr.getBaseline_overlappingRecords()- tr.getExpected_overlapping() ==0) ++baseline_perfectMatch_overlapping;
        }
    }

	private static String makeReadable() {
		String s = DB.toUpperCase()+", "+ graph.getTableNumber()+" tables ("+
				total_records+" records):\n\tPercent split : "+percent+"%, Overlapping: " + percentOverlapping+"%"
				+"\nRunningTime:";
		s += "\n\tAlgorithm:\t\t" + roundDecimal(algorithm_total_time, 1000) + "s, knapsack: "+roundDecimal(algorithm_total_knapsack, 1000)+
				"s (" + roundDecimal(algorithm_total_knapsack/algorithm_total_time*100, 100) +"%)";
		s += "\n\tBaseline:\t\t" +roundDecimal(baseline_total_time, 1000)+"s";
		s += "\n\tDifference:\t\t" + roundDecimal(algorithm_total_time-baseline_total_time, 1000)+"s " +
				((algorithm_total_time-baseline_total_time>0) ? "more" : "less") + " time required for algorithm";

		s += "\n\nVariation from expected:";
		s += "\n\tAlgorithm:\t\t"+
				((algorithm_total_variation>0) ? "+":"") +algorithm_total_variation +" records in tables ("+
				((algorithm_total_variation>0) ? "+":"")+
				roundDecimal(algorithm_total_variation/total_records*100, 100) +"%)";
		s += "\n\tBaseline:\t\t"+
				((baseline_total_variation>0) ? "+":"") +baseline_total_variation +" records in tables ("+
				((baseline_total_variation>0) ? "+":"")+
				roundDecimal(baseline_total_variation/total_records*100, 100) +"%)";

		s += "\nPerfect match:";
		s += "\n\tAlgorithm:\t\t"+ algorithm_perfectMatch_DB1 +",\t\t"+ algorithm_perfectMatch_DB2;
		s += "\n\tBaseline:\t\t"+ baseline_perfectMatch_DB1 +",\t\t"+ baseline_perfectMatch_DB2;

		s += "\n\nOverlapping:";
		s += "\nExpected:\t\t\t" + expected_overlapping + " records ("+percentOverlapping+"%)";
		s += "\nActual:\t\t\t";

		s += "\n\tAlgorithm:\t\t"+ algorithm_overlapping + "  ("+
				roundDecimal((algorithm_overlapping *100)/(total_records*(double)percent/100), 100) +"%)";
		s += "\n\tBaseline:\t\t"+ baseline_overlapping + " ("+
				roundDecimal((baseline_overlapping *100)/(total_records*(double)percent/100), 100) +"%)";

		s += "\nVariation from expected:";
		s += "\n\tAlgorithm:\t\t"+
				((algorithm_overlapping -expected_overlapping>0) ? "+":"") +(algorithm_overlapping -expected_overlapping) +" ("+
				roundDecimal((algorithm_overlapping -expected_overlapping)/expected_overlapping*100, 100) +"%)";
		s += "\n\tBaseline:\t\t"+
				((baseline_overlapping -expected_overlapping>0) ? "+":"") +(baseline_overlapping -expected_overlapping) +" ("+
				roundDecimal((baseline_overlapping -expected_overlapping)/expected_overlapping*100, 100) +"%)";

		s += "\nPerfect match:";
		s += "\n\tAlgorithm:\t\t"+ algorithm_perfectMatch_overlapping;
		s += "\n\tBaseline:\t\t"+ baseline_perfectMatch_overlapping;

		return s;
	}

	private static void writeToFileReadable(String path) {
		try {
			String filename = path+"txt files/report_" + fileName + "("+ID+").txt";
			File myObj = new File(filename);
			myObj.createNewFile();
			FileWriter myWriter = new FileWriter(filename, true);
			getValues();

			myWriter.write(makeReadable() + "\n\n");

			for (String str : report.keySet()){
				myWriter.write(report.get(str).toString() + "\n");
			}
			myWriter.write("\n\n_______________________________________________________________________\n\n");
			myWriter.close();
			//System.out.println("Successfully wrote to the file.");
		} catch (IOException e) {
			//System.out.println("An error occurred.");
			e.printStackTrace();
		}
	}

	private static void writeToFileCSV_slim (String path) {
		try {
			String filename = path+"csv files/report_" + fileName + "("+ID+").csv";
			File myObj = new File(filename);
			myObj.createNewFile();
			FileWriter myWriter = new FileWriter(filename, true);
			getValues();

			String top = "0,DB,#Tables,#records,percentSplit,percentageOverlapping,expectedOverlapping,"+
					"AlgorithmTime,algorithmTotalKnapsack,baselineTime," +
					"algorithmVariationToExpectedDB1,algorithmVariationToExpectedDB2," +
					"baselineVariationToExpectedDB1,baselineVariationToExpectedDB2," +

					"algorithmTotalOverlapping,baselineTotalOverlapping,"+

					"algorithmPerfectMatchDB1,algorithmPerfectMatchDB2,"+
					"baselinePerfectMatchDB1,baselinePerfectMatchDB2," +
					"algorithmPerfectMatchOverlapping,algorithmPerfectMatchOverlapping";



			String s = "2,"+ DB.toUpperCase() + "," + graph.getTableNumber() + "," +
					total_records + "," + percent + "%," + percentOverlapping + "%," +
					expected_overlapping + "," +

					roundDecimal(algorithm_total_time, 1000) + "," + roundDecimal(algorithm_total_knapsack, 1000) + "," +
					roundDecimal(baseline_total_time, 1000) + "," +

					((algorithm_variation_DB1 > 0) ? "+" : "" )+
					algorithm_variation_DB1 + "," +
					((algorithm_variation_DB2 > 0) ? "+" : "" )+
					algorithm_variation_DB2 + "," +
					((baseline_variation_DB1 > 0) ? "+" : "" )+
					baseline_variation_DB1 + "," +
					((baseline_variation_DB2 > 0) ? "+" : "" )+
					baseline_variation_DB2 + "," +

					algorithm_overlapping + "," + baseline_overlapping + "," +

					algorithm_perfectMatch_DB1 + "," + algorithm_perfectMatch_DB2 + "," +
					baseline_perfectMatch_DB1 + "," + baseline_perfectMatch_DB2+ "," +

					algorithm_perfectMatch_overlapping + "," + baseline_perfectMatch_overlapping;

			myWriter.write(top + "\n" + s + "\n\n");

			top = "0,Table,type,#records,percentageSplit,percentageOverlapping," +
					"algorithmTime,algorithmKnapsackTime,baselineTime," +

					"expectedDB1,expectedDB2," +
					"algorithmActualDB1,algorithmActualDB2," +
					"baselineActualDB1,baselineActualDB2," +

					"expectedOverlapping,"+
					"algorithmOverlappingRecords,"+
					"baselineOverlappingRecords";

			myWriter.write(top + "\n");
			for (String str : report.keySet()){
				TableReport tr = report.get(str);
				myWriter.write("1,"+ tr.toCsv_slim());
			}
			myWriter.write("\n\n");
			myWriter.close();
			//System.out.println("Successfully wrote to the file.");

		} catch (IOException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}

	}

	private static void getDBDataFromCsv(String path, String filename, int length){
		ArrayList<ArrayList<String>> map = new ArrayList<>();
		try {
			File myObj = new File(path +"csv files/"+ filename);
			Scanner myReader = new Scanner(myObj);
			while (myReader.hasNextLine()) {
				String line = myReader.nextLine();
				if (!line.isEmpty()) {
					String[] row = line.split(",");
					if (row[0].equalsIgnoreCase("0") && map.isEmpty()) {
						for (String s : row) {
							ArrayList<String> al = new ArrayList<>();
							al.add(s);
							map.add(al);
						}
					} else if (row[0].equalsIgnoreCase("2") && !map.isEmpty()) {
						int index = 0;
						for (ArrayList<String> al : map) {
							al.add(row[index]);
							index++;
						}
					}
				}
			}
			myReader.close();

			String filenameOut = path+"DBonly/" + filename.substring(0, filename.length()-4)+".txt";
			myObj = new File(filenameOut);
			myObj.createNewFile();
			FileWriter myWriter = new FileWriter(filenameOut, false);

			int index = 0;
			for (ArrayList<String> a : map)  {
				if (!a.isEmpty() && map.indexOf(a) != 0){
					myWriter.write(a.get(index) + " :\n ");
					//System.out.print(a.get(index) + " :\n\t ");
					++index;
					if (map.indexOf(a)<=3) {
						myWriter.write(a.get(index) + ", ");
						//System.out.print(a.get(index) + "\t\t ");
					} else {
						while (index < a.size()) {
							myWriter.write(a.get(index) + ", ");
							if (index % length == 0)
								myWriter.write("\n");
							//System.out.print(a.get(index) + ",\t\t ");
							++index;
						}
					}
					//System.out.print("\n");
					myWriter.write("\n");
				}
				index = 0;
			}
			myWriter.close();

		} catch (Exception e) {
			//System.out.println("An error occurred.");
			e.printStackTrace();
		}
	}


	private static void getConfig(){
		try {
			File myObj = new File("config.conf");
			Scanner myReader = new Scanner(myObj);

			while (myReader.hasNextLine()) {
				String line = myReader.nextLine();

				if (!line.isEmpty()) {
					String[] row = line.split("=");

					if (!row[0].equalsIgnoreCase("path") && row.length < 2)
						throw new RuntimeException("Argument is empty");

					switch (row[0].toLowerCase()) {
						case "dbms" -> {
							if (row[1].isEmpty())
								throw new RuntimeException("Argument dbms is empty");
							else DBMS = row[1];
						}
						case "sv" -> {
							if (row[1].isEmpty())
								throw new RuntimeException("Argument sv is empty");
							else sv = row[1];
						}
						case "user" -> {
							if (row[1].isEmpty())
								throw new RuntimeException("Argument user is empty");
							else user = row[1];
						}
						case "password" -> {
							if (row[1].isEmpty())
								throw new RuntimeException("Argument password is empty");
							else password = row[1];
						}
						case "path" -> {
							if (row.length > 1 && !row[1].isBlank()) {
								path = row[1];
								if (path.charAt(path.length()-1) != '/')
									path += "/";
							}
							else path = "";
						}
						case "dbs" -> {
							if (row[1].isEmpty())
								throw new RuntimeException("Argument dbs is empty");
							else {
								dbsArray = row[1].split(",");
							}
						}
						case "db1" -> {
							if (row[1].isEmpty())
								throw new RuntimeException("Argument db1 is empty");
							else DB1 = row[1];
						}
						case "db2" -> {
							if (row[1].isEmpty())
								throw new RuntimeException("Argument db2 is empty");
							else DB2 = row[1];
						}
						case "split" -> {
							if (row[1].isEmpty())
								throw new RuntimeException("Argument split is empty");
							else {
								String[] temp = row[1].split(",");
								splitArray = new int[temp.length];
								for (int i = 0; i < temp.length; ++i) {
									splitArray[i] = Integer.parseInt(temp[i]);
								}
							}
						}
						case "overlapping" -> {
							if (row[1].isEmpty())
								throw new RuntimeException("Argument overlapping is empty");
							else {
								String[] temp = row[1].split(",");
								overlappingArray = new int[temp.length];
								for (int i = 0; i < temp.length; ++i) {
									overlappingArray[i] = Integer.parseInt(temp[i]);
								}
							}
						}
						case "type" -> {
							if (row[1].isEmpty())
								throw new RuntimeException("Argument overlapping is empty");
							else executeWhat = Integer.parseInt(row[1]);
						}
					}
				}
			}
			myReader.close();
		} catch (Exception e) {
			//System.out.println("An error occurred.");
			e.printStackTrace();
		}
	}

	private static double roundDecimal(double number, int tens) {
		return (double) (Math.round(number*tens)) / tens;
	}
}

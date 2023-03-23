package src.Other;

import src.DBMutation.DBMutation;
import src.DBSBaseline.DBSplit_Baseline;
import src.DBSFromHigher.DBSplit_from_higher;
import src.DBSKnapsack.DBSplit_Knapsack;
import Graph.Graph;

import java.sql.SQLException;
import java.util.Scanner;

public class Main {
    /* Input reader  */
    private static final Scanner input = new Scanner(System.in).useDelimiter("\n");
    private static final Scanner input2 = new Scanner(System.in);

    /* Parameters */
    private static String DBMS; //used for connecting to DB via JDBC (e.g., oracle, mysql, sqlserver)
    private static String sv = "";
    private static String user = "";
    private static String password = "";
    private static String splitType = "";
    private static int percent = 0;
    private static int percentOverlapping = 0;
    private static String DB;
    private static String DB1;
    private static String DB2;

    static boolean prova = false;
    public static void main(String[] args) {
        System.out.println("Running DBSplit...\n");
        System.out.println("Arguments: <dbms> <server> <user> <password> <input-db> <output-db1> <output-db2> <perc-split>" +
                "<split-type> [<perc-overlapping>] <execution [bl-ks-fh]>\n");
        System.out.println("Example: mysql localhost:3306 user001 password001 testDB half1DB half2DB " +
                "30 overlapping 60 fh\n");
        boolean run = true;
        long numTables = 0;
        long time = 0;
        int errors = 0;
        short execution = 2;

        while (run) {
            if (prova) {
                DBMS = "sqlite";
                percent = 50;
                splitType = "overlapping";
                percentOverlapping = 50; //goes to 0 if disjoint
                execution = 2;
                run = false;

                switch (DBMS.toLowerCase()) {
                    case "sqlite" -> {
                        sv = "";
                        user = "";
                        password = "";
                        DB = "/home/ilan/Downloads/pokemon_db.db";
                        DB1 = "try1.db";
                        DB2 = "try2.db";
                    }
                    case "mysql" -> {
                        sv = "localhost:3306";
                        user = "root";
                        password = "Password_123";
                        DB = "pokemon_db";
                        DB1 = "DB_1";
                        DB2 = "DB_2";
                    }
                }

                //Input from args
            } else if ((args.length == 10 && args[7].equalsIgnoreCase("disjoint"))
                        || args.length == 11 && args[7].equalsIgnoreCase("disjoint")) {
                    int argIndex = 0;

                    DBMS = args[argIndex++];
                    System.out.println(DBMS);

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
                    if (splitType.equalsIgnoreCase("overlapping")) {
                        percentOverlapping = Integer.parseInt(args[argIndex++]);
                        System.out.println(percentOverlapping);
                    }

                    execution = switch (args[argIndex].toLowerCase()) {
                        case "bl", "baseline" -> 0;
                        case "ks", "knapsack" -> 1;
                        case "fh", "fromhigher", default -> 2;
                    };
                //user input
            } else {
                if (args.length > 0) {
                    System.out.println("Arguments were detected, but the number is incorrect.");
                    System.exit(1);//TODO not sure if thats a desired output, change it eventually
                }

                System.out.print("Input from keyboard\n\tDBMS: \n\t\t");
                DBMS = input.nextLine();

                System.out.print("\tServer name (usually 'localhost:3306' for MySQL or 'SQLEXPRESS' for SQLServer, leave empty for SQLite): \n\t\t");
                sv = input.nextLine();

                System.out.print("\tUser (usually 'root' for MySQL, leave empty for SQLite): \n\t\t");
                user = input.nextLine();

                System.out.print("\tPassword (usually '' for MySQL, leave empty for SQLite): \n\t\t");
                password = input.nextLine();

                System.out.print("\tInput Database (path to file.db/file.sqlite for SQLite): \n\t\t");
                DB = input.nextLine();

                System.out.print("\tOutput Database 1� half (path to file.db/file.sqlite for SQLite): \n\t\t");
                DB1 = input.nextLine();
                System.out.print("\tOutput Database 2� half (path to file.db/file.sqlite for SQLite): \n\t\t");
                DB2 = input.nextLine();

                System.out.print("\tSplit %: \n\t\t");
                percent = input2.nextInt();

                System.out.print("\tSplit type (disjoint/d or overlapping/ol): \n\t\t");
                splitType = input.nextLine();
                if (splitType.equalsIgnoreCase("overlapping") ||
                        splitType.equalsIgnoreCase("ol")) {
                    System.out.print("\tRecords overlapping % (0 means disjoint, 100 means equal): \n\t\t");
                    percentOverlapping = input2.nextInt();
                }

                System.out.print("\tExecution type (knapsack/ks or fromHigher/fh or baseline/bl): \n\t\t");
                execution = switch (input.nextLine().toLowerCase()) {
                    case "bl", "baseline" -> 0;
                    case "ks", "knapsack" -> 1;
                    case "fh", "fromhigher", default -> 2;
                };
            }

            Graph graph;
            try {
                graph = new Graph(DBMS, sv, user, password, DB);

                switch (execution) {
                    case 0 -> {
                        if (DBSplit_Baseline.DBSplit(DBMS, sv, user, password, DB, DB1, DB2, percent, splitType, percentOverlapping, graph, false))
                            run = false;
                        numTables = DBSplit_Baseline.numTables;
                        time = DBSplit_Baseline.getTime();
                        errors = DBSplit_Baseline.getErrors();
                    }
                    case 1 -> {
                        if (DBSplit_Knapsack.DBSplit(DBMS, sv, user, password, DB, DB1, DB2, percent, splitType, percentOverlapping, graph, false))
                            run = false;
                        numTables = DBSplit_Knapsack.numTables;
                        time = DBSplit_Knapsack.getTime();
                        errors = DBSplit_Knapsack.getErrors();

                    }
                    case 2 -> {
                        if (DBSplit_from_higher.DBSplit(DBMS, sv, user, password, DB, DB1, DB2, percent, splitType, percentOverlapping, graph, false))
                            run = false;
                        numTables = DBSplit_from_higher.numTables;
                        time = DBSplit_from_higher.getTime();
                        errors = DBSplit_from_higher.getErrors();
                    }
                }
            } catch (SQLException se) {
                System.out.println("Database information were somehow incorrect, connection to DB failed.");
            }

            // If run is still true something failed, user can choose to retry
            if (run)
                run = retry();
        }

        if (DBMS.equalsIgnoreCase("mysql")) {
            run = true;
            String risp;

            while (run) {
                System.out.println("Apply mutation to " + DB1 + " o " + DB2 + "? (y/n)");
                risp = input.nextLine();

                if (risp.equalsIgnoreCase("y")) {
                    String split;
                    String other;
                    do {
                        System.out.println("Type 1 for " + DB1 + " o 2 for " + DB2 + " to select which DB to mutate, " +
                                "or back per abort:");
                        split = input.nextLine();
                        if (split.equalsIgnoreCase("BACK")) break;

                        if (!split.equals("1") && !split.equals("2"))
                            System.out.println("Invalid input");

                    } while (!split.equals("1") && !split.equals("2"));

                    if (split.equalsIgnoreCase("BACK"))
                        break;

                    if (split.equalsIgnoreCase("1")) {
                        split = DB1;
                        other = DB2;
                    } else {
                        split = DB2;
                        other = DB1;
                    }
                    System.out.println("You are mutating: " + split + "\n");

                    DBMutation mutation = new DBMutation(DBMS, sv, user, password, split);
                    mutation.mutate();

                    System.out.println("Apply mutation to " + other + "? (y/n)");
                    do {
                        risp = input.nextLine();
                    } while (!risp.equalsIgnoreCase("y") &&
                            !risp.equalsIgnoreCase("n"));

                    if (risp.equals("y")) {
                        mutation = new DBMutation(DBMS, sv, user, password, other);
                        mutation.mutate();
                    }
                    run = false;

                } else if (risp.equalsIgnoreCase("n")) {
                    run = false;

                } else {
                    System.out.println("Invalid input");
                }
            }
        }

        System.out.println("DBSplit Completed...");
        System.out.println("Summary:");
        System.out.println("DB Name: " + DB);
        System.out.println("# Tables: " + numTables);
        System.out.println("1st Half DB Name: " + DB1);
        System.out.println("2nd Half DB Name: " + DB2);
        System.out.println("Split %: " + percent + "%");
        System.out.println("Split Type: " + splitType);
        if (splitType.equalsIgnoreCase("overlapping")) {
            System.out.println("Overlapping records %: " + percentOverlapping + "%"); }
        System.out.println("Execution Time (s): " + (double) time / 1000 + " s");
        System.out.println("Errors Found: " + errors);
    }

    public static boolean retry() {
        String choice="";
        do {
            System.out.println("Try again? (yes/no)");
            choice = input.nextLine();
            if (choice.equalsIgnoreCase("no")) {
                System.out.println("DBSplit aborted.");
                DBConnection.closeConn();
                System.exit(1);
                return false;
            } else if (choice.equals("yes")){
                return true;
            }
        } while (true);
    }
}

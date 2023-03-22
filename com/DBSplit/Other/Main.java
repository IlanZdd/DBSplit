package com.DBSplit.Other;

import com.DBSplit.DBSKnapsack.DBSplit;
import Graph.Graph;
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
    private static boolean knapsackExecution;

    static boolean prova = true;
    public static void main(String[] args) {
        System.out.println("Running DBSplit...\n");
        System.out.println("Arguments: <server> <user> <password> <input-db> <output-db1> <output-db2> <perc-split>" +
                "<split-type> [<perc-overlapping>] <dbms> <execution [baseline-knapsack]>\n");
        System.out.println("Example: localhost:3306 user001 password001 testDB half1DB half2DB " +
                "30 overlapping 60 mysql baseline\n");
        boolean run = true;

        while (run) {
            if (prova) {
                DBMS = "mysql";
                percent = 30;
                splitType = "overlapping";
                percentOverlapping = 50; //goes to 0 if disjoint
                knapsackExecution = true;
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

                knapsackExecution = args[argIndex].equalsIgnoreCase("knapsack");
                if (knapsackExecution)
                    System.out.println("knapsack");
                else
                    System.out.println("baseline");

                //user input
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

                System.out.print("Execution type (knapsack or baseline): ");
                knapsackExecution = input.nextLine().equalsIgnoreCase("knapsack");
            }

            Graph graph = new Graph(DBMS, sv, user, password, DB);

            if (knapsackExecution) {
                if (DBSplit.DBSplit(DBMS, sv, user, password, DB, DB1, DB2, percent, splitType, percentOverlapping, graph, false))
                    run = false;
            } else {
                if (com.DBSplit.DBSBaseline.DBSplit.DBSplit(DBMS, sv, user, password, DB, DB1, DB2, percent, splitType, percentOverlapping, graph, false))
                    run = false;
            }

            // If run is still true something failed, user can choose to retry
            if (run)
                run = retry();
        }
/*
        MUTATIONS
		run = true;
		risp = "";
		while(run) {
			System.out.println("Effettuare modifiche al database? (si/no)");
			risp = tastiera.nextLine();
			if(risp.equalsIgnoreCase("si")) {
				DBMutation.mutation(db);
				run = false;
			}else if(risp.equalsIgnoreCase("no")) {
				run = false;
			}
		}

 */
        System.out.println("DBSplit Completed...");
        System.out.println("Summary:");
        System.out.println("DB Name: " + DB);
        System.out.println("1st Half DB Name: " + DB1);
        System.out.println("2nd Half DB Name: " + DB2);
        System.out.println("# Tables: " + DBSplit.numTables);
        System.out.println("Split %: " + percent + "%");
        System.out.println("Split Type: " + splitType);
        if (splitType.equalsIgnoreCase("overlapping")) {
            System.out.println("Overlapping records %: " + percentOverlapping + "%"); }
        System.out.println("Execution Time (s): " + (double) DBSplit.time / 1000 + " s");
        System.out.println("Errors Found: " + DBSplit.getErrors());
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

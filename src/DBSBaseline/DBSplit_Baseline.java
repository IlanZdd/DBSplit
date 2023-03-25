package src.DBSBaseline;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import Graph.*;
import src.Other.*;

public class DBSplit_Baseline {
	/* Used by DBSplit */
	private static String query = "";
	private static Statement st = null;
	private static ResultSet rs = null;
	private static int n = -1, m = -1;

	/* For final print & reporting */
	private static int err = 0;
	public static long time = 0;//time
	public static long numTables = 0;
	public static boolean reporting;
	public static long startingTime; //Usato come seed per random

	/* DBSplit parameters */
	private static String DBMS; //used for connecting to DB via JDBC (e.g., oracle, mysql, sqlserver)
	private static String sv ="";
	private static String username = "";
	private static String password = "";
	private static int percent = 0;
	private static int percentOverlapping = 0;
	private static String originalDB;
	private static String DB1;
	private static String DB2;
	private static Graph graph;
	private static Set<String> hadToAddAPK;


	/** Entry point to the algorithm; will check for parameters validity, try to connect and create the DBs; then
	 * split.
	 * @param DBMS Database management system; supported MySQL and SQLite
	 * @param server_name "" for SQLite, localhost:3306 for mySql
	 * @param user Username to access the DB; "" for SQLite
	 * @param pwd Password to access the DB; "" for SQLite
	 * @param DB Original DB that will be split
	 * @param DB1 First half or the result
	 * @param DB2 Second half of the result
	 * @param percSplit Percentage of split
	 * @param splitType Type of Split: disjoint or overlapping
	 * @param percOverlapping Percentage of overlapping
	 * @param graph Working graph object from GraphSQL library
	 * @param reporting If true, a report will be generated
	 * @return TRUE if everything goes smooth; if any exception is thrown, false
	 */
	public static boolean DBSplit(String DBMS, String server_name, String user, String pwd,
							   String DB, String DB1, String DB2,
							   int percSplit, String splitType, int percOverlapping, Graph graph, boolean reporting) {
		DBSplit_Baseline.graph = graph;
		DBSplit_Baseline.reporting = reporting;

		//checks if parameters are alright
		if (!checkParameters(DBMS, server_name, user, pwd, DB, DB1, DB2, percSplit, splitType, percOverlapping))
			return false;

		System.out.print("Entered parameters: DBMS=" +DBMS + " server=" + sv + " username="+user+ " password=" +pwd +
				" original DB="+ DB + " 1st half DB=" + DB1 + " 2nd half DB=" +
				DB2 +" split type=" + splitType + " split %=" + percent);
		if (splitType.equalsIgnoreCase(("overlapping"))) System.out.print(" overlapping%=" +percentOverlapping);
		System.out.println(" execution=baseline");

		try {
			DBConnection.setConn(DBMS, sv, username, password, originalDB);
		} catch (Exception e) {
			System.out.println("Error: Invalid database parameters, connection failed");
			return false;
		}
		System.out.println("\nConnection to server started successfully!\n");

		if (!createSplitDatabases()) {
			hadToAddAPK = null;
			System.out.println("Error: Invalid database parameters, couldn't create split database");
			return false;
		} else System.out.println("Databases created successfully.");

		System.out.println("\nApplying DBSplit...\n");

		try {
			prepareSplit();
		} catch (RuntimeException re) {
			System.out.println("Split failed for "+DB+", split%:"+percSplit+", overlapping%:"+percOverlapping+".");
			return false;
		} finally {
			if (hadToAddAPK != null)
				removeAddedPks();
			hadToAddAPK = null;
			DBConnection.closeConn();
		}
		return true;
	}

	/** Check if parameters are legal and assigns them
	 * @param databaseMS DBMS: sqlServer, MySQL, SQLite
	 * @param server_name Server_name
	 * @param user Username for DBMS
	 * @param pwd Password for DBMS
	 * @param DB Original database
	 * @param DB1 First half of the split
	 * @param DB2 Second half of the split
	 * @param percSplit Percentage of splitting
	 * @param split_type Type of splitting: dijoint or overlapping
	 * @param percOverlapping If type is overlapping its percentage
	 * @return TRUE if everything is legal
	 */
	private static boolean checkParameters(String databaseMS, String server_name, String user, String pwd,
										   String DB, String DB1, String DB2,
										   int percSplit, String split_type, int percOverlapping) {
		if(!databaseMS.equalsIgnoreCase("mysql") &&
				!databaseMS.equalsIgnoreCase("sqlserver") &&
				!databaseMS.equalsIgnoreCase("sqlite")) {
			System.out.println("Error: Invalid or non-supported DBMS");
			return false;
		} else
			DBMS = databaseMS;

		if (percSplit < 0 || percSplit > 100) {
			System.out.println("Error: Invalid split %");
			return false;
		} else
			percent = percSplit;

		if (!split_type.equalsIgnoreCase("disjoint") && !split_type.equalsIgnoreCase("overlapping")){
			System.out.println("Error: Invalid split type");
			return false;
		}

		if (split_type.equalsIgnoreCase("overlapping")) {
			if (percOverlapping < 0 || percOverlapping > 100) {
				System.out.println("Error: Invalid records in common %");
				return false;
			} else percentOverlapping = percOverlapping;
		} else percentOverlapping = 0;

		sv = server_name;
		username = user;
		password = pwd;
		originalDB = DB;
		DBSplit_Baseline.DB1 = DB1;
		DBSplit_Baseline.DB2 = DB2;

		return true;
	}
	/** Clones database DB in DB1, DB2 with tables and records
	 * @return	True if clone is successful
	 */
	private static boolean createSplitDatabases() {
		startingTime = System.currentTimeMillis();
		//create the graph

		switch (DBMS.toLowerCase()) {
			case "sqlite":
				try {
					Files.copy(Paths.get(originalDB), Paths.get(DB1), StandardCopyOption.REPLACE_EXISTING);
					Files.copy(Paths.get(originalDB), Paths.get(DB2), StandardCopyOption.REPLACE_EXISTING);

					//this operation is needed for trickytripper app only, to handle with a problematic table which is emptied
					if (originalDB.contains("trickytripper")) {

						DBConnection.setConn(DBMS, sv, username, password, DB1);
						query = "DELETE FROM rel_payment_participant";
						st = DBConnection.getConn().createStatement();
						st.execute(query);

						DBConnection.setConn(DBMS, sv, username, password, DB2);
						query = "DELETE FROM rel_payment_participant";
						st = DBConnection.getConn().createStatement();
						st.execute(query);
					}
					return true;

				} catch (Exception e) {
					System.out.println("Error: " + e.getMessage());
					return false;
				}

			case "mysql": case "sqlserver":
				try {
					//WARNING: maybe it is not necessary, but dbo schema is needed by some sql server databases to access to tables
					if (DBMS.equalsIgnoreCase("sqlserver")) {
						originalDB += ".dbo";
						DB1 += ".dbo";
						DB2 += ".dbo";
					}

					//CREATE DB1, DB2 and clones the tables
					Statement st;
					st = DBConnection.getConn().createStatement();
					st.executeUpdate("DROP DATABASE IF EXISTS " + DB1);
					st.executeUpdate("DROP DATABASE IF EXISTS " + DB2);
					st.executeUpdate("CREATE DATABASE " + DB1);
					st.executeUpdate("CREATE DATABASE " + DB2);

					for (String t : graph.listTables()) {
						cloneTable(t, t, DB1);
						cloneTable(t, t, DB2);
					}

					Map<String, List<ForeignKeyColumn>> map = graph.listArcs();
					for (String t: map.keySet()) {
						for (ForeignKeyColumn fk: map.get(t)) {
							query = "ALTER TABLE " + DB1 +"."+t +
									" ADD FOREIGN KEY (" + fk.getName() +
									") REFERENCES " + fk.getReferredTable() +
									"(" + fk.getReferredPrimaryKey() +
									") ON DELETE " + fk.getOnDelete() +
									" ON UPDATE " + fk.getOnUpdate();

							st = DBConnection.getConn().createStatement();
							st.executeUpdate(query);
							query = "ALTER TABLE " + DB2 +"."+t +
									" ADD FOREIGN KEY (" + fk.getName() +
									") REFERENCES " + fk.getReferredTable() +
									"(" + fk.getReferredPrimaryKey() +
									") ON DELETE " + fk.getOnDelete() +
									" ON UPDATE " + fk.getOnUpdate();
							st = DBConnection.getConn().createStatement();
							st.executeUpdate(query);
						}
					}
					return true;
				} catch (SQLException se) {
					System.out.println("Error: " + se.getMessage());
					return false;
				}
			default:
				return false;
		}
	}


	/** Evokes split function on each table.
	 */
	private static void prepareSplit() {
		try {
			//Scorre i nomi delle tabelle ed effettua lo split
			// Since constraint stops the split if something is referring them, you need to start from sources
			for (String table: graph.sortTopological()) {
				numTables++;
				startingTime = System.currentTimeMillis();
				split(table);
				time = time + getTime();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			DBConnection.closeSt(st);
			DBConnection.closeRs(rs);
		}
	}

	/** Splits the given table in the databases DB1, DB2 according
	 * to requests
	 * @param table 	Table to split
	 */
	private static void split(String table) {
		long startTime = System.currentTimeMillis();

		int totalRecords;
		switch (DBMS.toLowerCase()) {
			case "sqlite" -> {
				if (table.equalsIgnoreCase("android_metadata") ||
						table.equalsIgnoreCase("sqlite_sequence") ||
						table.equalsIgnoreCase("cache"))
					return;//default tables are not split

				String recordsToDelete = "";
				try {
					//Computes parameters
					totalRecords = graph.getRecordNumberInTable(table);
					computeParameters(percent, totalRecords); //calcola n, m

					//Finds c based on lower splitting parameter
					int c;
					if (n <= m) {
						c = (int) Math.round((double) (n * percentOverlapping) / 100);
					} else {
						c = (int) Math.round((double) (m * percentOverlapping) / 100);
					}
					n -= c;
					m -= c;

					//System.out.println(table + ": c is "+c);
					//IF OVERLAPPING: creates a temporary table "temp" cloning the given table and removes
					// c common records; records in table will be deleted from split DBs based on n,m
					// else willrecordsToDelete delete all records in the original table
					/*if (percentOverlapping != 0) {
						recordsToDelete = "temp";
						if (cloneTable(table, recordsToDelete, originalDB)) {
							//deleteRandomRecords(recordsToDelete, c, originalDB, table);
							deleteRecordsFromTable(recordsToDelete, n, 0, originalDB);
						}
					} else*/
					recordsToDelete = table;

					List<Integer> rowids = getAllRowID_SQLite(originalDB, recordsToDelete);
					if (c > 0) {
						rowids.subList(0, c).clear();
					}

					deleteRecordsFromTable(table, n, DB1, rowids, false);
					deleteRecordsFromTable(table, c, DB1, rowids, true);
					deleteRecordsFromTable(table, c+m, DB2, rowids, false);

					//boolean is true if odd records
					//remaining records will be lost
					//delete records from tables
					/*boolean boo = (totalRecords % 2 == 1);
					int remainingRecords = (totalRecords - c - n - m);

					deleteRecordsFromTable(table, n, DB1, rowids, false);
					//System.out.println(table + ": after call 1 there are "+rowids.size() + " records in the list.");
					if (percentOverlapping > 0) {
						deleteRecordsFromTable(table, (boo ? remainingRecords + 1 : remainingRecords), DB1, rowids, true);
						//System.out.println(table + ": after call 1.5 there are "+rowids.size() + " records in the list.");
					}
					deleteRecordsFromTable(table, m + remainingRecords, DB2, rowids, false);*/
					//System.out.println(table + ": after call 2 there are "+rowids.size() + " records in the list.");
					//System.out.println();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					/*if (recordsToDelete.equals("temp"))
						dropTable(recordsToDelete);*/

					time = System.currentTimeMillis() - startTime;
				}
			}
			case "mysql", "sqlserver" -> {
				String recordsToDelete = "";
				try {
					//Computes parameters
					totalRecords = graph.getRecordNumberInTable(table);
					computeParameters(percent, totalRecords); //calcola n, m

					//Finds c based on lower splitting parameter
					int c;
					if (n <= m) {
						c = (int) Math.round((double) (n * percentOverlapping) / 100);
					} else {
						c = (int) Math.round((double) (m * percentOverlapping) / 100);
					}
					n -= c;
					m -= c;

					deleteRecordsFromTable(table, 0, n+c, DB1);
					deleteRecordsFromTable(table, n, c+m, DB2);

					/*
					//IF OVERLAPPING: creates a temporary table "temp" cloning the given table and removes
					// c common records; records in table will be deleted from split DBs based on n,m
					// else will delete all records in the original table
					if (percentOverlapping != 0) {
						recordsToDelete = "temp";
						if (cloneTable(table, recordsToDelete, originalDB)) {
							deleteRandomRecords(recordsToDelete, c, originalDB, table);
						}
					} else
						recordsToDelete = table;

					//boolean is true if odd records
					//remaining records will be lost
					//delete records from tables; records are randomized  according to starting time
					//	at every iteration of DBSplit but stable during execution
					boolean boo = (totalRecords % 2 == 1);
					int remainingRecords = (totalRecords - c - n - m);

					deleteRecordsFromTable(table, recordsToDelete, 0, n, DB1);
					if (percentOverlapping > 0)
						deleteRecordsFromTable(table, recordsToDelete, m, (boo ? remainingRecords + 1 : remainingRecords), DB1);
					deleteRecordsFromTable(table, recordsToDelete, n, m + remainingRecords, DB2);
*/
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					//Deletes temp if exists
					//if (recordsToDelete.equalsIgnoreCase("temp")) { dropTable(recordsToDelete); }

					DBConnection.closeSt(st);
					DBConnection.closeRs(rs);
					time = System.currentTimeMillis() - startTime;
				}
			}
			default -> {
			}
		}
	}

	/**
	 * Compute n, m parameter indicating quantity of desired records,
	 * based on split percentage and total number of records
	 * @param percentage	Split percentage
	 * @param tot_record	Total number of records in table
	 */
	private static void computeParameters(int percentage, int tot_record) {
		n = (int) Math.round((double)(tot_record * percentage)/100);
		m = tot_record - n;
	}

	/** Creates a clone of the given table in the specified database
	 * with the same records
	 * @param table	Table to clone
	 * @param newTable Cloned table
	 * @param DB DB where table is going to be
	 * @return	TRUE if successful
	 */
	private static boolean cloneTable(String table, String newTable, String DB){
		try {
			if (!DB.equals(originalDB) &&
					!DB.equals(DB1) &&
					!DB.equals(DB2))
				throw new SQLException("Database in input is wrong");

			switch (DBMS.toLowerCase()) {
				case "sqlite" -> {
					DBConnection.setConn(DBMS, sv, username, password, originalDB);

					query = "CREATE TABLE " + newTable + " AS SELECT * FROM " + table;

					st = DBConnection.getConn().createStatement();
					st.execute(query);
					DBConnection.closeSt(st);
					DBConnection.closeConn();
				}
				case "mysql" -> {
					query = "CREATE TABLE " + DB + "." + newTable + " LIKE " + originalDB + "." + table;

					st = DBConnection.getConn().createStatement();
					st.execute(query);

					if (!graph.hasPrimaryKeyInTable(table)) {
						query = "ALTER TABLE " + DB + "." + newTable +
								" ADD COLUMN thereShouldHaveBeenAPK int AUTO_INCREMENT PRIMARY KEY";
						st.execute(query);

						if (hadToAddAPK == null)
							hadToAddAPK = new TreeSet<>();
						hadToAddAPK.add(table);
					}

					String insertQuery = "INSERT INTO " + DB + "." + newTable + "(";
					String fields = "";
					for (Column column : graph.getColumnsInTable(table)) {
						fields += column.getName() + ",";
					}
					fields = fields.substring(0, fields.length()-1);
					insertQuery += fields + ") SELECT " + fields + " FROM " + originalDB + "." + table;

					st = DBConnection.getConn().createStatement();
					st.execute(insertQuery);
					DBConnection.closeSt(st);
				}
				case "sqlserver" -> {
					query = "SELECT * INTO " + DB + "." + newTable + " FROM " + originalDB + "." + table;
					st = DBConnection.getConn().createStatement();
					st.execute(query);
					DBConnection.closeSt(st);
				}
				default -> throw new IllegalArgumentException("DBMS not supported");
			}

			return true;
		} catch (SQLException se) {
			System.out.println("Error: " + se.getMessage());
			System.out.println("Error: Table " + newTable + " has not been created");
			se.printStackTrace();

			err++;
			DBConnection.closeSt(st);
			return false;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	private static void removeAddedPks() {
		try {
			if (DBMS.equalsIgnoreCase("mysql")) {
				for (String table : hadToAddAPK) {
					st = DBConnection.getConn().createStatement();
					st.execute("ALTER TABLE " + DB1 + "." + table +" DROP PRIMARY KEY, CHANGE thereShouldHaveBeenAPK thereShouldHaveBeenAPK INT");
					st.execute("ALTER TABLE " + DB2 + "." + table +" DROP PRIMARY KEY, CHANGE thereShouldHaveBeenAPK thereShouldHaveBeenAPK INT");

					st.execute("ALTER TABLE "+ DB1 + "." + table + " DROP COLUMN thereShouldHaveBeenAPK");
					st.execute("ALTER TABLE "+ DB2 + "." + table + " DROP COLUMN thereShouldHaveBeenAPK");
				}
			}
		} catch (SQLException se) {
			System.out.println("Error is removing PKs: " + se.getMessage());
		}
	}

	/** Deletes n random records from the given table in given database
	 * @param table Table name
	 * @param n	Number of records to delete
	 * @param DB Database
	 * @implNote Isn't this the same as the other delete method, only with one parameter?
	 */
	private static void deleteRandomRecords (String table, int n, String DB, String tableName) {
		try {
			if(!DB.equals(originalDB) && !DB.equals(DB1) && !DB.equals(DB2))
				throw new SQLException("Database is wrong");

			switch (DBMS.toLowerCase()) {
				case "sqlite" -> {
					DBConnection.setConn(DBMS, sv, username, password, DB);

					query = "DELETE FROM " + table +
							" WHERE ROWID IN (SELECT ROWID FROM " + table +
							" ORDER BY RANDOM() LIMIT " + n + ")";
					st = DBConnection.getConn().createStatement();
					st.execute(query);

					DBConnection.closeSt(st);
					DBConnection.closeConn();
				}
				case "mysql" -> {
					String pk = getPrimaryKeys(tableName, false);

					if (!graph.hasPrimaryKeyInTable(tableName)) {
						query = "WITH t AS (SELECT * FROM " + originalDB +"."+ tableName +
								" ORDER BY RAND("+startingTime+") LIMIT 0," + n + ")" +
								" DELETE FROM " + DB +"."+ table +
								" USING "+ DB +"."+ table + ", t WHERE ";
						for (Column column: graph.getColumnsInTable(tableName)) {
							if (column.isNullable())
								query += "( " + table +"."+ column.getName() + " is NULL or " +
										table +"."+ column.getName() +" = t."+ column.getName() +")";
							else
								query += table +"."+ column.getName() +" = t."+ column.getName();
							query += " and ";
						}
						query = query.substring(0, query.length()-5);

					} else {
						query = "DELETE FROM " + DB + "." + table + " WHERE ";

						if (graph.isPrimaryKeyComposedInTable(tableName))
							query += "(" + pk + ")";
						else query += pk;

						query += " IN (SELECT " + pk + " FROM " +
								"(SELECT " + pk + " FROM " + originalDB + "." + table +
								" ORDER BY RAND() LIMIT 0," + n + ") t);";
					}
					st = DBConnection.getConn().createStatement();
					st.execute(query);
					DBConnection.closeSt(st);
				}
				case "sqlserver" -> {
					/*
				if(n<=0)//this to avoid insert in empty table, since FETCH command arise problems when N<=0
					return;
				if(DBnum.equals("0"))
					query = "INSERT INTO "+ DB+"."+newTable +" SELECT* FROM "+ DB+"."+table +
							" ORDER BY newid() OFFSET 0 ROWS FETCH NEXT " + n + " ROWS ONLY";
				else if(DBnum.equals("1"))
					query = "INSERT INTO "+ DB1+"."+newTable +" SELECT* FROM "+ DB+"."+table +
							" ORDER BY newid() OFFSET 0 ROWS FETCH NEXT " + n + " ROWS ONLY";
				else
					query = "INSERT INTO "+ DB2+"."+newTable +" SELECT* FROM "+ DB+"."+table +
							" ORDER BY newid() OFFSET 0 ROWS FETCH NEXT " + n + " ROWS ONLY";
				st = DBConnection.getConn().createStatement();
				st.execute(query);
			*/
				}
				default -> throw new IllegalArgumentException("DBMS not supported");
			}
		} catch (SQLException se) {
			System.out.println("Error: " + se.getMessage());
			System.out.println(query);
			//System.out.println("Records in table " + table + " have not been deleted");
			err++;
		} catch (Exception e) {
			//System.out.println("Table "+table+ " in "+graph.listTables().toString());
			e.printStackTrace();
			System.exit(1);
		}
	}

	/** Gets rowid from a given table in a given DB
	 * @param DB Database
	 * @param table tabel name
	 * @return List of row id for table in database
	 */
	private static List<Integer> getAllRowID_SQLite(String DB, String table) {
		List<Integer> list = new ArrayList<>();
		try {
			DBConnection.setConn(DBMS, sv, username, password, DB);
			query = "SELECT ROWID FROM " + table + " ORDER BY RANDOM()";
			st = DBConnection.getConn().createStatement();
			rs = st.executeQuery(query);
			while (rs.next()) {
				list.add(rs.getInt(1));
			}
			DBConnection.closeSt(st);
			DBConnection.closeConn();
			return list;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/** Delete n records from given table from a list of rowids; records
	 * are removed from the list if keepRecords is false
	 * @param table	Table name
	 * @param n	Quantity of records to delete
	 * @param DB	Database
	 * @param rowids List of all rowids not yet removed
	 * @param keepRecords if TRUE records deleted arent removed from the list
	 */
	private static void deleteRecordsFromTable(String table, int n, String DB, List<Integer> rowids, boolean keepRecords) {
		try {
			DBConnection.setConn(DBMS, sv, username, password, DB);
			query = "DELETE FROM " + table + " WHERE ROWID IN (" + print(rowids, n, keepRecords) + ")";
			//System.out.println(table + "(" +DB+"): "+query);
			st = DBConnection.getConn().createStatement();
			st.execute(query);

			DBConnection.closeSt(st);
			DBConnection.closeConn();
		} catch (Exception ignored) { }
	}

	/**
	 * Deletes n records from given table skipping the first m records.
	 *
	 * @param table Table where records will be deleted
	 * @param n     Records to delete
	 * @param m     Offset
	 * @param DB    Database
	 */
	private static void deleteRecordsFromTable(String table, int n, int m, String DB) {
		try{
			if (!DB.equals(originalDB) && !DB.equals(DB1) && !DB.equals(DB2))
				throw new SQLException("Database is wrong");

			switch (DBMS.toLowerCase()) {
				case "sqlite" -> {
					DBConnection.setConn(DBMS, sv, username, password, DB);

					query = "DELETE FROM " + table +
							" WHERE ROWID IN (SELECT ROWID FROM " + table +
							" ORDER BY RANDOM() LIMIT " + n + ")";
					st = DBConnection.getConn().createStatement();
					st.execute(query);

					DBConnection.closeSt(st);
					DBConnection.closeConn();
				}
				case "mysql" -> {
					String pk = getPrimaryKeys(table, false);

						query = "DELETE FROM " + DB + "." + table + " WHERE ";
						if (graph.isPrimaryKeyComposedInTable(table))
							query += "(" + pk + ")";
						else query += pk;

						query += " IN (SELECT " + pk + " FROM " +
								"(SELECT " + pk + " FROM " + DB + "." + table +//+ originalDB + "." + table +
								" ORDER BY RAND(" + startingTime + ") LIMIT " + n + "," + m + ") t);";

					st = DBConnection.getConn().createStatement();
					st.execute(query);
				}
				case "sqlserver" -> {
					/*For sqlServer
					//sql server does not work with LIMIT
					else if(DBMS.equalsIgnoreCase("sqlserver")) {
						if(n<=0)//this to avoid insert in empty table, since FETCH command arise problems when N<=0
							return;
						if(DBnum.equals("0"))
							query = "INSERT INTO "+ DB+"."+newTable +" SELECT* FROM "+ DB+"."+table +
									" ORDER BY newid() OFFSET 0 ROWS FETCH NEXT " + n + " ROWS ONLY";
						else if(DBnum.equals("1"))
							query = "INSERT INTO "+ DB1+"."+newTable +" SELECT* FROM "+ DB+"."+table +
									" ORDER BY newid() OFFSET 0 ROWS FETCH NEXT " + n + " ROWS ONLY";
						else
							query = "INSERT INTO "+ DB2+"."+newTable +" SELECT* FROM "+ DB+"."+table +
									" ORDER BY newid() OFFSET 0 ROWS FETCH NEXT " + n + " ROWS ONLY";
						st = DBConnection.getConn().createStatement();
						st.execute(query);
					}
					 */
				}
				default -> {
				}
			}
		} catch (SQLException se) {
			System.out.println("Records in table " + table + " have not been deleted");
			/*if (graph.getForeignKeysInTable(table) != null) {
				for (ForeignKeyColumn fk : graph.getForeignKeysInTable(table)) {
					System.out.println("\tON DELETE " + fk.getName() +": "+fk.getOnDelete());
				}
			}*/
		} catch (Exception e) {
			e.printStackTrace();
		} finally { DBConnection.closeSt(st); }
	}

	/** Concats 'limit' rowids from a list, removing them if KeepRedcords is false
	 * @param rowids List of rowids
	 * @param limit	Number of rowids to concats, if out of bound is all rowids in list
	 * @param keepRecords if TRUE rowids aren't removed from list
	 * @return String of rowids, separated by commas
	 */
	private static String print(List<Integer> rowids, int limit, boolean keepRecords) {
		StringBuilder rowidsString = new StringBuilder();
		if (limit < 1 || limit>rowids.size())
			limit = rowids.size();
		int startingSize = rowids.size();
		int i = 0;
		while (i < limit) {
			if (keepRecords)
				rowidsString.append(rowids.get(i)).append(", "); //build the query string of N rowids to remove from tab1
			else rowidsString.append(rowids.remove(0)).append(", "); //build the query string of N rowids to remove from tab1
			++i;
		}

		if (startingSize>0)
			rowidsString = new StringBuilder(rowidsString.substring(0, rowidsString.length() - 2));//remove last ','

		return rowidsString.toString();
	}


	/** Drops table with given name from DB
	 * @param table Table name
	 */
	private static void dropTable(String table) {
		Statement st;
		try {
			switch (DBMS) {
				case "sqlite" -> {
					DBConnection.setConn(DBMS, sv, username, password, originalDB);
					String query = "DROP TABLE " + table;
					st = DBConnection.getConn().createStatement();
					st.executeUpdate(query);
					DBConnection.closeSt(st);
					DBConnection.closeConn();
				}
				case "sqlserver", "mysql" -> {
					String query = "DROP TABLE " + originalDB + "." + table;
					st = DBConnection.getConn().createStatement();
					st.executeUpdate(query);
					DBConnection.closeSt(st);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}




	/* it inserts into a table (newTable) N/M records from original table given a starting record number to add from
	 */
	private static void insertIntoTable(String table, String newTable, int n, int m, String DB) {
		try {
			if(!DB.equals(originalDB) && !DB.equals(DB1) && !DB.equals(DB2))
				throw new SQLException("Database is wrong");

			switch (DBMS.toLowerCase()) {
				case "mysql" -> {
					query = "INSERT INTO " + DB + "." + newTable + " SELECT* FROM " + originalDB + "." + table + " LIMIT " + n + "," + m;

					st = DBConnection.getConn().createStatement();
					st.execute(query);
				}
				case "sqlserver" -> {
					if (m <= 0)//this to avoid to insert in empty table, since FETCH command arises problems when M<=0
						return;
					String db = DB.split("\\.")[0]; //since we have DB.dbo

					//find primary keys
					StringBuilder primaryKeysQuery = new StringBuilder(" ORDER BY ");
					DatabaseMetaData metaData = DBConnection.getConn().getMetaData();
					try (ResultSet tables = metaData.getTables(db, "dbo", "%", new String[]{"TABLE"})) {
						while (tables.next()) {
							if (!tables.getString("TABLE_NAME").equals(table))
								continue;
							String catalog = tables.getString("TABLE_CAT");
							String schema = tables.getString("TABLE_SCHEM");
							String tableName = tables.getString("TABLE_NAME");
							int numPrimaryKeys = 0; //this is used in case of tables with no primary keys. in that case, newid() is used instead

							try (ResultSet primaryKeys = metaData.getPrimaryKeys(catalog, schema, tableName)) {
								while (primaryKeys.next()) {
									numPrimaryKeys++;
									primaryKeysQuery.append(primaryKeys.getString("COLUMN_NAME")).append(",");
								}
								if (numPrimaryKeys == 0)
									primaryKeysQuery = new StringBuilder(" ORDER BY newid() ");
								else
									primaryKeysQuery = new StringBuilder(primaryKeysQuery.substring(0, primaryKeysQuery.length() - 1)); //remove last ,
							}
						}
					}
					query = "INSERT INTO " + DB + "." + newTable + " SELECT* FROM " + originalDB + "." + table +
							primaryKeysQuery + " OFFSET " + n + " ROWS FETCH NEXT " + m + " ROWS ONLY";
					st = DBConnection.getConn().createStatement();
					st.execute(query);
				}
				default -> throw new IllegalArgumentException("DBMS not supported");
			}
		} catch(SQLException se) {
			System.out.println("Error: " + se.getMessage());
			System.out.println("Records in table " + newTable + " have not been inserted");
			dropTable(newTable);
			err++;
		} finally {
			DBConnection.closeSt(st);
		}
	}

	/* it inserts into a table (newTable) N random records from original table
	 */
	private static void insertIntoTable(String table, String newTable, int n, String DB) {
		try {
			if(!DB.equals(originalDB) && !DB.equals(DB1) && !DB.equals(DB2))
				throw new SQLException("Database is wrong");

			switch (DBMS.toLowerCase()) {
				case "mysql" -> {
					query = "INSERT INTO " + DB + "." + newTable +
							" SELECT* FROM " + originalDB + "." + table + " ORDER BY RAND() LIMIT " + n;

					st = DBConnection.getConn().createStatement();
					st.execute(query);
				}
				case "sqlserver" -> {
					if (n <= 0)//this to avoid insert in empty table, since FETCH command arise problems when N<=0
						return;
					query = "INSERT INTO " + DB + "." + newTable +
							" SELECT* FROM " + originalDB + "." + table +
							" ORDER BY newid() OFFSET 0 ROWS FETCH NEXT " + n + " ROWS ONLY";

					st = DBConnection.getConn().createStatement();
					st.execute(query);
				}
				default -> throw new IllegalArgumentException("DBMS not supported");
			}
		} catch (SQLException se) {
			System.out.println("Error: " + se.getMessage());
			System.out.println("Records in table " + newTable + " have not been inserted");
			dropTable(newTable);
			err++;
		} finally {
			DBConnection.closeSt(st);
		}
	}


	/** Returns number of errors
	 * @return Number of errors
	 */
	public static int getErrors() {
		return err;
	}

	/** Returns execution time
	 * @return execution time
	 */
	public static long getTime() {
		return time;
	}
	public static double getNowTime() {
		return ((double) System.currentTimeMillis()-startingTime)/1000;
	}

	private static String getPrimaryKeys (String table, boolean parenthesis) {
		if (graph.hasPrimaryKeyInTable(table)) {
			if (parenthesis)
				return graph.getPrimaryKeysStringInTable(table, ',', "(", ")");
			else return graph.getPrimaryKeysStringInTable(table, ',', "", "");
		} else return "thereShouldHaveBeenAPK";
	}
}
package src.DBSFromHigher;

import Graph.*;
import src.Other.DBConnection;
import src.Other.MainDebug;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.rmi.UnexpectedException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class DBSplit_from_higher {

	/* Used by DBSplit */
	private static String query = "";
	private static Statement st = null;
	private static ResultSet rs = null;
	private static int n = -1, m = -1, c = -1, u = -1;

	/* Final print purpose & reporting */
	private static int err = 0;
	public static long time = 0;//time
	public static long numTables = 0;
	public static long startingTime = System.currentTimeMillis();
	protected static boolean reporting;

	/* DBSplit parameters */
	protected static String DBMS; //used for connecting to DB via JDBC (e.g., oracle, mysql, sqlserver)
	protected static String sv ="";
	protected static String username = "";
	protected static String password = "";
	private static int percent = 0;
	private static int percentOverlapping = 0;
	protected static String DB1;
	protected static String DB2;
	protected static String originalDB;

	/* Algorithm required */
	protected static Graph graph;
	private  static boolean inverting = false;
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
		DBSplit_from_higher.graph = graph;
		DBSplit_from_higher.reporting = reporting;

		//checks if parameters are alright
		if (!checkParameters(DBMS, server_name, user, pwd, DB, DB1, DB2, percSplit, splitType, percOverlapping))
			return false;

		System.out.print("Entered parameters: server=" +sv + " username="+user + " password=" +pwd +
				" original DB="+DB + " 1� half DB="+DB1 + " 2� half DB="+DB2 +
				" split type="+splitType + " split %="+percent + " execution=alternative");

		// Tries to connect to the original DB for the first time
		try {
			DBConnection.setConn(DBMS, sv, username, password, originalDB);
		} catch (Exception e) {
			++err;
			System.out.println("Error: Invalid database parameters, connection failed");
			return false;
		}
		System.out.println("\nConnection to server started successfully!\n");

		//creates split dbs
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
			re.printStackTrace();
			return false;
		} finally {
			if (hadToAddAPK != null) {
				removeAddedPks();
				hadToAddAPK = null;
			}
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
	 * @param split_type Type of splitting: disjoint or overlapping
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

		// If percSplit is over 50, will consider its complementary
		if (percSplit > 50) {
			percent = 100 - percSplit;
			DBSplit_from_higher.DB1 = DB2;
			DBSplit_from_higher.DB2 = DB1;
			inverting = true;
		} else {
			DBSplit_from_higher.DB1 = DB1;
			DBSplit_from_higher.DB2 = DB2;
		}
		return true;
	}


    /** Clones database DB in DB1, DB2 with tables and records
     * @return	True if clone is successful
     */
    public static boolean createSplitDatabases() {

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
			case "sqlserver":
				//WARNING: maybe it is not necessary, but dbo schema is needed by some sql server databases to access to tables
				if (DBMS.equalsIgnoreCase("sqlserver")) {
					originalDB += ".dbo";
					DB1 += ".dbo";
					DB2 += ".dbo";
				}
			case "mysql":
                try {
                    //CREATE DB1, DB2 and clones the tables
                    Statement st;
                    st = DBConnection.getConn().createStatement();
					st.executeUpdate("DROP DATABASE IF EXISTS " + DB1);
					st.executeUpdate("DROP DATABASE IF EXISTS " + DB2);
					st.executeUpdate("CREATE DATABASE " + DB1);
					st.executeUpdate("CREATE DATABASE " + DB2);

                    for (String t : graph.sortTopological()) {
                        cloneTable(t, t, DB1, false); //creates a full copy of the tables
						cloneTable(t, t, DB2, true); //only copies the table structure
                    }
					for (String t : graph.listProblematicTables()) {
						copyTableContent(t, t, DB2);
						addReferences(t, " from " + originalDB + "." + t, null, new ArrayList<>(), true, originalDB);
					}

					//adds fks
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
                } catch (Exception se) {
					System.out.println("Error in cloning: " + se.getMessage());
					System.out.println("\t"+query);
                    return false;
                } finally {
					query = "";
				}
            default:
                return false;
        }
    }

	/** Evokes split function on each table.
	 */
	public static void prepareSplit() {
		try {
			//gets all tables in topological order, explores all from sources to wells
            for (String table : graph.sortTopological()) {
				startingTime = System.currentTimeMillis();
				if (!graph.hasProblematicArcs(table))
					split(table);

				numTables++;
				time = time + getTime();

				if (reporting) MainDebug.report.get(table).setAlgorithm_runningTime(getNowTime());
            }

			//if report is active, gets information about the tables
			if (reporting) {
				if (inverting) {
					String t = DB1;
					DB1 = DB2;
					DB2 = t;
				}
				Graph graph1 = null;
				Graph graph2 = null;
				switch (DBMS.toLowerCase()) {
					case "sqlite" -> {
						DBConnection.closeConn();
						DBConnection.setConn(DBMS, sv, username, password, DB1);
						graph1 = new Graph(DBMS, DBConnection.getConn(), DB1);
						DBConnection.closeConn();
						DBConnection.setConn(DBMS, sv, username, password, DB2);
						graph2 = new Graph(DBMS, DBConnection.getConn(), DB2);
						DBConnection.closeConn();
					}
					case "mysql" -> {
						graph1 = new Graph(DBMS, DBConnection.getConn(), DB1);
						graph2 = new Graph(DBMS, DBConnection.getConn(), DB2);
					}
				}
				for (String t : graph.listTables()) {
					assert graph1 != null;
					MainDebug.report.get(t).setAlgorithm(graph1.getRecordNumberInTable(t),
							graph2.getRecordNumberInTable(t));
					String query = "";
					try {
						switch (DBMS.toLowerCase()) {
							case  "mysql" -> {
								query = formOverlappingQuery(t);
								st = DBConnection.getConn().createStatement();
								rs = st.executeQuery(query);

								while (rs.next())
									MainDebug.report.get(t).setAlgorithm_overlappingRecords(rs.getInt(1));
							}
							case "sqlite" -> {
								try {
									int count = 0;

									ArrayList<Integer> rowidsList = new ArrayList<>();
									query = "SELECT rowid FROM " + t;

									DBConnection.closeConn();
									DBConnection.setConn(DBSplit_from_higher.DBMS, DBSplit_from_higher.sv, DBSplit_from_higher.username, DBSplit_from_higher.password, DBSplit_from_higher.DB1);
									st = DBConnection.getConn().createStatement();
									rs = st.executeQuery(query);

									while (rs.next()) {
										rowidsList.add(rs.getInt(1));
									}

									DBConnection.closeConn();
									DBConnection.setConn(DBSplit_from_higher.DBMS, DBSplit_from_higher.sv, DBSplit_from_higher.username, DBSplit_from_higher.password, DBSplit_from_higher.DB2);
									st = DBConnection.getConn().createStatement();
									rs = st.executeQuery(query);

									//looks for common fk
									while (rs.next()) {
										if (rowidsList.contains(rs.getInt(1)))
											count++;
									}

									MainDebug.report.get(t).setAlgorithm_overlappingRecords(count);
								} catch (Exception se) {
									System.out.println("Overlapping computation for table " +t+ " failed : " + se.getMessage());
									System.out.println("\t" + query);
								} finally {
									query = "";
									DBConnection.closeRs(rs);
									DBConnection.closeSt(st);
								}
							}
						}
					} catch (SQLException se) {
						System.out.println("Report generation failed: \n\t" + se.getMessage());
						System.out.println("\t"+ query);
					} finally {
						DBConnection.closeRs(rs);
						DBConnection.closeSt(st);
					}
				}
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
	public static void split(String table) throws UnexpectedException {
		long startTime = System.currentTimeMillis();

        //Computes parameters
        int totalRecords = graph.getRecordNumberInTable(table);
        computeParameters(percent, totalRecords); //computes n, c, u
		computeOverlapping(percentOverlapping, table);

		switch (DBMS.toLowerCase()) {
			case "sqlite" -> {
				if (table.equalsIgnoreCase("android_metadata") ||
						table.equalsIgnoreCase("sqlite_sequence") ||
						table.equalsIgnoreCase("cache"))
					return;//default tables are not split

				switch (graph.getTableType(table)) {
					case external_node -> {
						try {
							//EXECUTION FOR EXTERNAL NODE:
							//	Deletes u+c records from DB1 and u+m from DB2
							//	Records are ordered as ucnm
							List<Integer> rowids = getAllRowID_SQLite(DB1, table);

							DBConnection.closeConn();
							DBConnection.setConn(DBMS, sv, username, password, DB1);
							st = DBConnection.getConn().createStatement();
							query = "DELETE FROM " + table +
									" WHERE rowid IN (" + printStr(rowids, c, c+n+u)+ ")";
							st.execute(query);

							DBConnection.closeConn();
							DBConnection.setConn(DBMS, sv, username, password, DB2);
							st = DBConnection.getConn().createStatement();
							query = "DELETE FROM " + table +
									" WHERE rowid IN (" + printStr(rowids, c+n, rowids.size()) + ")";
							st.execute(query);
						} catch (Exception e) {
							System.out.println("Split for external node " +table+ " failed : " + e.getMessage());
							System.out.println("\t" + query);
							++err;
						} finally {
							query = "";
							DBConnection.closeSt(st);
							DBConnection.closeRs(rs);
							time = System.currentTimeMillis() - startTime;
						}
					}
					case well -> {
						try {
							int lengthDB1 = 0;
							ArrayList<Integer> rowidsDB1 = new ArrayList<>();
							int lengthDB2 = 0;
							ArrayList<Integer> rowidsDB2 = new ArrayList<>();
							int lengthCommon = 0;
							ArrayList<Integer> rowidsCommon = new ArrayList<>();

							// Select non-referenced fks and how many times they appear
							query = "SELECT rowid FROM " + table +
									referencesQuery(table, true);

							DBConnection.closeConn();
							DBConnection.setConn(DBSplit_from_higher.DBMS, DBSplit_from_higher.sv, DBSplit_from_higher.username, DBSplit_from_higher.password, DBSplit_from_higher.DB1);
							st = DBConnection.getConn().createStatement();
							rs = st.executeQuery(query);

							// Gets all non-referenced fks in DB1
							while (rs.next()){
								rowidsDB1.add(rs.getInt(1));
								lengthDB1++;
							}

							DBConnection.closeConn();
							DBConnection.setConn(DBSplit_from_higher.DBMS, DBSplit_from_higher.sv, DBSplit_from_higher.username, DBSplit_from_higher.password, DBSplit_from_higher.DB2);
							st = DBConnection.getConn().createStatement();
							rs = st.executeQuery(query);

							// Gets all non-referenced rowids in DB2 and checks the common ones
							while (rs.next()){
								if (rowidsDB1.contains(rs.getInt(1))) {
									rowidsCommon.add(rs.getInt(1));
									lengthCommon++;
								}
								rowidsDB2.add(rs.getInt(1));
								lengthDB2++;
							}

							int i = 0;
							for (; i < c && i < rowidsCommon.size(); ++i) {
								rowidsDB1.remove(rowidsCommon.get(i));
								--lengthDB1;

								rowidsDB2.remove(rowidsCommon.get(i));
								--lengthDB2;
							}

							String lostRowids = "";
							for (; i < c+u && i < rowidsCommon.size(); ++i) {
								lostRowids += rowidsCommon.get(i) + ", ";

								rowidsDB1.remove(rowidsCommon.get(i));
								--lengthDB1;

								rowidsDB2.remove(rowidsCommon.get(i));
								--lengthDB2;
							}

							String deleteNfromDB1 = "";
							for (i = 0; i < n && i < rowidsDB1.size(); ++i) {
								deleteNfromDB1 += rowidsDB1.get(i) + ", ";
								if (rowidsDB2.contains(rowidsDB1.get(i))){
									rowidsDB2.remove(rowidsDB1.get(i));
									--lengthDB2;
								}
							}

							String deleteMfromDB2 = "";
							for (i = 0; i < m  && i < rowidsDB2.size(); ++i) {
								deleteMfromDB2 += rowidsDB2.get(i) + ", ";
							}

							DBConnection.closeConn();
							DBConnection.setConn(DBMS, sv, username, password, DB2);
							st = DBConnection.getConn().createStatement();

							//tolgo u+m da db2
							query = "DELETE FROM " + table +
									" WHERE rowid IN ( SELECT rowid FROM " + table +
									" WHERE rowid IN (";
							if (lostRowids.length() > 2) {
								query += lostRowids.substring(0, lostRowids.length() - 2);
							}
							if (lostRowids.length() > 2 && deleteMfromDB2.length() > 2)
								query += ", ";
							if (deleteMfromDB2.length() > 2) {
								query += deleteMfromDB2.substring(0, deleteMfromDB2.length()-2);
							}
							query += ") LIMIT " + (u+m) + ")";
							st.execute(query);

							DBConnection.closeConn();
							DBConnection.setConn(DBMS, sv, username, password, DB1);
							st = DBConnection.getConn().createStatement();

							//tolgo u+n da db1
							query = "DELETE FROM " + table +
									" WHERE rowid IN ( SELECT rowid FROM " + table +
									" WHERE rowid IN (";
							if (lostRowids.length() > 2) {
								query += lostRowids.substring(0, lostRowids.length() - 2);
							}
							if (lostRowids.length() > 2 && deleteNfromDB1.length() > 2)
								query += ", ";
							if (deleteNfromDB1.length() > 2) {
								query += deleteNfromDB1.substring(0, deleteNfromDB1.length()-2);
							}
							query += ") LIMIT " + (u+n) + ")";
							st.execute(query);
						}  catch (Exception e) {
							System.out.println("Split for well node " +table+ " failed : " + e.getMessage());
							System.out.println("\t" + query);
							++err;
						} finally {
							query = "";
							DBConnection.closeSt(st);
							DBConnection.closeRs(rs);
							time = System.currentTimeMillis() - startTime;
						}
					}
					case mid_node -> {
						try {
							KS_Return chosenFks = GetFromHigher.SQLiteMid(table, graph.getForeignKeysInTable(table), n, m, u, c);
							DBConnection.closeConn();
							DBConnection.setConn(DBMS, sv, username, password, DB2);
							st = DBConnection.getConn().createStatement();

							//tolgo u da db2
							query = "DELETE FROM " + table +
									" WHERE rowid IN (" +
									"SELECT rowid FROM " + table +
									" WHERE rowid IN (" +
									chosenFks.commons + ")" +
									" LIMIT " + u + ")";
							st.execute(query);

							//tolgo m da db2
							query = "DELETE FROM " + table +
									" WHERE rowid IN (" +
									"SELECT rowid FROM " + table +
									" WHERE rowid IN (" +
									chosenFks.deleteDB2 + ")" +
									" LIMIT " + m + ")";

							st.execute(query);

							DBConnection.closeConn();
							DBConnection.setConn(DBMS, sv, username, password, DB1);
							st = DBConnection.getConn().createStatement();
							//tolgo u da db1
							query = "DELETE FROM " + table +
									" WHERE rowid IN (" +
									"SELECT rowid FROM " + table +
									" WHERE rowid IN (" +
									chosenFks.commons + ")" +
									" LIMIT " + u + ")";
							st.execute(query);
							//tolgo n da db1
							query = "DELETE FROM " + table +
									" WHERE rowid IN (" +
									"SELECT rowid FROM " + table +
									" WHERE rowid IN (" +
									chosenFks.deleteDB1 + ")" +
									" LIMIT " + n + ")";
							st.execute(query);
						} catch (Exception e) {
							System.out.println("Split for mid node " +table+ " failed : " + e.getMessage());
							System.out.println("\t" + query);
							++err;
						} finally {
							query = "";
							DBConnection.closeSt(st);
							DBConnection.closeRs(rs);
							time = System.currentTimeMillis() - startTime;
						}
					}
					case source -> {
						try {
							//EXECUTION FOR SOURCE NODES
							//	Knapsack searches for a FK valid enough (tries to reach u+c+n value, tries to avoid single count FKs
							//		For mids, avoids Fks present in records that are referred by upper tables
							DBConnection.closeConn();
							DBConnection.setConn(DBMS, sv, username, password, DB1);
							KS_Return chosenFks = GetFromHigher.MySqlAndSources(table, graph.getForeignKeysInTable(table),
									u, c, n);

							ForeignKeyColumn fk = chosenFks.getFk();
							//	Changes the total of found records, I don't care how many I have actually found,
							//		I don't need all of them
							chosenFks.setSum(Math.min(chosenFks.getSum(), u+n+c));

							DBConnection.closeConn();
							DBConnection.setConn(DBMS, sv, username, password, DB1);
							st = DBConnection.getConn().createStatement();
							//tolgo u, n da db1
							query = "DELETE FROM " + table +
									" WHERE rowid IN (" +
									"SELECT rowid FROM " + table +
									" WHERE " +  fk.getName() + " IN (" +
									printStr(chosenFks.getResult(), 0, chosenFks.getResult().size()) + ")" +
									" LIMIT " + (u+n) + " OFFSET " + c + ")";
							st.execute(query);

							DBConnection.closeConn();
							DBConnection.setConn(DBMS, sv, username, password, DB2);
							st = DBConnection.getConn().createStatement();
							//tolgo u, m da db2
							query = "DELETE FROM " + table +
									" WHERE rowid IN (" +
									"SELECT rowid FROM " + table +
									" WHERE " +  fk.getName() + " IN (" +
									printStr(chosenFks.getResult(), 0, chosenFks.getResult().size()) + ")" +
									" LIMIT " + u + " OFFSET " + (n+c) + ")";
							st.execute(query);
							query = "DELETE FROM " + table +
									" WHERE rowid IN (" +
									"SELECT rowid FROM " + table +
									" WHERE " +  fk.getName() + " NOT IN (" +
									printStr(chosenFks.getResult(), 0, chosenFks.getResult().size()) + ")" +
									" LIMIT " + m + ")";
							st.execute(query);
						} catch (Exception e) {
							System.out.println("Split for source node " +table+ " failed : " + e.getMessage());
							System.out.println("\t" + query);
							++err;
						} finally {
							query = "";
							DBConnection.closeSt(st);
							DBConnection.closeRs(rs);
							time = System.currentTimeMillis() - startTime;
						}
					}
				}
			}
			case "mysql", "sqlserver" -> {
				switch (graph.getTableType(table)) {
					case external_node -> {
						try {
							//EXECUTION FOR EXTERNAL NODE:
							// c is equal to u
							// Selects n+c records after u records from DB1 and
							//		copies them into DB2
							// Deletes c+n records from DB1, leaving the last c+m records
							// First u records are lost, n records are in DB2,
							//		c records are in common, m records are in DB1
							st = DBConnection.getConn().createStatement();

                            query = "INSERT IGNORE INTO " + DB2 + "." + table +
									" SELECT *" +
									" FROM " + DB1 + "." + table +
									" ORDER BY RAND(" + startingTime + ")" +
									" LIMIT " + clamp(n+c) + " OFFSET " + c;
                            st.execute(query);

							query = "DELETE FROM " + DB1 + "." + table +
									" ORDER BY RAND(" + startingTime + ")" +
									" LIMIT " + clamp(c+n);
							st.execute(query);
						} catch (Exception e) {
							System.out.println("Split for external node " +table+ " failed : " + e.getMessage());
							//System.out.println("\t" + query);
							++err;
						} finally {
							query = "";
							DBConnection.closeSt(st);
							DBConnection.closeRs(rs);
							time = System.currentTimeMillis() - startTime;
						}
					}

					case well -> {
						//EXECUTION FOR WELL
						// Selects at most u+c+n primary keys not referred by any record in DB1, and can be removed,
						//		and memorizes them in a matrix
						// Creates the where condition as "pk1 IN (v1, v2, ...) AND pk2 IN (v1, v2, ...)"
						// Execute the insertion of n+c after u (or i-n-c) records in DB2,
						//		taken from DB1 according to where condition
						// Deletes from DB1 n+c records according to where condition

						query = "SELECT " + getPrimaryKeys(table, false) +
								" FROM " + DB1 + "." + table + " WHERE ";

						Map<String, List<ForeignKeyColumn>> map = graph.getForeignKeysReferringTo(table);
						for (String s : map.keySet()) {
							for (ForeignKeyColumn fk : map.get(s)) {
								query += fk.getReferredPrimaryKey() + " NOT IN " +
										"(SELECT DISTINCT " + fk.getName() +
										" FROM " + DB1 + "." + s + ") and ";
							}
						}
						query = query.substring(0, query.length() - 5) + " LIMIT " + (u+n+c);

						//	Memorizes all PKS (considering compound PKs, hence the matrix)
						String[][] s = new String[graph.getPrimaryKeyNumberInTable(table)][u+n+c];
						try {
							st = DBConnection.getConn().createStatement();
							rs = st.executeQuery(query);

							int i = 0;//number of records returned from query
							while (rs.next()) {
								if (i>=u+n+c) break;
								for (int j = 0; j < graph.getPrimaryKeyNumberInTable(table); ++j) {
									s[j][i] = rs.getString(j + 1);
								}
								++i;
							}

							//	if at least 1 value, creates the WHERE condition:
							//		Pk1 in (values) and pk2 in (values)...
							if (i != 0) {
								query = " WHERE ";
								for (int k = 0; k < graph.getPrimaryKeyNumberInTable(table); ++k) {
									String around = ((graph.getPrimaryKeyAtIndexInTable(table, k).getDatatype() == 4) ?
											"" : "'");
									query += graph.getPrimaryKeyAtIndexInTable(table, k).getName() +
											" IN (" + printStr(s[k], around, i) + ") and ";
								}
								query = query.substring(0, query.length() - 5);

							//	Inserts in DB2 c+n records from DB1, ignores i-n-c records 	(on optimal condition,
							//		equals u records that will be deleted from DB1 to reach overlapping stability)
								st.execute("INSERT IGNORE INTO " + DB2 + "." + table +
										" SELECT *" +
										" FROM " + DB1 + "." + table +
											query +
										" LIMIT " + clamp(c+n) + " OFFSET " + clamp(i-n-c));
							//	Deletes u+n records from DB1; c records will be common
								st.execute("DELETE FROM " + DB1 + "." + table + query +
										" LIMIT " + clamp(i-c));
							}
						} catch (SQLException se) {
							System.out.println("Split for well node " +table+ " failed : " + se.getMessage());
							//System.out.println("\t" + query);
							++err;
						} finally {
							query = "";
							DBConnection.closeRs(rs);
							DBConnection.closeSt(st);
							time = System.currentTimeMillis() - startTime;
						}
					}

					case mid_node, source -> {
						try {
							//EXECUTION FOR MID NODES (mids) AND SOURCE NODES
							// Foreign keys are selected (from non-referenced records, if the table is a mid-node)
							//		to gather at least u+c+n records
							// If the result set is empty, nothing can be done; else updates the values it needs
							//		for the execution
							// Adds to DB2 all the references needed by the records that will be added, then the
							//		records themselves, in n+c records after u (or sum-c-n)
							// 		Records are ordered by fk, to add the least number of references
							//		TODO Order by key numerosity
							// n+c records are deleted from DB1

							KS_Return chosenFks = GetFromHigher.MySqlAndSources(
									table, graph.getForeignKeysInTable(table), u, c, n);

							if (!chosenFks.hasValues() || chosenFks.getResult().isEmpty()) return;

							ForeignKeyColumn fk = chosenFks.getFk();
							chosenFks.setSum(Math.min(chosenFks.getSum(), u+n+c));

							// add references
							query = " FROM (" +
										"SELECT * " +
										" FROM " + DB1 + "." + table +
										" WHERE " + fk.getName() +
										" IN (" + printStr(chosenFks.getResult(), 0,
															chosenFks.getResult().size()) + ")" +
										" ORDER BY " + fk.getName() +
										" LIMIT " + clamp(n+c) + " OFFSET " + clamp(chosenFks.sum-c-n) +
									") as tempTable";
							addReferences(table, query, null, new ArrayList<>(), true, DB1);

                            st = DBConnection.getConn().createStatement();

							// Insert into DB2
							query = "INSERT IGNORE INTO " + DB2 + "." + table +
									" SELECT * " +
									" FROM " + DB1 + "." + table +
									" WHERE " + fk.getName() + " IN (" +
													printStr(chosenFks.getResult(), 0,
															chosenFks.getResult().size()) + ")" +
									" ORDER BY " + fk.getName() +
									" LIMIT " + clamp(n+c) + " OFFSET " + clamp(chosenFks.sum-c-n);
							st.execute(query);

							//	Deletes from DB1
							query = "DELETE FROM " + DB1 + "." + table +
									" WHERE " + fk.getName() +
									" IN (" + printStr(chosenFks.getResult(), 0,
											chosenFks.getResult().size())  + ") " +
									" ORDER BY " + fk.getName() +
									" LIMIT " + clamp(chosenFks.getSum()-c);
							st.execute(query);
						} catch (Exception e) {
							System.out.println("Split for mid/source node " +table+ " failed : " + e.getMessage());
							System.out.println("\t" + query);
							++err;
						} finally {
							query = "";
							DBConnection.closeSt(st);
							DBConnection.closeRs(rs);
							time = System.currentTimeMillis() - startTime;
						}
					}
					case unknown -> throw new UnexpectedException("What");
				}
			}
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


	private static void computeOverlapping(int percentOverlapping, String table) {
		c = (int) Math.round((double) (Math.min(m,n) * percentOverlapping) / 100);
		u = c;
		n -= c;
		m -= c;
		boolean requiresCheck = (graph.getTableType(table) == Graph.nodeType.mid_node ||
				graph.getTableType(table) == Graph.nodeType.well);

		if (DBMS.equalsIgnoreCase("mysql") && requiresCheck) {
			query = "DELETE FROM " + DB1 + "." + table +
					" WHERE " + getPrimaryKeys(table, true) + " IN " +
					" (SELECT DISTINCT " + getPrimaryKeys(table, true) +
					" FROM " + DB2 + "." + table +
					" WHERE ";

			Map<String, List<ForeignKeyColumn>> map = graph.getForeignKeysReferringTo(table);
			for (String referringTable : map.keySet()) {
				for (ForeignKeyColumn foreignKey : map.get(referringTable)) {
					query += foreignKey.getReferredPrimaryKey() + " NOT IN " +
							"(SELECT DISTINCT " + foreignKey.getName() +
							" FROM " + DB1 + "." + referringTable + ") and ";
				}
			}
			query = query.substring(0, query.length() - 5) + ") LIMIT " + n;
			try {
				st = DBConnection.getConn().createStatement();
				st.execute(query);

				n -= st.getUpdateCount();
			} catch (SQLException e) {
				System.out.println("Failed preemptive delete for node "+ table + ": " + e.getMessage());
			}
		}

		//	If the table is a well or a mid-node, records have already been added (as references to upper tables)
		//	Considers existing records as already added common records, and removes them from c
		if (c > 0 && requiresCheck) {
			switch (DBMS.toLowerCase()) {
				case "mysql" -> {
					try {
						query = formOverlappingQuery(table);
						st = DBConnection.getConn().createStatement();
						rs = st.executeQuery(query);

						while (rs.next()) {
							c = Math.max(c - rs.getInt(1), 0);
							if (c == 0)
								break;
						}

					} catch (SQLException se) {
						System.out.println("Overlapping computation for table " +table+ " failed : " + se.getMessage());
						System.out.println("\t" + query);
					} finally {
						query = "";
						DBConnection.closeRs(rs);
						DBConnection.closeSt(st);
					}
				}
				case "sqlite" -> {
					try {
						ArrayList<Integer> rowidsList = new ArrayList<>();
						query = "SELECT rowid FROM " + table + referencesQuery(table, false);

						DBConnection.closeConn();
						DBConnection.setConn(DBSplit_from_higher.DBMS, DBSplit_from_higher.sv, DBSplit_from_higher.username, DBSplit_from_higher.password, DBSplit_from_higher.DB1);
						st = DBConnection.getConn().createStatement();
						rs = st.executeQuery(query);

						while (rs.next()) {
							rowidsList.add(rs.getInt(1));
						}

						DBConnection.closeConn();
						DBConnection.setConn(DBSplit_from_higher.DBMS, DBSplit_from_higher.sv, DBSplit_from_higher.username, DBSplit_from_higher.password, DBSplit_from_higher.DB2);
						st = DBConnection.getConn().createStatement();
						rs = st.executeQuery(query);

						//looks for common fk
						while (rs.next()) {
							if (rowidsList.contains(rs.getInt(1)))
								--c;
							if (c == 0) break;
						}
					} catch (Exception se) {
						System.out.println("Overlapping computation for table " +table+ " failed : " + se.getMessage());
						System.out.println("\t" + query);
					} finally {
						query = "";
						DBConnection.closeRs(rs);
						DBConnection.closeSt(st);
					}
				}
			}
		}
	}

	private static String referencesQuery(String table, boolean usingNot) {
		//select rowid
		// from types
		// where rowid not in
		// 	(select distinct rowid from types where
		//		name in (select distinct type1 from pokemon) and
		// rowid not in
		// (select distinct rowid from types where
		//		name in (select distinct this_type from isnt_effective)
		String query = "";
		Map<String, List<ForeignKeyColumn>> map = DBSplit_from_higher.graph.getForeignKeysReferringTo(table);
		if (map.size() > 0) {
			query += " WHERE ";
			for (String s : map.keySet()) {
				for (ForeignKeyColumn foreignKey : map.get(s)) {
					query += "rowid " + ((usingNot)?" NOT":"") + " IN (SELECT DISTINCT rowid FROM ";
					query+= table + " WHERE ";
					query += foreignKey.getReferredPrimaryKey() + " IN (SELECT DISTINCT " + foreignKey.getName() +
							" FROM " + s + ")) " + ((usingNot)?"and ":"or ");
				}
			}
			query = query.substring(0, query.length() - ((usingNot)?5:4));
		}
		return query;
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

	/**
	 * Recursive function, explores from top of each tree to bottom, goes back up adding
	 * all references needed to add records in DB2; uses a list of visited tables to avoid
	 * infinite cycles
	 *
	 * @param table       table being explored in this call
	 * @param bottomQuery query being constructed for this tree
	 * @param withQuery
	 * @param copyFromDB  Database from which to copy
	 */
	private static void addReferences(String table, String bottomQuery, String withQuery, List<String> addedRefs,
									  boolean first, String copyFromDB) {
		//TODO if table is full do nothing
        addedRefs.add(table);

        if (graph.isTableReferring(table)) {
			//for all FKs not already visited on this path adds references
			for (ForeignKeyColumn s: graph.getForeignKeysInTable(table)) {
                if (!addedRefs.contains(s.getReferredTable()) &&
						!graph.hasProblematicArcs(s.getReferredTable())) {
                    String query = " FROM " + copyFromDB + "." + s.getReferredTable() +
                            " WHERE " + s.getReferredPrimaryKey() + " IN" +
                            " (SELECT DISTINCT " + s.getName() + bottomQuery + ")";
                    addReferences(s.getReferredTable(), query, withQuery, addedRefs, false, copyFromDB);
                }
			}
		}
		addedRefs.remove(table);
		if (!first){
			try {
				st = DBConnection.getConn().createStatement();
				bottomQuery = " INSERT IGNORE INTO " + DB2 + "." + table + " " +
						((withQuery == null) ? "" : withQuery) +
						" SELECT * " + bottomQuery;

				//System.out.println("\t"+q);
				st.execute(bottomQuery);
			} catch (SQLException se) {
				System.out.println("Reference insertion for table " +table+ " failed : " + se.getMessage());
				System.out.println("\t" + bottomQuery);
			} finally {
				query  = "";
				DBConnection.closeSt(st);
			}
		}
	}

	/**
	 * Creates a clone of the given table in the specified database
	 * with the same records
	 * @param table    Table to clone
	 * @param newTable Cloned table
	 * @param DB       DB where table is going to be
	 */
	private static void cloneTable(String table, String newTable, String DB, boolean justClone){
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

                    if (!justClone) {
						copyTableContent(table, newTable, DB);
                    }
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

		} catch (SQLException se) {
			System.out.println("Cloning for table " +table+ " failed : " + se.getMessage());
			System.out.println("\t" + query);
			se.printStackTrace();
			err++;
			DBConnection.closeSt(st);
		} catch (Exception e) {
			query = "";
			e.printStackTrace();
		}
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
	private static void copyTableContent (String table, String newTable, String DB) throws Exception {
		try {
			if (!DB.equals(originalDB) &&
					!DB.equals(DB1) &&
					!DB.equals(DB2))
				throw new SQLException("Database in input is wrong");

			if ("mysql".equalsIgnoreCase(DBMS)) {
				st = DBConnection.getConn().createStatement();
				String insertQuery = "INSERT INTO " + DB + "." + newTable + "(";
				String fields = "";
                for (Column column : graph.getColumnsInTable(table)) {
                    fields += column.getName() + ",";
                }
				fields = fields.substring(0, fields.length()-1);
                insertQuery += fields + ") SELECT " + fields + " FROM "
						+ originalDB + "." + table;
				st.execute(insertQuery);

				DBConnection.closeSt(st);
			} else {
				throw new IllegalArgumentException("DBMS not supported");
			}

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
	}

	protected static String printStr(List list, int offset, int limit) {
		//	Builds a string from a list of strings, without encompassing character, starting from an offset index
		StringBuilder stringBuilder = new StringBuilder();
		int i = offset;
		limit = clamp(Math.min(list.size(), limit));

		while (i < limit) {
			stringBuilder.append(list.get(i)).append(", ");
			++i;
		}

		if (stringBuilder.length()>2)
			stringBuilder = new StringBuilder(stringBuilder.substring(0, stringBuilder.length() - 2));//remove last ','

		return stringBuilder.toString();
	}


	private static String printStr(String[] s, String around, int limit) {
		//	Builds a string from a list of strings, encompassing each string with an 'around' character,
		//		up to a limit
		StringBuilder string = new StringBuilder();
		limit = Math.min(limit, s.length);

		for (int i =0; i<limit; ++i) {
			String value = s[i];
			if (value != null)
				string.append(around).append(value).append(around).append(", ");
		}
		string = new StringBuilder(string.substring(0, string.length() - 2));//remove last ','

		return string.toString();
	}

	private static String formOverlappingQuery (String table) {
		String overlapQuery;
		if (graph.hasPrimaryKeyInTable(table)){
			String pkString = getPrimaryKeys(table, false);
			overlapQuery = "SELECT count(*) FROM " + DB1 + "." + table + " WHERE ";
			if (graph.isPrimaryKeyComposedInTable(table))
				overlapQuery += "(" + pkString + ")";
			else overlapQuery += pkString;

			overlapQuery += " IN (SELECT " + pkString + " FROM " + DB2 + "." + table + ")";
		} else {
			overlapQuery = "SELECT COUNT(*) FROM (SELECT "+table+".* FROM " + DB1 +"."+ table +
					", (SELECT * FROM "+ DB2 +"."+table+") as tabel WHERE ";
			for (Column column: graph.getColumnsInTable(table)) {
				if (column.isNullable())
					overlapQuery += "( " + table +"."+ column.getName() + " is NULL or " +
							table +"."+ column.getName() +" = tabel."+ column.getName() +")";
				else
					overlapQuery += table +"."+ column.getName() +" = tabel."+ column.getName();
				overlapQuery += " and ";
			}
			overlapQuery = overlapQuery.substring(0, overlapQuery.length()-5) + ") tt";
		}
		return overlapQuery;
	}

	private static String getPrimaryKeys (String table, boolean parenthesis) {
		if (graph.hasPrimaryKeyInTable(table)) {
			if (parenthesis)
				return graph.getPrimaryKeysStringInTable(table, ',', "(", ")");
			else return graph.getPrimaryKeysStringInTable(table, ',', "", "");
		} else return "thereShouldHaveBeenAPK";
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

	private static int clamp(int n) {
		return Math.max(n, 0);
	}

	public static int getErrors() {
		return err;
	}
}

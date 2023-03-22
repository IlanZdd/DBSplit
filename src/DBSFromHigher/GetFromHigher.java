package src.DBSFromHigher;

import Graph.ForeignKeyColumn;
import src.Other.DBConnection;
import src.Other.MainDebug;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

import static src.DBSFromHigher.DBSplit_from_higher.graph;

public class GetFromHigher {

    protected static KS_Return MySqlAndSources (String table, List<ForeignKeyColumn> allFKs, int u, int c, int n){
        long timer = System.currentTimeMillis();
        ForeignKeyColumn fk;
        int fkIndex = 0;
        KS_Return returnValue = new KS_Return();
        int length = 0;
        boolean allOne = true;
        int sum = 0;
        try {
            while (fkIndex < allFKs.size() && (allOne || length == 0 || sum < (u+c+n))) {
                fk = allFKs.get(fkIndex++);

                //  Counts all the mentions of a specific FK in the table
                String query = "SELECT count(*) as count, " + fk.getName() +
                        " FROM ";
                if (DBSplit_from_higher.DBMS.equalsIgnoreCase("mysql"))
                    query += DBSplit_from_higher.DB1 + ".";
                query += table;

                //      if the table is a mid-node, FKs of referred records will not be considered
                //      not to ruin referential integrity
                query += referencesQuery(table, fk);

                Statement st = DBConnection.getConn().createStatement();
                ResultSet rs = st.executeQuery(query);

                //initialize value
                length = 0;
                allOne = true;
                sum = 0;

                //      Since the references are already ordered, only adds the ones it needs
                List<String> fks = new ArrayList<>();

                String around = (fk.getDatatype()==12) ? "'" : "";

                while (rs.next()) {
                    fks.add(around + rs.getString(2) + around);

                    if (rs.getInt(1) != 1) allOne = false;
                    sum += rs.getInt(1);
                    ++length;

                    if (sum >= u+c+n) break;
                }
                if ((returnValue.hasValues() && returnValue.getSum()> sum) || length == 0)
                    continue;
                int indexForRefs = 0;
                returnValue.set(fks, fk);
                returnValue.setAddReferencesOnward(indexForRefs);
                returnValue.setSum(sum);

                if (DBSplit_from_higher.reporting)
                    MainDebug.report.get(table).setAlgorithm_knapsackTime((double)(System.currentTimeMillis()-timer)/1000);
                if (!(allOne || sum < (u + c + n)))
                    return returnValue;
            }
        } catch (Exception se) {
            System.out.println("Error in Knapsack for " + table + " on FK " +
                    allFKs.get(clamp(fkIndex-1)).getName() + ": " + se.getMessage());
            se.printStackTrace();
        }
        if (DBSplit_from_higher.reporting)
            MainDebug.report.get(table).setAlgorithm_knapsackTime((double)(System.currentTimeMillis()-timer)/1000);
        return returnValue;
    }

    private static String referencesQuery(String table, ForeignKeyColumn fk) {
        String query = "";
        Map<String, List<ForeignKeyColumn>> map = graph.getForeignKeysReferringTo(table);
        if (map.size() > 0) {
            query += " WHERE ";
            for (String s : map.keySet()) {
                for (ForeignKeyColumn foreignKey : map.get(s)) {
                    query += fk.getName() + (" NOT") + " IN (SELECT DISTINCT " + fk.getName() +
                            " FROM ";
                    if (DBSplit_from_higher.DBMS.equalsIgnoreCase("mysql"))
                        query+= DBSplit_from_higher.DB1 + ".";
                    query+= table + " WHERE ";
                    query += foreignKey.getReferredPrimaryKey() + " IN (SELECT DISTINCT " + foreignKey.getName() +
                            " FROM ";
                    if (DBSplit_from_higher.DBMS.equalsIgnoreCase("mysql"))
                        query+= DBSplit_from_higher.DB1 + ".";
                    query+= s + ")";
                    if (fk.isNullable())
                        query += " and " + fk.getName() + " IS NOT NULL";
                    query += ") " + ("and ");
                }
            }
            query = query.substring(0, query.length() - (5));
        }
        return query += " GROUP BY " + fk.getName() + " ORDER BY count DESC";
    }

    private static int clamp(int i) {
        return Math.max(i, 0);
    }

    public static KS_Return SQLiteMid(String table, List<ForeignKeyColumn> allFKs, int n, int m, int u, int c) {
        long knapsackTimer = System.currentTimeMillis();
        int fkIndex = 0;
        ForeignKeyColumn fk = null;
        KS_Return returnValue = new KS_Return();
        Statement st = null;
        ResultSet rs = null;
        String query = null;

        try {
            // Gets the unreferenced records from DB1 and DB2, grouped by foreign key, and searches for common ones;
            fk = allFKs.get(fkIndex);
            ++fkIndex;

            Map<String, BeanForSQLite> db1 = new HashMap<>();
            Map<String, BeanForSQLite> db2 = new HashMap<>();
            Map<String, BeanForSQLite> commons = new HashMap<>();

            // Writes the gathering query
            // The query is the same for DB1 and DB2
            query = "SELECT " + fk.getName() + ", rowid FROM " + table + " WHERE ";

            Map<String, List<ForeignKeyColumn>> map = graph.getForeignKeysReferringTo(table);
            if (map.size() > 0) {
                for (String s : map.keySet()) {
                    for (ForeignKeyColumn foreignKey : map.get(s)) {
                        query += foreignKey.getReferredPrimaryKey() + " NOT IN (SELECT DISTINCT " + foreignKey.getName() +
                                " FROM " + s;
                        if (fk.isNullable())
                            query += " and " + fk.getName() + " IS NOT NULL";
                        query += ") AND ";
                    }
                }
            }
            query = query.substring(0, query.length() - 5) + " GROUP BY " + fk.getName() + ", rowid";

            DBConnection.closeConn();
            DBConnection.setConn(DBSplit_from_higher.DBMS, DBSplit_from_higher.sv, DBSplit_from_higher.username, DBSplit_from_higher.password, DBSplit_from_higher.DB1);
            st = DBConnection.getConn().createStatement();
            rs = st.executeQuery(query);

            // Gets all non-referenced foreign keys in DB1
            while (rs.next()){
                if (!db1.containsKey(rs.getString(1)))
                    db1.put(rs.getString(1), new BeanForSQLite(rs.getString(1)));
                db1.get(rs.getString(1)).addID(rs.getInt(2));
            }

            DBConnection.closeConn();
            DBConnection.setConn(DBSplit_from_higher.DBMS, DBSplit_from_higher.sv, DBSplit_from_higher.username, DBSplit_from_higher.password, DBSplit_from_higher.DB2);
            st = DBConnection.getConn().createStatement();
            rs = st.executeQuery(query);

            // Gets all non-referenced fks in DB2 and puts the common ones in the map
            while (rs.next()){
                if (db1.containsKey(rs.getString(1)) &&
                        db1.get(rs.getString(1)).containsID(rs.getInt(2))){
                    if (!commons.containsKey(rs.getString(1)))
                        commons.put(rs.getString(1), new BeanForSQLite(rs.getString(1)));
                    commons.get(rs.getString(1)).addID(rs.getInt(2));
                }
                if (!db2.containsKey(rs.getString(1)))
                    db2.put(rs.getString(1), new BeanForSQLite(rs.getString(1)));
                db2.get(rs.getString(1)).addID(rs.getInt(2));
            }

            List<BeanForSQLite> sortedFks = new ArrayList<>(commons.values());
            Collections.sort(sortedFks);

            String commonFKs = "";
            int upTo = 0;
            for (int i = 0; i < sortedFks.size() && upTo < u+c; ++i) {
                String key = sortedFks.get(i).getForeignKey();
                commonFKs += DBSplit_from_higher.printStr(sortedFks.get(i).getRowIds(), 0, (u+c) - upTo) + ", ";
                upTo += sortedFks.get(i).size();
                if (db1.containsKey(key)) {
                    db1.get(key).removeAll(commons.get(key).getRowIds());
                    if (db1.get(key).size() == 0) db1.remove(key);
                }
                if (db2.containsKey(key)) {
                    db2.get(key).removeAll(commons.get(key).getRowIds());
                    if (db2.get(key).size() == 0) db2.remove(key);
                }
            }

            if (commonFKs.length() > 2)
                commonFKs = commonFKs.substring(0, commonFKs.length()-2);

            sortedFks = new ArrayList<>(db1.values());
            Collections.sort(sortedFks);
            upTo = 0;

            String deleteNfromDB1 = "";
            for (int i = 0; i < sortedFks.size() && upTo < n; ++i) {
                String key = sortedFks.get(i).getForeignKey();
                deleteNfromDB1 += DBSplit_from_higher.printStr(sortedFks.get(i).getRowIds(), 0, n - upTo) + ", ";
                upTo += sortedFks.get(i).size();

                if (db2.containsKey(key)) {
                    db2.get(key).removeAll(db1.get(key).getRowIds());
                    if (db2.get(key).size() == 0) db2.remove(key);
                }
            }
            if (deleteNfromDB1.length() > 2)
                deleteNfromDB1 = deleteNfromDB1.substring(0, deleteNfromDB1.length()-2);

            sortedFks = new ArrayList<>(db2.values());
            Collections.sort(sortedFks);
            upTo = 0;

            String deleteMfromDB2 = "";
            for (int i = 0; i < sortedFks.size() && upTo < m; ++i) {
                deleteMfromDB2 += DBSplit_from_higher.printStr(sortedFks.get(i).getRowIds(), 0, m - upTo) + ", ";
                upTo += sortedFks.get(i).size();
            }
            if (deleteMfromDB2.length() > 2)
                deleteMfromDB2 = deleteMfromDB2.substring(0, deleteMfromDB2.length()-2);

            returnValue.set(null, fk);
            returnValue.setCommons(commonFKs);
            returnValue.setDeleteDB1(deleteNfromDB1);
            returnValue.setDeleteDB2(deleteMfromDB2);

            if (DBSplit_from_higher.reporting)
                MainDebug.report.get(table).setAlgorithm_knapsackTime((double)(System.currentTimeMillis()-knapsackTimer)/1000);

            return returnValue;

        } catch (Exception e) {
            System.out.println("Error in SQLKnapsack for " + table + " on FK " + fk.getName() + ": " + e.getMessage());
            System.out.println("\t" + query);
            e.printStackTrace();
        } finally {
            DBConnection.closeSt(st);
            DBConnection.closeRs(rs);
        }
        return returnValue;
    }
}

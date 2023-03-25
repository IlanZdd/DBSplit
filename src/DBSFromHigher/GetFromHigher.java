package src.DBSFromHigher;

import Graph.ForeignKeyColumn;
import src.Other.DBConnection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static src.DBSFromHigher.DBSplit_from_higher.graph;

public class GetFromHigher {

    protected static KS_Return MySqlAndSources (String table, List<ForeignKeyColumn> allFKs, int rR, int c, int n) throws SQLException {
        long timer = System.currentTimeMillis();
        ForeignKeyColumn fk;
        int fkIndex = 0;
        KS_Return returnValue = new KS_Return();
        int length = 0;
        boolean allOne = true;
        int sum = 0;

        try {
            while (fkIndex < allFKs.size() && (allOne || length == 0 || sum < (rR+c+n))) {
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

                //initialize flags
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

                    if (sum >= rR+c+n) break;
                }
                // if we have a better key, or this key is useless we keep searching
                if ((returnValue.hasValues() && returnValue.getSum()> sum) || length == 0) continue;

                // otherwise we set this key as viable
                returnValue.set(fks, fk);
                returnValue.setSum(sum);

                // if all flags are off, we chose this key
                if (!(allOne || sum < (rR + c + n)))
                    return returnValue;
            }
        } catch (Exception se) {
            System.out.println("Error in Knapsack for " + table + " on FK " +
                    allFKs.get(clamp(fkIndex-1)).getName() + ": " + se.getMessage());
            throw se;
        }

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
        return query + (" GROUP BY " + fk.getName() + " ORDER BY count DESC");
    }

    private static int clamp(int i) {
        return Math.max(i, 0);
    }

    public static KS_Return SQLiteMid(String table, List<ForeignKeyColumn> allFKs, int n, int m, int rR, int c) throws Exception {
        long knapsackTimer = System.currentTimeMillis();
        int fkIndex = 0;
        ForeignKeyColumn fk = null;
        KS_Return returnValue = new KS_Return();
        Statement st = null;
        ResultSet rs = null;
        String query;

        try {
            // Gets the unreferenced records from DB1 and DB2, grouped by foreign key, and searches for common ones;
            fk = allFKs.get(fkIndex);

            Map<String, BeanForSQLite> db1 = new HashMap<>();
            Map<String, BeanForSQLite> db2 = new HashMap<>();
            Map<String, BeanForSQLite> commons = new HashMap<>();

            // Writes the gathering query, that excludes records that are referenced in the DB
            // The query is the same for DB1 and DB2
            query = "SELECT " + fk.getName() + ", rowid FROM " + table + " WHERE ";

            Map<String, List<ForeignKeyColumn>> map = graph.getForeignKeysReferringTo(table);
            for (String s : map.keySet()) {
                for (ForeignKeyColumn foreignKey : map.get(s)) {
                    query += foreignKey.getReferredPrimaryKey() + " NOT IN ( SELECT DISTINCT " + foreignKey.getName() +
                            " FROM " + s;
                    if (fk.isNullable())
                        query += " AND " + fk.getName() + " IS NOT NULL";
                    query += ") AND ";
                }
            }
            query = query.substring(0, query.length() - 5) + " GROUP BY " + fk.getName() + ", rowid";


            // Query is run on DB1; resulting rowids are stored by foreign key in a map
            DBConnection.closeConn();
            DBConnection.setConn(
                    DBSplit_from_higher.DBMS, DBSplit_from_higher.sv,
                    DBSplit_from_higher.username, DBSplit_from_higher.password,
                    DBSplit_from_higher.DB1);
            st = DBConnection.getConn().createStatement();
            rs = st.executeQuery(query);

            while (rs.next()){
                if (!db1.containsKey(rs.getString(1)))
                    db1.put(rs.getString(1), new BeanForSQLite(rs.getString(1)));
                db1.get(rs.getString(1)).addID(rs.getInt(2));
            }


            // Query is run on DB2; resulting rowids are stored by foreign key in a map
            //   If the rowid is also in DB1 it will be added to a map of commons
            DBConnection.closeConn();
            DBConnection.setConn(
                    DBSplit_from_higher.DBMS, DBSplit_from_higher.sv,
                    DBSplit_from_higher.username, DBSplit_from_higher.password,
                    DBSplit_from_higher.DB2);
            st = DBConnection.getConn().createStatement();
            rs = st.executeQuery(query);

            while (rs.next()){
                //Adds to common if also in DB1
                if (db1.containsKey(rs.getString(1)) &&
                        db1.get(rs.getString(1)).containsID(rs.getInt(2))){
                    if (!commons.containsKey(rs.getString(1)))
                        commons.put(rs.getString(1), new BeanForSQLite(rs.getString(1)));
                    commons.get(rs.getString(1)).addID(rs.getInt(2));
                }
                //Adds to DB2
                if (!db2.containsKey(rs.getString(1)))
                    db2.put(rs.getString(1), new BeanForSQLite(rs.getString(1)));
                db2.get(rs.getString(1)).addID(rs.getInt(2));
            }

            StringBuilder commonFKs = new StringBuilder();
            StringBuilder deleteNFromDB1 = new StringBuilder();
            StringBuilder deleteMFromDB2 = new StringBuilder();

            //Sorts the common foreign keys objects by number of records that use it
            List<BeanForSQLite> sortedFks = new ArrayList<>(commons.values());
            Collections.sort(sortedFks);

            // Select n+c records
            int upTo = 0;
            for (int i = 0; i < sortedFks.size() && upTo < rR + c; ++i) {
                String key = sortedFks.get(i).getForeignKey();
                commonFKs.append(
                        DBSplit_from_higher.printStr(sortedFks.get(i).getRowIds(), 0, (rR + c) - upTo))
                        .append(", ");
                upTo += sortedFks.get(i).size();

                if (db1.containsKey(key)) { // Removes from DB1 map, and removes key if empty
                    db1.get(key).removeAll(commons.get(key).getRowIds());
                    if (db1.get(key).size() == 0) db1.remove(key);
                }
                if (db2.containsKey(key)) { // Removes from DB2 map, and removes key if empty
                    db2.get(key).removeAll(commons.get(key).getRowIds());
                    if (db2.get(key).size() == 0) db2.remove(key);
                }
            }

            sortedFks = new ArrayList<>(db1.values());
            Collections.sort(sortedFks);

            // Select n records
            upTo = 0;
            for (int i = 0; i < sortedFks.size() && upTo < n; ++i) {
                String key = sortedFks.get(i).getForeignKey();
                deleteNFromDB1.append(
                        DBSplit_from_higher.printStr(sortedFks.get(i).getRowIds(), 0, n - upTo))
                        .append(", ");
                upTo += sortedFks.get(i).size();

                if (db2.containsKey(key)) { // Removes from DB2 map, and removes key if empty
                    db2.get(key).removeAll(db1.get(key).getRowIds());
                    if (db2.get(key).size() == 0) db2.remove(key);
                }
            }

            sortedFks = new ArrayList<>(db2.values());
            Collections.sort(sortedFks);

            // Select m records
            upTo = 0;
            for (int i = 0; i < sortedFks.size() && upTo < m; ++i) {
                deleteMFromDB2.append(
                        DBSplit_from_higher.printStr(sortedFks.get(i).getRowIds(), 0, m - upTo))
                        .append(", ");
                upTo += sortedFks.get(i).size();
            }

            // removes the last ", "
            if (commonFKs.length() > 2)
                commonFKs = new StringBuilder(commonFKs.substring(0, commonFKs.length() - 2));
            if (deleteNFromDB1.length() > 2)
                deleteNFromDB1 = new StringBuilder(deleteNFromDB1.substring(0, deleteNFromDB1.length() - 2));
            if (deleteMFromDB2.length() > 2)
                deleteMFromDB2 = new StringBuilder(deleteMFromDB2.substring(0, deleteMFromDB2.length() - 2));

            // Sets the return value
            returnValue.set(null, fk);
            returnValue.setCommons(commonFKs.toString());
            returnValue.setDeleteDB1(deleteNFromDB1.toString());
            returnValue.setDeleteDB2(deleteMFromDB2.toString());

            return returnValue;
        } catch (Exception e) {
            System.out.println("Error in SQLite-mid for " + table + " on FK " + fk.getName() + ": " + e.getMessage());
            //System.out.println("\t" + query);
            throw e;
        } finally {
            DBConnection.closeSt(st);
            DBConnection.closeRs(rs);
            DBConnection.closeConn();
        }
    }
}

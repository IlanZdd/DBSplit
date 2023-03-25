package src.DBSKnapsack;

import Graph.ForeignKeyColumn;
import src.Other.DBConnection;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

import static src.DBSKnapsack.DBSplit_Knapsack.graph;

public class Knapsack {
    /* Parallel lists */
    private static List<String> fks;
    private static List<Integer> fksValues;

    /* Best values during KS */
    static BitSet bestBitSet;
    private static int allValueSum;

    /* To avoid unnecessary steps */
    private static int length =-1;
    private static boolean allOne;
    private static int sum;

    /** Function for Sources and Mid-node for MySQL; multiple execution are enabled to avoid bad fks */
    protected static KS_Return entryPointSources_MySQL(String table, List<ForeignKeyColumn> allFKs, int neededRecords, int c, int n) {

        int fkIndex = 0;
        KS_Return returnValue = new KS_Return();
        length = 0;
        allOne = true;
        sum = 0;

        //  From the first Fk onward, tries to find the optimal solution to the knapsack problem,
        //       where the items are the foreign keys mentioned in the table and the relative values are how many
        //       times they are mentioned
        //      Search will continue on subsequent FKs until last one, or KS is executed.
        try {
            while (fkIndex < allFKs.size() && (allOne || length == 0 || sum < neededRecords)) {
                List<String> returnList = new ArrayList<>();
                ForeignKeyColumn fk = allFKs.get(fkIndex);
                ++fkIndex;
                fksValues = new ArrayList<>();
                fks = new ArrayList<>();
                allValueSum = 0;
                int counter_indexForRefs = 0;
                boolean flag = true;
                int indexForRefs = 0;

                gatherer(table, fk, neededRecords);


                //      if no free foreign key appears in the table, tries other tables (or exits)
                if (length == 0) { continue; }

                //      If the Fk is a String, adds ' ' around the values
                String x = "";
                if (fk.getDatatype() == 12)
                    x = "'";

                //      If it found only sum < neededRecords FKs (and it's either the first execution or previous ones found
                //      less than this one), all of them will be returned
                if (sum < neededRecords) {
                    if (!returnValue.hasValues() || sum>returnValue.getSum()) {
                        returnValue.clear();
                        for (int i = 0; i < length; ++i) {
                            returnList.add(x + fks.get(i) + x);
                            //      The first u records are deleted from DB1 in overlapping mode;
                            //       searches the index from which records' references will be added,
                            //       to avoid adding useless ones to DB2
                            if (flag && (counter_indexForRefs + fksValues.get(i)) <= sum-c-n) {
                                counter_indexForRefs += fksValues.get(i);
                                ++indexForRefs;
                            }
                            else flag = false;
                        }
                        returnValue.set(returnList, fk);
                        returnValue.setAddReferencesOnward(indexForRefs);
                        returnValue.setSum(sum);
                    }
                    continue;
                }

                //      If all found FK values have value '1', it will take only neededRecords of them,
                //      without the need of running the knapsack (only if it's the first execution)
                if (allOne) {
                    if (!returnValue.hasValues()) {
                        for (int i = 0; i < Math.min(neededRecords, length); ++i) {
                            returnList.add(x + fks.get(i) + x);
                            if (flag && (counter_indexForRefs + fksValues.get(i)) <= neededRecords-c-n) {
                                counter_indexForRefs += fksValues.get(i);
                                ++indexForRefs;
                            }
                            else flag = false;
                        }
                        returnValue.set(returnList, fk);
                        returnValue.setAddReferencesOnward(indexForRefs);
                        returnValue.setSum(sum);
                    }
                    continue;
                }

                //      Having completed the knapsack, it will return the best set it was found
                returnValue.clear();
                for (int i = 0; i < length; ++i) {
                    if (bestBitSet.get(i)) {
                        returnList.add(x + fks.get(i) + x);
                        if (flag && (counter_indexForRefs + fksValues.get(i)) <= neededRecords-c-n) {
                            counter_indexForRefs += fksValues.get(i);
                            ++indexForRefs;
                        }
                        else flag = false;
                    }
                }
                returnValue.set(returnList, fk);
                returnValue.setAddReferencesOnward(indexForRefs);
                returnValue.setSum(sum);
                break;
            }
            return returnValue;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return returnValue;
    }

    /** Forms and executes the query, calls the knapsack if needed */
    private static void gatherer(String table, ForeignKeyColumn fk, int ucn) {
        //TODO Update to consider a NULL Fk, for the possibility of adding fewer references
        try {
            //  Counts all the mentions of a specific FK in the table
            String query = "SELECT count(*), " + fk.getName() + " FROM ";
            if (DBSplit_Knapsack.DBMS.equalsIgnoreCase("mysql"))
                query+= DBSplit_Knapsack.DB1 + ".";
            query+= table;

            //      if the table is a mid-node, FKs of referred records will not be considered
            //      not to ruin referential integrity
            Map<String, List<ForeignKeyColumn>> map = graph.getForeignKeysReferringTo(table);

            if (map.size() > 0) {
                query += " WHERE ";
                for (String s : map.keySet()) {
                    for (ForeignKeyColumn foreignKey : map.get(s)) {
                        query += fk.getName() + " NOT IN (SELECT DISTINCT " + fk.getName() +
                                " FROM " + DBSplit_Knapsack.DB1 + "." + table + " WHERE ";
                        query += foreignKey.getReferredPrimaryKey() + " IN (SELECT DISTINCT " + foreignKey.getName() +
                                " FROM " + DBSplit_Knapsack.DB1 + "." + s + ")";
                        if (fk.isNullable())
                            query += " and " + fk.getName() + " IS NOT NULL";
                        query += ") and ";
                    }
                }
                query = query.substring(0, query.length() - 5);
            }
            query += " GROUP BY " + fk.getName();

            Statement st = DBConnection.getConn().createStatement();
            ResultSet rs = st.executeQuery(query);

            //initialize value
            length = 0;
            allOne = true;
            sum = 0;
            bestBitSet = new BitSet();

            //      Puts in parallel lists the feasible FKs and relative count,
            //       counts how many FKs were found, the total sum and searches for not-single instances
            while (rs.next()) {
                fks.add(length, rs.getString(2));
                fksValues.add(length, rs.getInt(1));
                if (fksValues.get(length) != 1) allOne = false;
                sum += fksValues.get(length);
                ++length;
            }
            if (allOne || sum < ucn || length == 0) {
                return;
            }

            actualKnapsack(ucn, fksValues, length);
        } catch (Exception se) {
            System.out.println("Error in Knapsack for " + table + " on FK " + fk.getName()+": " + se.getMessage());
            se.printStackTrace();
        }
    }

    /** Function for Mid-node in SQLite; can't query between DBs, so it needs a different logic */
    protected static KS_Return entryPointMidSQLite(String table, List<ForeignKeyColumn> allFKs, int n, int m, int u, int c) {
        length = 0;
        ForeignKeyColumn fk = null;
        KS_Return returnValue = new KS_Return();
        Statement st = null;
        ResultSet rs = null;
        String query = null;

        try {
            //get common u+c records
            //get disjoint n/m records to delete
            fk = allFKs.get(0);
            Map<String, ArrayList<Integer>> mapDB1 = new HashMap<>();
            Map<String, ArrayList<Integer>> mapDB2 = new HashMap<>();
            Map<String, ArrayList<Integer>> mapCommons = new HashMap<>();

            // Select non-referenced fks and how many times they appear
            query = "SELECT " + fk.getName() + ", rowid FROM " + table + " WHERE ";

            Map<String, List<ForeignKeyColumn>> map = graph.getForeignKeysReferringTo(table);
            if (map.size() > 0) {
                for (String s : map.keySet()) {
                    for (ForeignKeyColumn foreignKey : map.get(s)) {
                        query += foreignKey.getReferredPrimaryKey() + " NOT IN (" +
                                "SELECT DISTINCT " + foreignKey.getName() + " FROM " + s;
                        if (fk.isNullable())
                            query += " and " + fk.getName() + " IS NOT NULL";
                        query += ") AND ";
                    }
                }
            }
            query = query.substring(0, query.length() - 5) + " GROUP BY " + fk.getName() + ", rowid";

            DBConnection.closeConn();
            DBConnection.setConn(DBSplit_Knapsack.DBMS, DBSplit_Knapsack.sv, DBSplit_Knapsack.username, DBSplit_Knapsack.password, DBSplit_Knapsack.DB1);
            st = DBConnection.getConn().createStatement();
            rs = st.executeQuery(query);

            // Gets all non-referenced fks in DB1
            while (rs.next()){
                if (!mapDB1.containsKey(rs.getString(1)))
                    mapDB1.put(rs.getString(1), new ArrayList<>());
                mapDB1.get(rs.getString(1)).add(rs.getInt(2));
            }

            DBConnection.closeConn();
            DBConnection.setConn(DBSplit_Knapsack.DBMS, DBSplit_Knapsack.sv, DBSplit_Knapsack.username, DBSplit_Knapsack.password, DBSplit_Knapsack.DB2);
            st = DBConnection.getConn().createStatement();
            rs = st.executeQuery(query);

            // Gets all non-referenced fks in DB2 and checks the common ones
            while (rs.next()){
                if (mapDB1.containsKey(rs.getString(1)) &&
                        mapDB1.get(rs.getString(1)).contains(rs.getInt(2))){
                    if (!mapCommons.containsKey(rs.getString(1)))
                        mapCommons.put(rs.getString(1), new ArrayList<>());
                    mapCommons.get(rs.getString(1)).add(rs.getInt(2));
                }
                if (!mapDB2.containsKey(rs.getString(1)))
                    mapDB2.put(rs.getString(1), new ArrayList<>());
                mapDB2.get(rs.getString(1)).add(rs.getInt(2));
            }

            ArrayList<Integer> fksCommonCount = new ArrayList<>();
            ArrayList<String> fksCommon = new ArrayList<>();
            for (String key : mapCommons.keySet()) {
                fksCommonCount.add(mapCommons.get(key).size());
                fksCommon.add(key);
            }

            BitSet chosenBits = actualKnapsack(u+c, fksCommonCount, fksCommonCount.size());
            String commonFKs = "";
            for (int i = 0; i < mapCommons.size(); ++i) {
                if (chosenBits.get(i)) {
                    commonFKs += DBSplit_Knapsack.listToString(mapCommons.get(fksCommon.get(i)), 0, fksCommonCount.get(i)) + ", ";
                    if (mapDB1.containsKey(fksCommon.get(i))){
                        mapDB1.get(fksCommon.get(i)).removeAll(mapCommons.get(fksCommon.get(i)));
                        if (mapDB1.get(fksCommon.get(i)).size() == 0) mapDB1.remove(fksCommon.get(i));
                    }
                    if (mapDB2.containsKey(fksCommon.get(i))){
                        mapDB2.get(fksCommon.get(i)).removeAll(mapCommons.get(fksCommon.get(i)));
                        if (mapDB2.get(fksCommon.get(i)).size() == 0) mapDB2.remove(fksCommon.get(i));
                    }
                }
            }
            if (commonFKs.length() > 2)
                commonFKs = commonFKs.substring(0, commonFKs.length()-2);

            fksCommon.clear();
            fksCommonCount.clear();
            for (String key : mapDB1.keySet()) {
                fksCommonCount.add(mapDB1.get(key).size());
                fksCommon.add(key);
            }
            chosenBits = actualKnapsack(n, fksCommonCount, fksCommonCount.size());
            String deleteNFromDB1 = "";
            for (int i = 0; i < mapDB1.size(); ++i) {
                if (chosenBits.get(i)) {
                    deleteNFromDB1 += DBSplit_Knapsack.listToString(mapDB1.get(fksCommon.get(i)), 0, fksCommonCount.get(i)) + ", ";
                    if (mapDB2.containsKey(fksCommon.get(i))) {
                        mapDB2.get(fksCommon.get(i)).removeAll(mapDB1.get(fksCommon.get(i)));
                        if (mapDB2.get(fksCommon.get(i)).size() == 0) mapDB2.remove(fksCommon.get(i));
                    }
                }
            }
            if (deleteNFromDB1.length() > 2)
                deleteNFromDB1 = deleteNFromDB1.substring(0, deleteNFromDB1.length()-2);

            fksCommon.clear();
            fksCommonCount.clear();
            for (String key : mapDB2.keySet()) {
                fksCommonCount.add(mapDB2.get(key).size());
                fksCommon.add(key);
            }
            chosenBits = actualKnapsack(m, fksCommonCount, fksCommonCount.size());
            String deleteMFromDB2 = "";
            for (int i = 0; i < mapDB2.size(); ++i) {
               if (chosenBits.get(i))
                    deleteMFromDB2 += DBSplit_Knapsack.listToString(mapDB2.get(fksCommon.get(i)), 0, fksCommonCount.get(i)) + ", ";
            }
            if (deleteMFromDB2.length() > 2)
                deleteMFromDB2 = deleteMFromDB2.substring(0, deleteMFromDB2.length()-2);

            returnValue.set(null, fk);
            returnValue.setCommons(commonFKs);
            returnValue.setDeleteDB1(deleteNFromDB1);
            returnValue.setDeleteDB2(deleteMFromDB2);

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

    /** The function that actually executes the KS */
    private static BitSet actualKnapsack(int weight, List<Integer> fk_values, int length) {
        //      Knapsack execution: since KS only references indexes 'i' and 'i-1', it only needs those
        //       two arrays; execution as usual, updates a "BestBitSet" with the max value found if a
        //       higher value is found or a similar value that uses less FKs; if it reaches neededRecords,
        //       the execution stops.
        //      Translate the index 'i' (1) at 'i-1' (0) before the next execution, creating the bestBitSet at 0, 0
        //       that is never created in the above steps.

        //create objects
        int[][] ks_best_weights = new int[2][weight + 1];
        BitSet[][] chosenItemsBit = new BitSet[2][weight + 1];

        for (int j = 0; j <= weight; ++j) {
            ks_best_weights[0][j] = 0;
            chosenItemsBit[0][j] = new BitSet();
        }

        int temp;
        allValueSum = -1;
        for (int i = 1; i <= length; ++i) {
            for (int p = 1; p <= weight; ++p) {
                //System.out.println("i: "+i+", p: "+p + ", length: "+ length+ ", ucn: "+ ucn);
                if (fk_values.get(i - 1) <= p)
                    temp = ks_best_weights[0][clamp(p - fk_values.get(i - 1))] + fk_values.get(i - 1);
                else temp = -1;
                chosenItemsBit[1][p] = new BitSet();
                if (ks_best_weights[0][p] > temp) {
                    ks_best_weights[1][p] = ks_best_weights[0][p];
                    chosenItemsBit[1][p] = (BitSet) chosenItemsBit[0][p].clone();
                } else {
                    ks_best_weights[1][p] = temp;
                    chosenItemsBit[1][p] = (BitSet) chosenItemsBit[0][clamp(p - fk_values.get(i - 1))].clone();
                    chosenItemsBit[1][p].flip(i - 1);

                    if ((ks_best_weights[1][p] > allValueSum) ||
                            (ks_best_weights[1][p] == allValueSum &&
                                    bestBitSet.cardinality() > chosenItemsBit[1][p].cardinality())) {
                        bestBitSet = (BitSet) chosenItemsBit[1][p].clone();
                        allValueSum = ks_best_weights[1][p];
                    }
                    if (allValueSum == weight) return bestBitSet;
                }
            }
            ks_best_weights[0] = ks_best_weights[1].clone();
            chosenItemsBit[0] = chosenItemsBit[1].clone();
            chosenItemsBit[0][0] = new BitSet();
        }
        return bestBitSet;
    }

    private static int clamp(int i) { return Math.max(i, 0); }
}

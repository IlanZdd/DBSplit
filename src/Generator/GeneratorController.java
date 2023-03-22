package src.Generator;

import Graph.*;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class GeneratorController {
    String dbName;
    Map<String, Generator> generators;
    Graph graph;

    int patternPerc, alterPerc, combinePerc, noMutationPerc, nullablePerc;
    public GeneratorController(String dbName, Graph dbGraph,  int pPerc, int aPerc, int cPerc, int noPerc, int nPerc) {
        this.dbName = dbName;
        patternPerc = pPerc;
        alterPerc = aPerc;
        combinePerc = cPerc;
        noMutationPerc = noPerc;
        nullablePerc = nPerc;
        graph = dbGraph;
        generators = new HashMap<>();
        GeneratorStorage.initialize();
    }

    public void addGenerator(String table, Connection connection) {

        Generator generator = new Generator(graph, table, connection, patternPerc, alterPerc, combinePerc, noMutationPerc, nullablePerc);
        generators.put(table, generator);
    }

    public String generateValue(String generatorName, String fieldName, boolean getRandom) throws Exception {

        String value = checkEscapes(generators.get(generatorName).generateValue(fieldName, getRandom));

        return value;
    }


    public void updateForeignValues(String generatorName, String fieldName, String value) {
        Column col = graph.searchColumnInTable(fieldName, generatorName);
        //Updates ForeignKeys possible generated values
        if (col.isPrimaryKey()) {
            List<String> referring = graph.getTablesReferringTo(generatorName);
            for (String table : referring) {
                if (generators.containsKey(table)) {
                    List<ForeignKeyColumn> fks = graph.getForeignKeysInTable(table);
                    for (ForeignKeyColumn fk : fks){
                        if (fk.getReferredPrimaryKey().equals(col.getName())) {
                            //TODO check foreign values of autoincrementing primary keys
                            generators.get(table).addForeignValue(fk.getName(), value);

                        }
                    }


                }
            }


        }
    }
    public void updateTotal(String table, String name) {
        generators.get(table).updateTotal(name);
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
}

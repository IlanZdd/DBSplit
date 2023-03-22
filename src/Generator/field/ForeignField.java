package src.Generator.field;

import java.util.HashSet;
import java.util.List;
import java.util.Random;

public class ForeignField extends Field {
    private String referencedTable;
    private String referencedPk;
    private List<String> items;

    public ForeignField(String name, boolean isPrimary, String referencedTable, String referencedPk, List<String> items){
        super(name, isPrimary);
        this.referencedTable = referencedTable;
        this.referencedPk = referencedPk;
        this.items = items;
    }

    public String getRandomForeignValue() {
        String s = items.get((new Random()).nextInt(items.size()));
        return s;
    }

    public String getReferenceTable() {
        return referencedTable;
    }

    public String getReferencedPrimaryKey() {
        return referencedPk;
    }

    @Override
    public String toString() {
        return super.toString() + "\n" +
                "classes.generator.ForeignField: " +
                "referencedtable: " + referencedTable + "\nreferencedPk: " + referencedPk;
    }

    public void updateValues(List<String> list) {
        if (new HashSet<>(items).containsAll(list)) return;
        list.removeAll(items);
        items.addAll(list);
    }

    public void addValue(String value) {

        if (!items.contains(value)) {
            items.add(value);
        }
    }
}

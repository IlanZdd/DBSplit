package src.DBSFromHigher;

import java.util.*;

public class BeanForSQLite implements Comparable<BeanForSQLite> {
    private final String foreignKey;
    private final List<Integer> rowIds;

    public BeanForSQLite(String fk) {
        foreignKey = fk;
        rowIds = new ArrayList<>();
    }

    protected List<Integer> getRowIds() {
        return rowIds;
    }
    protected String getForeignKey() {
        return foreignKey;
    }
    protected void removeAll(Collection<Integer> collection) {
        rowIds.removeAll(collection);
    }

    protected void addID (int i) {
        rowIds.add(i);
    }

    protected boolean containsID (int i) {
        return rowIds.contains(i);
    }

    protected int size() { return rowIds.size(); }

    @Override
    public int compareTo(BeanForSQLite o) {
        return this.rowIds.size()-o.rowIds.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BeanForSQLite that = (BeanForSQLite) o;
        return Objects.equals(getForeignKey(), that.getForeignKey());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getForeignKey());
    }
}

package src.DBSFromHigher;

import Graph.ForeignKeyColumn;

import java.util.Arrays;
import java.util.List;

public class KS_Return {
    protected List<String> result;
    protected ForeignKeyColumn fk;
    protected String commons;
    protected String deleteDB1;
    protected String deleteDB2;
    protected int sum;
    protected int addReferencesOnwardIndex;
    protected int commonRecordsIndex;
    public KS_Return() {
        this.result = null;
        this.fk = null;
        this.commons = "";
        this.deleteDB1 = "";
        this.deleteDB2 = "";
        sum = -1;
        addReferencesOnwardIndex = -1;
    }

    public List<String> getResult() {
        return result;
    }

    public void setCommons(String commons) {
        this.commons = commons;
    }

    public void setDeleteDB1(String deleteDB1) {
        this.deleteDB1 = deleteDB1;
    }

    public void setDeleteDB2(String deleteDB2) {
        this.deleteDB2 = deleteDB2;
    }

    public String getCommons() {
        return commons;
    }

    public String getDeleteDB1() {
        return deleteDB1;
    }

    public String getDeleteDB2() {
        return deleteDB2;
    }

    public void set(List<String> result, ForeignKeyColumn fk) {
        this.result = result;
        this.fk = fk;
    }

    public void clear() {
        this.result = null;
        this.fk = null;
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }

    public int getAddReferencesOnward() {
        return addReferencesOnwardIndex;
    }

    public void setAddReferencesOnward(int addReferencesOnwardIndex) {
        this.addReferencesOnwardIndex = addReferencesOnwardIndex;
    }

    public int getCommonRecordsIndex() {
        return commonRecordsIndex;
    }

    public void setCommonRecordsIndex(int commonRecordsIndex) {
        this.commonRecordsIndex = commonRecordsIndex;
    }

    public ForeignKeyColumn getFk() {
        return fk;
    }

    public boolean hasValues() {
        return fk!=null;
    }

    @Override
    public String toString() {
        return "KSreturn{" +
                "result=" + ((result != null) ? Arrays.toString(result.toArray()) : "null") +
                ", fk=" + fk +
                ", sum=" + sum +
                '}';
    }
}

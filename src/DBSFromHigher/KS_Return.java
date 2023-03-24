package src.DBSFromHigher;

import Graph.ForeignKeyColumn;

import java.util.Arrays;
import java.util.List;

public class KS_Return {
    private List<String> result;
    private ForeignKeyColumn fk;
    private String commons;
    private String deleteDB1;
    private String deleteDB2;
    private int sum;

    public KS_Return() {
        this.result = null;
        this.fk = null;
        this.commons = "";
        this.deleteDB1 = "";
        this.deleteDB2 = "";
        sum = -1;
    }
    public void set(List<String> result, ForeignKeyColumn fk) {
        this.result = result;
        this.fk = fk;
    }

    public ForeignKeyColumn getFk() {
        return fk;
    }
    protected List<String> getResult() {
        return result;
    }

    public int getSum() {
        return sum;
    }
    public void setSum(int sum) {
        this.sum = sum;
    }


    protected void setCommons(String commons) {
        this.commons = commons;
    }
    public String getCommons() {
        return commons;
    }

    protected void setDeleteDB1(String deleteDB1) {
        this.deleteDB1 = deleteDB1;
    }
    public String getDeleteDB1() {
        return deleteDB1;
    }

    protected void setDeleteDB2(String deleteDB2) {
        this.deleteDB2 = deleteDB2;
    }
    public String getDeleteDB2() {
        return deleteDB2;
    }

    public boolean hasValues() {
        return fk!=null;
    }

    @Override
    public String toString() {
        return "KSReturn{" +
                "result=" + ((result != null) ? Arrays.toString(result.toArray()) : "null") +
                ", fk=" + fk +
                ", sum=" + sum +
                '}';
    }
}

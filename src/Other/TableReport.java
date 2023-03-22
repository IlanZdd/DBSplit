package src.Other;

import Graph.Graph;

public class TableReport {
    private final String table;
    private final int totalRecords;
    private Graph.nodeType type;
    private final int percent;
    private final int percentageOverlapping;
    private final double expected_overlapping;
    private final double expected_DB1;
    private final double expected_DB2;

    private double Algorithm_runningTime=0;
    private double Algorithm_knapsackTime=0;
    private double Baseline_runningTime=0;

    private int Algorithm_records_in_DB1=0;
    private int Algorithm_records_in_DB2=0;
    private int Baseline_records_in_DB1=0;
    private int Baseline_records_in_DB2=0;

    private int Algorithm_variationToExpected_DB1=0;
    private int Algorithm_variationToExpected_DB2=0;
    private int Baseline_variationToExpected_DB1=0;
    private int Baseline_variationToExpected_DB2=0;

    private int Algorithm_overlappingRecords=0;
    private int Baseline_overlappingRecords=0;

    public TableReport(String table, int totalRecords, Graph.nodeType type, double expected_DB1, double expected_DB2,
                       int expectedOverlapping, int percentageOverlapping, int percent) {
        this.table = table;
        this.totalRecords = totalRecords;
        this.type = type;
        this.expected_DB1 = expected_DB1;
        this.expected_overlapping = expectedOverlapping;
        this.expected_DB2 = expected_DB2;
        this.percentageOverlapping = percentageOverlapping;
        this.percent = percent;
    }

    public void setAlgorithm(int algorithm_records_in_DB1, int algorithm_records_in_DB2) {
        Algorithm_records_in_DB1 = algorithm_records_in_DB1;
        Algorithm_records_in_DB2 = algorithm_records_in_DB2;

        Algorithm_variationToExpected_DB1 = (int) (algorithm_records_in_DB1-expected_DB1);
        Algorithm_variationToExpected_DB2 = (int)(algorithm_records_in_DB2-expected_DB2);
    }


    public void setBaseline(int baseline_records_in_DB1, int baseline_records_in_DB2) {
        Baseline_records_in_DB1 = baseline_records_in_DB1;
        Baseline_records_in_DB2 = baseline_records_in_DB2;

        Baseline_variationToExpected_DB1 = (int) (baseline_records_in_DB1 - expected_DB1);

        Baseline_variationToExpected_DB2 = (int) (baseline_records_in_DB2 - expected_DB2);
    }

    public String toCsv_slim() {
        String s = getTable().toUpperCase() +","+ type +","+getTotalRecords() +
                "," + percent + "," + percentageOverlapping;
        s += "," + roundDecimal(getAlgorithm_runningTime(),1000) + "," +
                    roundDecimal(getAlgorithm_knapsackTime(), 1000) +
                "," +roundDecimal(getBaseline_runningTime(), 1000);

        s += ","+expected_DB1+","+expected_DB2;
        s += ","+getAlgorithm_records_in_DB1()+","+getAlgorithm_records_in_DB2();
        s += ","+getBaseline_records_in_DB1()+","+getBaseline_records_in_DB2();

        s += "," + expected_overlapping;

        s += ","+getAlgorithm_overlappingRecords();
        s += ","+ getBaseline_overlappingRecords();

        s += "\n";
        return s;
    }

    public String toString () {
        String s = getTable().toUpperCase() +" ("+ type +"), records: "+getTotalRecords();
        s += "\nRunningTime:";
        s += "\n\tAlgorithm:\t\t" +getAlgorithm_runningTime() + "s, knapsack: "+getAlgorithm_knapsackTime() +
                "s (" + roundDecimal(getAlgorithm_knapsackTime()/getAlgorithm_runningTime()*100, 100) +"%)";
        s += "\n\tBaseline:\t\t" + getBaseline_runningTime()+"s";

        s += "\nExpected:\t\t\t"+expected_DB1+",\t"+expected_DB2;
        s += "\nActual:\t\t\t";
        s += "\n\tAlgorithm:\t\t"+getAlgorithm_records_in_DB1()+",\t\t"+getAlgorithm_records_in_DB2();
        s += "\n\tBaseline:\t\t"+getBaseline_records_in_DB1()+",\t\t"+getBaseline_records_in_DB2();

        s += "\nVariation from expected:";
        s += "\n\tAlgorithm:\t\t"+
                ((Algorithm_variationToExpected_DB1>0) ? "+":"") +getAlgorithm_variationToExpected_DB1()+",\t\t"+
                ((Algorithm_variationToExpected_DB2>0) ? "+":"") +getAlgorithm_variationToExpected_DB2();
        s += "\n\tBaseline:\t\t"+
                ((getBaseline_variationToExpected_DB1()>0) ? "+":"") + getBaseline_variationToExpected_DB1()+",\t\t"+
                ((getBaseline_variationToExpected_DB2()>0) ? "+":"") + getBaseline_variationToExpected_DB2();

        s += "\nVariation from expected (%):";
        s += "\n\tAlgorithm:\t\t"+
                ((Algorithm_variationToExpected_DB1>0) ? "+":"")
                +roundDecimal(getAlgorithm_variationToExpected_DB1()/expected_DB1*100, 100) +"%,\t\t"+
                ((Algorithm_variationToExpected_DB2>0) ? "+":"") +
                roundDecimal(getAlgorithm_variationToExpected_DB2()/expected_DB2*100, 100) + "%";
        s += "\n\tBaseline:\t\t"+
                ((getBaseline_variationToExpected_DB1()>0) ? "+":"") +
                roundDecimal(getBaseline_variationToExpected_DB1()/expected_DB1*100, 100) +"%,\t\t"+
                ((getBaseline_variationToExpected_DB2()>0) ? "+":"") +
                roundDecimal(getBaseline_variationToExpected_DB2()/expected_DB2*100, 100) + "%";

        s += "\nOverlapping:";
        s += "\nExpected:\t\t\t" + expected_overlapping;
        s += "\nActual:\t\t\t";

        s += "\n\tAlgorithm:\t\t"+getAlgorithm_overlappingRecords() + " ("+
                roundDecimal((getAlgorithm_overlappingRecords()*100)/(getTotalRecords()*(double)percent/100), 100) +"%)";
        s += "\n\tBaseline:\t\t"+ getBaseline_overlappingRecords() + " ("+
                roundDecimal((getBaseline_overlappingRecords()*100)/(getTotalRecords()*(double)percent/100), 100) +"%)";

        s += "\nVariation to Expected:";
        s += "\n\tAlgorithm:\t\t"+(getAlgorithm_overlappingRecords()-expected_overlapping) + " (" +
                ((getAlgorithm_overlappingRecords()-expected_overlapping>0) ? "+":"");
        if (expected_overlapping != 0)
            s += roundDecimal((getAlgorithm_overlappingRecords()-expected_overlapping)/expected_overlapping*100, 100) +"%)";
        else s += "inf)";
        s += "\n\tBaseline:\t\t"+(getBaseline_overlappingRecords()-expected_overlapping)+ " (" +
                ((getBaseline_overlappingRecords()-expected_overlapping>0) ? "+":"");
        if (expected_overlapping != 0)
            s += roundDecimal((getBaseline_overlappingRecords()-expected_overlapping)/expected_overlapping*100, 100) +"%)";
        else s += "inf)";
        s += "\n";
        return s;
    }

    private static double roundDecimal(double number, int tens) {
        return (double) (Math.round(number*tens)) / tens;
    }

    public String getTable() {
        return table;
    }

    public int getTotalRecords() {
        return totalRecords;
    }

    public double getExpected_overlapping() {
        return expected_overlapping;
    }

    public Graph.nodeType getType() {
        return type;
    }

    public void setType(Graph.nodeType type) {
        this.type = type;
    }

    public double getAlgorithm_runningTime() {
        return Algorithm_runningTime;
    }

    public void setAlgorithm_runningTime(double algorithm_runningTime) {
        Algorithm_runningTime = algorithm_runningTime;
    }

    public double getBaseline_runningTime() {
        return Baseline_runningTime;
    }

    public void setBaseline_runningTime(double baseline_runningTime) {
        Baseline_runningTime = baseline_runningTime;
    }

    public int getAlgorithm_records_in_DB1() {
        return Algorithm_records_in_DB1;
    }

    public int getAlgorithm_records_in_DB2() {
        return Algorithm_records_in_DB2;
    }

    public int getBaseline_records_in_DB1() {
        return Baseline_records_in_DB1;
    }


    public int getBaseline_records_in_DB2() {
        return Baseline_records_in_DB2;
    }
    public double getAlgorithm_variationToExpected_DB1() {
        return Algorithm_variationToExpected_DB1;
    }


    public double getAlgorithm_variationToExpected_DB2() {
        return Algorithm_variationToExpected_DB2;
    }

    public double getBaseline_variationToExpected_DB1() {
        return Baseline_variationToExpected_DB1;
    }


    public double getBaseline_variationToExpected_DB2() {
        return Baseline_variationToExpected_DB2;
    }


    public int getAlgorithm_overlappingRecords() {
        return Algorithm_overlappingRecords;
    }

    public void setAlgorithm_overlappingRecords(int algorithm_overlappingRecords) {
        Algorithm_overlappingRecords = algorithm_overlappingRecords;
    }

    public int getBaseline_overlappingRecords() {
        return Baseline_overlappingRecords;
    }

    public void setBaseline_overlappingRecords(int baseline_overlappingRecords) {
        Baseline_overlappingRecords = baseline_overlappingRecords;
    }

    public double getAlgorithm_knapsackTime() {
        return Algorithm_knapsackTime;
    }

    public void setAlgorithm_knapsackTime(double algorithm_knapsackTime) {
        Algorithm_knapsackTime = algorithm_knapsackTime;
    }
}

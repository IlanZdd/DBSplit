package src.DBMutation.Generator.field;

import java.sql.Types;
import java.util.Date;
import java.util.Objects;

public class NumberField extends Field{
    private final int decimalLength;
    private long upperBound;
    private final long lowerBound;
    private final boolean canBeGenerated;


    public NumberField(String name, boolean isPrimary, boolean isNullable, boolean canBeGenerated, int length, int minLength, int maxSize,
                       int decimalLength, long upperBound, long lowerBound, int dataType, int totalValues) {
        super(name, dataType, isPrimary, isNullable, length, minLength, maxSize, totalValues);
        this.canBeGenerated = canBeGenerated;
        this.decimalLength = decimalLength;
        this.upperBound = upperBound;
        this.lowerBound = lowerBound;
    }
    public int getRangeDim() {
        return (int)(upperBound - lowerBound);
    }
    public int getDecimalLength() {
        return decimalLength;
    }

    public boolean canBeGenerated() {
        return canBeGenerated;
    }

    public long getUpperBound() {
        return upperBound;
    }


    public long getLowerBound() {
        return lowerBound;
    }

    @Override
    public String toString() {
        return super.toString() +
                "\nNumberField{" +
                "decimalLength=" + decimalLength +
                ", upperBound=" + (getDatatype() == Types.DATE ? new Date(upperBound) : upperBound) +
                ", lowerBound=" + (getDatatype() == Types.DATE ? new Date(lowerBound) : lowerBound) +
                ", dataType=" +  getDatatype() +
                '}';
    }



    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        NumberField that = (NumberField) o;
        return getDecimalLength() == that.getDecimalLength() && getUpperBound() == that.getUpperBound() && getLowerBound() == that.getLowerBound() && getDatatype() == that.getDatatype();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getDecimalLength(), getUpperBound(), getLowerBound(), getDatatype());
    }

    public void setUpperBound(long upperBound) {
        this.upperBound = upperBound;
    }

    private int exponent = 0;
    @Override
    public void update() {
        super.update();
        if (isPrimaryKey()) {
            if (decimalLength == 0 && getTotalValues() == upperBound - lowerBound) {
                int bound = 0;
                switch (getDatatype()) {
                    case Types.INTEGER -> {
                        bound = Integer.MAX_VALUE;
                    }
                    case Types.SMALLINT, Types.TINYINT -> {
                        bound = Short.MAX_VALUE;
                    }
                }
                if (upperBound == bound) return;

                if (bound != 0)
                    upperBound = (long) Math.min(Math.pow(10, getLength() + exponent + 1), bound);
                else
                    upperBound = (long) Math.pow(10, getLength());
                exponent++;
            }
        }
    }
}

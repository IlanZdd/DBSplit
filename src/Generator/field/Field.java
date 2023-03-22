package src.Generator.field;

import Graph.Column;

import java.util.Objects;

public class Field extends Column {
    private final int length;
    private final int minLength;

    private int totalValues;
    public Field(String name, int dataType, boolean isPrimary, boolean isNullable, int length, int minLength, int maxSize, int totalValues){
        super(name, dataType, maxSize, isNullable);
        setPrimaryKey(isPrimary);
        setAutoIncrementing(false);
        this.length = length;
        this.minLength = minLength;
        this.totalValues = totalValues;
    }
    public Field(String name, boolean isPrimary, boolean isAutoIncrementing) {
        super(name, 4, 0, isPrimary, isAutoIncrementing);
        length = 0;
        minLength = 0;
        totalValues = 0;
        setNullable(false);
        setAutoIncrementing(isAutoIncrementing);

    }
    public Field(String name, boolean isPrimary) {
        super(name, 4, 0, isPrimary, false);
        length = 0;
        minLength = 0;
        totalValues = 0;
    }


    public int getTotalValues() {
        return totalValues;
    }
    public int getLength() {
        return length;
    }

    public int getMinLength() {
        return minLength;
    }


    @Override
    public String toString() {
        return "Field{" +
                "name='" + getName() + '\'' +
                ", length=" + length +
                ", minLength=" + minLength +
                ", maxSize=" + getColumnSize() +
                ", isPrimary=" + isPrimaryKey() +
                ", isAutoIncrementing=" + isAutoIncrementing() +
                ", totalValues=" + totalValues +
                '}';
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Field field = (Field) o;
        return getLength() == field.getLength() && getMinLength() == field.getMinLength() && isPrimaryKey() == field.isPrimaryKey() && isAutoIncrementing() == field.isAutoIncrementing() && getName().equals(field.getName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getLength(), getMinLength(), isPrimaryKey(), isAutoIncrementing());
    }

    public void update() {
        totalValues++;
    }
}

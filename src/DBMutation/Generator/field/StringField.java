package src.DBMutation.Generator.field;


import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.regex.Pattern;

public class StringField extends Field {
    private final Type type;
    private final List<Pattern> patterns;
    private List<String> items;
    public StringField(String name, boolean isPrimary, boolean isNullable, int length, int minLength, int maxSize, Type type, List<Pattern> patterns, List<String> items) {

        super(name, 12, isPrimary, isNullable, length, minLength, maxSize, items.size());
        this.type = type;
        this.patterns = patterns;
        this.items = items;
    }

    public String getRandomValue() {
        String s = items.get((new Random()).nextInt(items.size()));
        return s;
    }
    public void addNewValue(String value) {
        if (!items.contains(value))
            items.add(value);
    }
    @Override
    public String toString() {
        return super.toString() +
                "\nStringField{" +
                "type=" + type +
                '}';
    }

    public boolean hasPatterns() {
        //TODO In patternGenerator far ritornare null e mai lista vuota
        return patterns != null && patterns.size() > 0;
    }

    public Pattern getAPattern() {
        return patterns.get((new Random()).nextInt(patterns.size()));
    }

    public Pattern matches(String s) {
        for(Pattern pat : patterns){
            if (pat.matcher(s).matches()) return pat;
        }
        return null;
    }

    public Type getType() {
        return type;
    }



    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        StringField that = (StringField) o;
        return getType() == that.getType();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getType());
    }
}

package src.DBMutation.Generator;

import Graph.Column;
import Graph.ForeignKeyColumn;
import Graph.Graph;
import com.github.curiousoddman.rgxgen.RgxGen;
import src.DBMutation.Generator.field.*;
import patterngenerator.PatternGenerator;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;
//TODO sistemare i throw per terminare le esecuzioni più esterne
public class Generator {
    private final String table;
    private final List<Field> fields;
    private final int alterPerc;
    private final int combinePerc;
    private final int nullablePerc;
    private final int noMutationPerc;
    private final int patternPerc;

    public String getName() {
        return table;
    }
   //TODO togliere references a graph e connection
    public Generator(Graph graph, String table, Connection connection, int pPerc, int aPerc, int cPerc, int noPerc, int nPerc) throws SQLException {
        this.table = table;

        if (aPerc+cPerc+noPerc > 100)
            throw new IllegalArgumentException("Percentages' sum can't more than 100");
        else if (aPerc+cPerc+noPerc < 100)
            throw new IllegalArgumentException("Percentages' sum can't be less than 100");

        //Selezione a roulette
        //Ogni variabile identifica il bound della possibilità di ogni evento su una roulette [0, 100)
        alterPerc = aPerc;                          //[0, aPerc)
        combinePerc = aPerc + cPerc;                //[aPerc, cPerc+aPerc)
        noMutationPerc = aPerc + cPerc + noPerc;     //[cPerc+aPerc, aPerc+cPerc+noPerc == 100)

        patternPerc = pPerc;
        nullablePerc = nPerc;

        List<Column> columns = graph.getColumnsInTable(table);
        fields = new ArrayList<>();

        for (Column column : columns) {
            String query = "";

            //Se è una foreignKey crea un ForeignField
            if (column instanceof ForeignKeyColumn) {
                ForeignKeyColumn fk = (ForeignKeyColumn) column;
                List<String> list = new ArrayList<>();
                try {
                    query = "SELECT " + fk.getReferredPrimaryKey() + " " +
                            "FROM " + fk.getReferredTable();
                    ResultSet rs = connection.createStatement().executeQuery(query);
                    while (rs.next())
                        list.add(rs.getString(fk.getReferredPrimaryKey()));

                } catch (Exception e) {
                    System.out.println("There was a problem in getting the values for the foreign key " + column.getName());
                    if (!column.isNullable() && list.isEmpty()) throw e;
                }

                fields.add(new ForeignField(fk.getName(), fk.isPrimaryKey(), fk.getReferredTable(), fk.getReferredPrimaryKey(), list));
            } else if (column.isAutoIncrementing())
                fields.add(new Field(column.getName(), true, true));
            else {
                int maxLength = 0;
                int minLength = Integer.MAX_VALUE;

                //list that contains the field values
                List<String> list = new ArrayList<>();
                try {
                     Statement st = connection.createStatement();
                     query = "SELECT " + column.getName() + "" +
                            " FROM " + table + "" +
                            " WHERE " + column.getName() + " IS NOT NULL" +
                             (column.getDatatype() == Types.TIMESTAMP || column.getDatatype() == Types.DATE ? "" : " AND "+ column.getName() + "!='';");
                    ResultSet rs = st.executeQuery(query);
                    while (rs.next())
                        list.add(rs.getString(column.getName()));

                } catch (Exception e) {
                    System.out.println("There was a problem in getting the values for " + column.getName());
                }

            /*
             Crea una istanza di Field in base al datatype, contando la loro grandezza massima e minima
                    String -> prova a capire dal nome del campo che cos'è. Se non lo capisce gli da come tipo NOTFOUND
                    Numerico -> calcola minimo e massimo dei numeri presenti e conta le sue cifre decimali
                        Il tipo data è compreso in quest'ultimo poiche è rappresentabile come long
            */
                switch (column.getDatatype()) {
                    case Types.CHAR, Types.NCHAR, Types.LONGVARCHAR, Types.LONGNVARCHAR, Types.VARCHAR, Types.NVARCHAR -> {
                        String columnName = column.getName().toLowerCase();
                        Type type = Type.getType(columnName);
                        //Scorre tutti i valori per trovare lunghezza minima e massima
                        if (list.size() == 0) {
                            minLength = 0;
                            maxLength = 10;
                        } else {
                            for (String s : list) {
                                maxLength = Math.max(maxLength, s.length());
                                minLength = Math.min(minLength, s.length());
                            }
                        }

                        List<Pattern> patterns = null;
                        if (column.isPrimaryKey() && minLength == maxLength) {
                            if (column.getColumnSize() != maxLength)
                                ++maxLength;
                            else if (minLength > 0)
                                --minLength;
                            else {
                                throw new IllegalStateException("It should't be possible");
                            }
                        }
                        switch (type) {
                            case OTHER, NAME -> patterns = PatternGenerator.getPatterns(list);
                        }

                        StringField s = new StringField(column.getName(), column.isPrimaryKey(), column.isNullable(),
                                maxLength, minLength, column.getColumnSize(), type, patterns, list);

                        fields.add(s);
                    }

                    case Types.INTEGER, Types.SMALLINT, Types.BIGINT, Types.REAL, Types.FLOAT, Types.DOUBLE,
                            Types.DECIMAL, Types.DATE, Types.TINYINT, Types.TIMESTAMP -> {
                        int decimalLength = 0;
                        long lowerBound = Long.MAX_VALUE;
                        long upperBound = Long.MIN_VALUE;

                        boolean canBeGenerated = true;

                        //Sets up possible Number digits length and values bounds based on field content from DB
                        if (list.size() == 0) {
                            maxLength = column.getColumnSize();
                            minLength = 0;
                            //Non avendo nessuna informazione di dati nella colonna, il range è stabilito dalla
                            //Size della variabile nel dbms
                            upperBound = (long) Math.pow(10, maxLength - 1);
                            lowerBound = 0;
                        } else {
                            if (!column.isPrimaryKey()) {
                                long number;
                                for (String s : list) {
                                    //Timestamps and dates can be represented by long values
                                    if (column.getDatatype() == Types.TIMESTAMP) {
                                        Timestamp timestamp = Timestamp.valueOf(s);
                                        number = timestamp.getTime();
                                    } else if (column.getDatatype() == Types.DATE) {
                                        Date date = java.sql.Date.valueOf(s);
                                        number = date.getTime();
                                    } else
                                        number = (long) Double.parseDouble(s);

                                    upperBound = Math.max(upperBound, number);
                                    lowerBound = Math.min(lowerBound, number);

                                    String[] split = s.split("\\.");
                                    //Calcola min e max della lunghezza della parte intera
                                    maxLength = Math.max(maxLength, split[0].length() - (split[0].charAt(0) == '-' ? 1 : 0));
                                    minLength = Math.min(minLength, split[0].length() - (split[0].charAt(0) == '-' ? 1 : 0));

                                    if (split.length > 1)
                                        decimalLength = Math.min(minLength, split[1].length());
                                }
                            } else {
                                for (String s : list) {
                                    String[] split = s.split("\\.");
                                    //Calcola min e max della lunghezza della parte intera
                                    maxLength = Math.max(maxLength, split[0].length() - (split[0].charAt(0) == '-' ? 1 : 0));
                                    minLength = Math.min(minLength, split[0].length() - (split[0].charAt(0) == '-' ? 1 : 0));

                                    if (split.length > 1)
                                        decimalLength = Math.min(minLength, split[1].length());
                                }
                                int uBound = 0;
                                //Sets the bounds for integer and short values to their Max values to bound the bound values to avoid overflow
                                //Every other datatype have higher than that and will always bound to the long Max value
                                //and to powers of 10
                                switch(column.getDatatype()){
                                    case Types.INTEGER ->
                                        uBound = Integer.MAX_VALUE;

                                    case Types.SMALLINT, Types.TINYINT ->
                                        uBound = Short.MAX_VALUE;
                                }

                                //Clamps the bound values to avoid overflow
                                //19 is roughly the power of 10 that gives Long.MaxValue: log(2^63-1) = 18.9649
                                long longUBound = (maxLength < 19 ? (long) Math.pow(10, maxLength) : Long.MAX_VALUE);
                                lowerBound = (long) Math.pow(10,Math.min(18, minLength - 1));

                                //If its 0 then its a data type with max value more than Integer.MaxValue
                                if (uBound != 0)
                                    upperBound = Math.min(longUBound, uBound);
                                else
                                    upperBound = longUBound;

                                //If the number of records is equal to the number of possible non decimal numbers in the
                                //range, its possible that no new primary keys can be generated so the range will
                                //be enlarged by a power of 10 if possible
                                if (list.size() == (upperBound - lowerBound)) {
                                    if (maxLength < column.getColumnSize())
                                        upperBound = (long) Math.pow(10, maxLength + 1);
                                    else
                                        canBeGenerated = false;
                                }
                            }

                            //In case bounds are the same
                            if (lowerBound == upperBound) {
                                lowerBound = (long) Math.pow(10, minLength - 1);
                                upperBound = (long) Math.pow(10, maxLength);
                            }
                        }
                        fields.add(new NumberField(column.getName(), column.isPrimaryKey(), column.isNullable(), canBeGenerated, maxLength, minLength, column.getColumnSize(),
                                decimalLength, upperBound, lowerBound, column.getDatatype(), list.size()));
                    }
                    case Types.BIT -> {
                        boolean canBeGenerated = true;
                        if (column.isPrimaryKey()) {
                            if (list.size() == 2 && column.isPrimaryKey())
                                canBeGenerated = false;
                            if (list.size() == 3 && column.isPrimaryKey() && column.isNullable())
                                canBeGenerated = false;
                        }
                        NumberField n = new NumberField(column.getName(), column.isPrimaryKey(), column.isNullable(),
                                canBeGenerated, 1, 1, 1, 0, 1, 0,
                                Types.BIT, list.size());
                        fields.add(n);
                    }
                    default -> {
                        //Nel caso di default è un tipo non numerico ma non stringaSQL
                        //Temporaneamente lo tratta come una stringa di tipo NOTFOUND

                        StringField s = new StringField(column.getName(), column.isPrimaryKey(), column.isNullable(),
                                column.getColumnSize(), 0, column.getColumnSize(), Type.NOTFOUND, null, list);
                        fields.add(s);
                    }
                }
            }
        }
    }
    int violateBounds = 0;

    public String generateValue(String name, boolean getRandom) throws Exception {
        Field field = getField(name);
        if (field == null)
            throw new IllegalArgumentException(name + " not present in field list");

        Random r = new Random();
        String generatedValue = "";

        if (field.isAutoIncrementing()) return "AUTO";

        if (!(field instanceof ForeignField)) {
            if (field.isNullable() && r.nextInt(100) < nullablePerc)
                return "null";

            //TODO offset for violation of bounds might overflow or underflow
            if (field instanceof NumberField) {
                NumberField n = (NumberField) field;
                if (getRandom) {
                    long newUpperBound = Math.min(10 * n.getUpperBound(), (long)Math.pow(10, n.getColumnSize()));
                    n.setUpperBound(newUpperBound);
                }

                //Decides if it wants to try and generate a value outside the calculated bounds of the generator
                //Probably it will work. Its there just for test related purposed for the software that will use
                //this program
                //the generated value will surely be outside of the bounds
                int offset = 0;
                if (r.nextInt(100) < violateBounds) {
                    if (r.nextInt(100) < 50)
                        offset = -(n.getRangeDim() + 1);
                    else
                        offset = n.getRangeDim() + 1;
                }
                try{
                    switch (n.getDatatype()) {
                        case Types.INTEGER -> {
                            int random = r.nextInt((int) n.getLowerBound(), (int) n.getUpperBound()) + offset;
                            generatedValue = Integer.toString(random);
                            return generatedValue;
                        }
                        case Types.BIGINT -> {
                            long random = r.nextLong(n.getLowerBound(), n.getUpperBound()) + offset;
                            generatedValue = Long.toString(random);
                            return generatedValue;
                        }

                        case Types.FLOAT, Types.REAL -> {
                            float random = Generator.roundFloat(r.nextFloat(n.getLowerBound(), n.getUpperBound()) + offset, n.getDecimalLength());

                            generatedValue = Float.toString(random);
                            return generatedValue;
                        }
                        case Types.DOUBLE -> {
                            double random = Generator.roundDouble(r.nextDouble(n.getLowerBound(), n.getUpperBound()) + offset, n.getDecimalLength());
                            generatedValue = Double.toString(random);
                            return generatedValue;
                        }
                        case Types.DATE -> {
                            java.sql.Date random = new java.sql.Date(r.nextLong(n.getLowerBound(), n.getUpperBound()) + offset);
                            generatedValue = "CONVERT(\"" + random.toString() + "\", DATE)";
                            return generatedValue;
                        }
                        case Types.DECIMAL -> {
                            BigDecimal bd = BigDecimal.valueOf(r.nextDouble(n.getLowerBound(), n.getUpperBound()) + offset);
                            bd = bd.setScale(n.getDecimalLength(), RoundingMode.HALF_UP);
                            generatedValue = bd.toString();
                            return generatedValue;
                        }
                        case Types.TIMESTAMP -> {
                            Timestamp random = new Timestamp(r.nextLong(n.getLowerBound(), n.getUpperBound()) + offset);
                            generatedValue ="CONVERT(\""+ random.toString() + "\", DATETIME)";
                            return generatedValue;
                        }
                        case Types.TIME -> {
                            Time random = new Time(r.nextLong(n.getLowerBound(), n.getUpperBound()) + offset);
                            generatedValue = "CONVERT(\"" + random.toString() + "\", TIME)";
                            return generatedValue;
                        }
                        case Types.TINYINT, Types.SMALLINT -> {
                            short random = (short) (r.nextInt((int) n.getLowerBound(), (int) n.getUpperBound()) + offset);
                            generatedValue = Short.toString(random);
                            return generatedValue;
                        }
                        case Types.BIT -> {
                            //Il bit è escluso dalla possibilità di valori fuori bound perche sql darebbe sicuramente errore
                            int random = r.nextInt() % 2;
                            generatedValue = random + "";
                            return generatedValue;
                        }
                        default -> throw new IllegalStateException("Numerical type not handled, technically unreachable");
                    }
                } catch(Exception e) {
                    throw e;
                }
            }

            StringField s = (StringField) field;

           /*
                Tries to select null or empty for a values types that are not handled
            */
            if(s.getType() == Type.NOTFOUND) {
                if (s.isNullable())
                    return "null";
                else
                    return "";
            }

            String s1 = "";
            String s2 = ""; //Second value for combine mutation if needed
            int prob = r.nextInt(100);
            boolean whatMutation = true;
            int howMany = 0;
            if (s.getLength() > 3){
                whatMutation = (prob < alterPerc || (prob >= combinePerc && prob < noMutationPerc));

                if (whatMutation)
                    //Happened alterPercentage or noMutationPercentage
                    howMany = 1;
                else
                    //Happened combinePerc
                    howMany = 2;
            } else
                //The string can't be long enough to be combined
                howMany = 1;


            if (s.getTotalValues() == 0) {
                if (s.getType() != Type.OTHER && r.nextInt() < 50)
                    //TODO controllare la possibilità che generatorStorage si blocchi
                    s1 = GeneratorStorage.getValue(s);
                else
                    s1 = getRandomlyGeneratedValue(s);
                if (!whatMutation) {
                    do {
                        if (s.getType() != Type.OTHER && r.nextInt() < 50)
                            s2 = GeneratorStorage.getValue(s);
                        else
                            s2 = getRandomlyGeneratedValue(s);
                    } while (s2.equals(s1));
                }
            } else if (getRandom)
                getRandomlyGeneratedValue(s);
            else {
                /*
                    Gets the values it needs in various ways:
                        -Pattern
                        -From existing values
                        -GeneratorStorage if possible
                */
                if (patternPerc > 0 && s.hasPatterns()) {
                    int patternProbs1 = r.nextInt(100);
                    if (patternProbs1 < patternPerc) {
                        Pattern p = s.getAPattern();
                        RgxGen regGens1 = new RgxGen(p.pattern());
                        do {
                            s1 = regGens1.generate();
                        } while(s1.length() > s.getLength() || s1.length() < s.getMinLength());
                        //System.out.println("Generated s1: " + s1 + " from pattern: " + p.pattern());
                        --howMany;
                    }
                    if (!whatMutation){
                        int patternProbs2 = r.nextInt(100);
                        if (patternProbs2 < patternPerc) {
                            Pattern p = s.getAPattern();
                            RgxGen regGens2 = new RgxGen(p.pattern());
                            do {
                                s2 = regGens2.generate();
                            } while(s2.length() > s.getLength() || s2.length() < s.getMinLength());
                            //System.out.println("Generated s2: " + s2 + " from pattern: " + p.pattern());
                            --howMany;
                        }
                    }
                }


                if (howMany > 0) {
                    /*if it still needs values it didnt generate from patterns
                         -From existing values
                         -GeneratorStorage if possible
                    */
                    if (s1.isEmpty()) {
                        if (s.getType() != Type.OTHER && r.nextInt() < 50)
                            s1 = GeneratorStorage.getValue(s);
                        else
                            s1 = s.getRandomValue();
                    } else {
                        do {
                            if (s.getType() != Type.OTHER && r.nextInt() < 50)
                                s2 = GeneratorStorage.getValue(s);
                            else
                                s2 = s.getRandomValue();
                        } while(s1.equals(s2));
                    }
                    if (s2.isEmpty())
                        s2 = s.getRandomValue();
                }
            }

            if (whatMutation) {
                //Forces Altering mutation if its a primary key, to avoid duplicates
                if (prob < alterPerc || s.isPrimaryKey())
                    generatedValue = generateAlteredValue(s, s1);
                else
                    generatedValue = s1;
            } else
                generatedValue = generateCombinedValue(s1, s2);
            s.addNewValue(generatedValue);

            if (generatedValue.length() > s.getColumnSize())
                throw new IllegalStateException("Out of column size value generated");

            return generatedValue;
        }

        generatedValue = ((ForeignField) field).getRandomForeignValue();
        return generatedValue;
    }

    /**
     * Metodo che costruisce una stringa completamente randomica
     * @param field
     * @return
     */
    private static String getRandomlyGeneratedValue(StringField field) {
        Random r = new Random();
        int length =0;
        String random = "";
        if (field.getTotalValues() == 0)
            length = r.nextInt(field.getMinLength(), field.getLength());
        else {
            random = field.getRandomValue();
            length = r.nextInt(field.getLength()) + 1;
        }
        String characters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        StringBuilder builder = new StringBuilder();

        for(int i = 0; i < length; ++i) {
            char c = 0;
            if (field.getTotalValues() == 0) {
                c = characters.charAt(r.nextInt(characters.length()));
            } else {
                c = Generator.getRandomLetterAs(random.charAt(random.length() - 1));
            }
            builder.append(c);
        }
        return builder.toString();
    }



    public static String generateAlteredValue(StringField field, String value) {
        Random r = new Random();
        int howMany = 0;

        if (value.length() == 1) howMany = 1;
        else if (field.getMinLength() == value.length())
            howMany = r.nextInt(0, value.length() / 2) + 1;
        else
            howMany = r.nextInt(field.getMinLength() / 2, (int) Math.ceil((double) value.length() / 2)) + 1;

        List<Integer> altered = new ArrayList<>();


        for(int i = 0; i < howMany; ++i){
            int position = 0;
            do {
                position = r.nextInt(value.length());
            }while (altered.contains(position));

            char present = value.charAt(position);

            char character = 32;

            //Number
            if (present >= 48 && present < 58)
                character = (char) r.nextInt(48, 58);
             else
                character = Generator.getRandomLetterAs(present);

            value = value.substring(0, position) + character + value.substring(position + 1);
            altered.add(position);
        }
        return value;
    }

    private static char getRandomLetterAs(char present) {
        char[] bigVowels = {65, 69, 73, 79, 85};
        char[] smallVowels = {97, 101, 105, 111, 117};
        char[] bigConsonant = {66, 67, 68, 70, 71, 72, 74, 75, 76, 77, 78, 80, 81, 82, 83, 84, 86, 87, 88, 89, 90};
        char[] smallConsonant = {98, 99, 100, 102, 103, 104, 106, 107, 108, 109, 110, 112, 113, 114, 115, 116, 118, 119, 120, 121, 122};
        char character = 32;
        Random r = new Random();
        for(char c : bigVowels) {
            if (c == present){
                do {
                    character = bigVowels[r.nextInt(bigVowels.length)];
                } while(character == present);
                return character;
            }
        }
        for(char c : bigConsonant) {
            if (c == present){
                do {
                    character = bigConsonant[r.nextInt(bigVowels.length)];
                } while(character == present);
                return character;
            }
        }
        for(char c : smallVowels) {
            if (c == present){
                do {
                    character = smallVowels[r.nextInt(bigVowels.length)];
                } while(character == present);
                return character;
            }
        }
        for(char c : smallConsonant) {
            if (c == present){
                do {
                    character = smallConsonant[r.nextInt(bigVowels.length)];
                } while(character == present);
                return character;
            }
        }
        //Teoricamente unreachable
        character = present;
        return character;
    }

    public static String generateCombinedValue(String s1, String s2) {
        Random r = new Random();
        if (s1.length() == 1 && s2.length() == 1){
            if (r.nextInt(10) % 2 == 0)
                return s1;
            else
                return s2;
        }
        int random = r.nextInt(100);
        int s1Index = (int) Math.floor((double) s1.length() / 2);
        int s2Index = (int) Math.floor((double) s2.length() / 2);
        if (s1Index == 0) {
            ++s1Index;
            if (s2Index > 1)
                --s2Index;
        }
        if (s2Index == 0) {
            ++s2Index;
            if (s1Index > 1)
                --s1Index;
        }
        if (s1Index == 1 && s2Index == 1) {
            if (r.nextInt(10) % 2 == 0)
                return s1;
            else
                return s2;
        }

        String combined = "";
        if (random < 50)
            combined = s1.substring(0, s1Index) + (s2Index == 1 ? s2 : s2.substring(s2Index));
        else
            combined = s2.substring(0, s2Index) + (s1Index == 1 ? s1 : s1.substring(s1Index));

        return combined;

    }
    public Field getField(String name) {
        for(Field field : fields){
            if (field.getName().equalsIgnoreCase(name)) return field;
        }
        return null;
    }

    public long getOperationBound(String name, char operation) {

        if (!(getField(name) instanceof NumberField field))
            throw new IllegalArgumentException("Method cant be used on a non-numerical Field like " + name);
        else if (operation != '+' && operation != '-' && operation != '*' && operation != '/') {
            throw new IllegalArgumentException("Invalid operation " + operation);
        }

        switch (operation) {
            case '-', '/' -> {
                return field.getLowerBound();
            }
            case '+', '*' ->{
                return  field.getUpperBound();
            }

        }
        return -1;
    }

    public int getUpperBound(String name) {
        return (int)((NumberField)getField(name)).getUpperBound();
    }

    public void updateTotal(String name) {
        Field f = getField(name);
        if (f != null){
            f.update();
        }
        //System.out.println("Updated " + name);
    }

    private static double roundDouble(double number, int tens) {
        return (double) (Math.round(number*tens)) / tens;
    }

    private static float roundFloat(double number, int tens) {
        return (float) (Math.round(number*tens)) / tens;
    }

    public void addForeignValue(String foreignField, String value) {
        Field field = getField(foreignField);
        if (field != null) {
            ((ForeignField) field).addValue(value);
        }
    }
}

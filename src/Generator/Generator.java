package src.Generator;

import Graph.Column;
import Graph.ForeignKeyColumn;
import Graph.Graph;
import src.Generator.field.*;
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
    //private List<StringFieldGenerator> stringFieldGenerators;

    private final Connection connection;
    private final String table;
    private final String[] words = {"pesce", "cannuccia", "pane", "carta da forno", "Salice", "Pannacotta", "tavolo", "Paolo", "ab", "tre", "a", "e", "i", "o", "u"};
    private final String[] names  = {"Lorenzo", "Francesco", "Sara", "Calogero", "Patrizia"};
    private final String[] surnames = {"Angelicola"};
    private final String[] addresses = {"via sotto i ponti", "Angolo Bar di Gino", "Corso dei viali", "via dele vie", "le sei vie"};
    private final String[] countries = {"USA", "Italy", "Germany", "Spain", "France", "Portugal", "Brazil", "Japan", "Greece"};
    private final String[] cities = {"Milan"};
    private final String[] phoneNumbers = {"0123456789"};
    private final String[] email = {"username@domain.com"};
    private final List<Field> fields;
    private final int alterPerc;
    private final int combinePerc;
    private final int nullablePerc;
    private final int noMutationPerc;
    private final int patternPerc;

    public boolean hasPatterns(String fieldName) {
        Field f = getField(fieldName);
        if (f instanceof StringField s)
            return s.hasPatterns();
        return false;
    }

    public String getName() {
        return table;
    }
   //TODO togliere references a graph e connection
    public Generator(Graph graph, String table, Connection connection, int pPerc, int aPerc, int cPerc, int noPerc, int nPerc) {
        this.connection = connection;
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

            System.out.println(column.getName());
            String query = "";

            //Se è una foreignKey crea un ForeignField
            if (column instanceof ForeignKeyColumn) {
                ForeignKeyColumn fk = (ForeignKeyColumn) column;
                List<String> list = new ArrayList<>();
                try {
                    query = "SELECT " + fk.getReferredPrimaryKey() + " " +
                            "FROM " + fk.getReferredTable();
                    ResultSet rs = connection.createStatement().executeQuery(query);
                    while (rs.next()) {
                        list.add(rs.getString(fk.getReferredPrimaryKey()));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(column.getDatatype());
                    System.out.println(query);
                    list = new ArrayList<>();
                }
                fields.add(new ForeignField(fk.getName(), fk.isPrimaryKey(), fk.getReferredTable(), fk.getReferredPrimaryKey(), list));
            } else if (column.isAutoIncrementing()) {
                //Se è autoIncrementing allora crea un field generico primarykey con la flag isAutoIncrementing a true
                fields.add(new Field(column.getName(), true, true));
            } else {
                int maxLength = 0;
                int minLength = Integer.MAX_VALUE;

                //Lista che conterrà tutti i valori del campo
                List<String> list = new ArrayList<>();

                //Query per ottenere tutti i valori del campo presenti nella tabella

                try {
                    if (column.getDatatype() == Types.VARBINARY) {
                        fields.add(new NumberField(column.getName(), column.isPrimaryKey(),
                                column.isNullable(), true, 0, 0, column.getColumnSize(), 0, 0, 0, column.getDatatype(), 0));
                        return;
                    }
                    Statement st = connection.createStatement();
                     query = "SELECT " + column.getName() + "" +
                            " FROM " + table + "" +
                            " WHERE " + column.getName() + " IS NOT NULL" +
                             (column.getDatatype() == Types.TIMESTAMP || column.getDatatype() == Types.DATE ? "" : " AND "+ column.getName() + "!='';");
                    ResultSet rs = st.executeQuery(query);
                    while (rs.next()) {
                        list.add(rs.getString(column.getName()));
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(column.getDatatype());
                    System.out.println(query);
                    System.exit(1);
                }

            /*  Crea una istanza di Field in base al datatype, contando la loro grandezza massima e minima
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
                /*
                    Longblob = integer for some reason

                */
                    case Types.INTEGER, Types.SMALLINT, Types.BIGINT, Types.REAL, Types.FLOAT, Types.DOUBLE,
                            Types.DECIMAL, Types.DATE, Types.TINYINT, Types.TIMESTAMP -> {
                        int decimalLength = 0;
                        long lowerBound = Long.MAX_VALUE;
                        long upperBound = Long.MIN_VALUE;

                        /*
                        for (String s : list) {

                            if (!column.isPrimaryKey()) {
                                if (column.getDatatype() == Types.SMALLINT || column.getDatatype() == Types.TINYINT) {
                                    short sh = Short.parseShort(s);
                                    upperBound = Math.max(upperBound, sh);
                                    lowerBound = Math.min(lowerBound, sh);
                                }
                                if (column.getDatatype() == Types.FLOAT
                                        || column.getDatatype() == Types.REAL) {
                                    float f = Float.parseFloat(s);
                                    //Check if float and dobule minvalue dont make problem like the integer
                                    upperBound = Math.max(upperBound, (long) f);
                                    lowerBound = Math.min(lowerBound, (long) f);

                                } else if (column.getDatatype() == Types.DOUBLE) {
                                    double f = Double.parseDouble(s);
                                    upperBound = Math.max(upperBound, (long) f);
                                    lowerBound = Math.min(lowerBound, (long) f);

                                } else if (Types.DECIMAL == column.getDatatype()) {
                                    BigDecimal bd = new BigDecimal(s);
                                    upperBound = Math.max(upperBound, bd.longValue());
                                    lowerBound = Math.min(lowerBound, bd.longValue());

                                } else if (column.getDatatype() == Types.DATE) {
                                    Date date = java.sql.Date.valueOf(s);
                                    long l = date.getTime();
                                    upperBound = Math.max(upperBound, l);
                                    lowerBound = Math.min(lowerBound, l);

                                } else if (column.getDatatype() == Types.TIMESTAMP) {
                                    Timestamp timestamp = Timestamp.valueOf(s);
                                    long l = timestamp.getTime();
                                    upperBound = Math.max(upperBound, l);
                                    lowerBound = Math.min(lowerBound, l);

                                } else if (column.getDatatype() == Types.BIGINT) {
                                    long l = Long.parseLong(s);
                                    upperBound = Math.max(upperBound, l);
                                    lowerBound = Math.min(lowerBound, l);

                                } else if (column.getDatatype() == Types.INTEGER) {
                                    int i = Integer.parseInt(s);
                                    upperBound = Math.max(upperBound, i);
                                    lowerBound = Math.min(lowerBound, i);
                                }
                            }

                            //Divide la stringa in parte intera e decimale se esiste
                            String[] split = s.split("\\.");

                            //Calcola min e max della lunghezza della parte intera

                            maxLength = Math.max(maxLength, split[0].length() - (split[0].charAt(0) == '-' ? 1 : 0));
                            minLength = Math.min(minLength, split[0].length() - (split[0].charAt(0) == '-' ? 1 : 0));

                            if (split.length > 1)
                                decimalLength = Math.min(minLength, split[1].length());

                        }
                         */
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
                                    case Types.INTEGER -> {
                                        uBound = Integer.MAX_VALUE;
                                    }
                                    case Types.SMALLINT, Types.TINYINT -> {
                                        uBound = Short.MAX_VALUE;
                                    }
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
                                //range of values, its possible that no new primary keys can be generated so the range will
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
                        List<Pattern> patterns = null;
                        if (column.getDatatype() != Types.BINARY)
                            patterns = PatternGenerator.getPatterns(list);

                        StringField s = new StringField(column.getName(), column.isPrimaryKey(), column.isNullable(),
                                column.getColumnSize(), 0, column.getColumnSize(), Type.NOTFOUND, patterns, list);
                        fields.add(s);
                    }
                }
            }
        }

        /*
        for(Field f : fields) {
            System.out.println(f.toString());
        }
         */

    }


    int violateBounds = 0;

    public String generateValue(String name, boolean getRandom) throws Exception {
        Field field = getField(name);

        if (field == null)
            throw new IllegalArgumentException(name + " not present in Field list");

        Random r = new Random();
        String generatedValue = "";
        try {
            if (field.isAutoIncrementing()) return "AUTO";
        } catch(Exception e){
            System.out.println("genereted by " );
        }
        if (!(field instanceof ForeignField)) {
            if (field.isNullable() && r.nextInt(100) < nullablePerc)
                return "null";

            //TODO add for remaining types if useful
            //TODO offset for violation of bounds might overflow or underflow
            //TODO check for problem for long date and timestamps
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
                        /*
                        int random =
                                r.nextInt((int) n.getLowerBound(), (int) n.getUpperBound()) +
                                        (r.nextInt(100) < violateBounds ? (r.nextInt(100) < 50 ? n.getRangeDim() : -n.getRangeDim()) : 0);
                        */
                        generatedValue = random + "";
                        return generatedValue;
                    }
                    case Types.BIGINT -> {
                        long random = r.nextLong(n.getLowerBound(), n.getUpperBound()) + offset;
                        generatedValue = random + "";
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
                        generatedValue = bd + "";
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
                        generatedValue = random + "";
                        return generatedValue;
                    }
                    case Types.BIT -> {
                        //Il bit è escluso dalla possibilità di valori fuori bound perche sql darebbe sicuramente errore
                        int random = r.nextInt() % 2;
                        generatedValue = random + "";
                        return generatedValue;
                    }
                    default -> throw new IllegalStateException("Numerical type forgotten my bad");
                }
            } catch(Exception e) {
                    System.out.println("Error caused by " + n.toString());
                    throw e;
                }
            }

            StringField s = (StringField) field;
            try {
                //Se è un tipo di dato che il programma non ha riconosciuto e nullable allora gli mette proprio null
                //per evitare problemi più tardi
                //perche non è gestibile
                if(s.getType() == Type.NOTFOUND && s.isNullable())
                    return "null";
                else if (s.getType() == Type.NOTFOUND)
                    return "";

                String s1 = "";
                String s2 = "";
                int prob = r.nextInt(100);
                boolean case1 = true;
                int howMany = 0;
                if (s.getLength() > 3){
                    case1 = (prob < alterPerc || (prob >= combinePerc && prob < noMutationPerc));
                    if (case1)
                        howMany = 1;
                    else howMany = 2;
                } else {
                    howMany = 1;
                }

                if (s.getTotalValues() == 0) {
                    if (s.getType() != Type.OTHER && r.nextInt() < 50)
                        s1 = GeneratorStorage.getValue(s);
                    else
                        s1 = getRandomlyGeneratedValue(s);
                    if (!case1) {
                        do {
                            if (s.getType() != Type.OTHER && r.nextInt() < 50)
                                s2 = GeneratorStorage.getValue(s);
                            else
                                s2 = s.getRandomValue();
                        } while (s2.equals(s1));
                    }
                } else if (getRandom) {
                    s.increasePossibleValues();
                    getRandomlyGeneratedValue(s);
                } else {

                    //Prende 1 o 2 valori presenti nella tabella oppure genera con pattern
                    //Potrebbe prendere le stringhe da un pattern
                    //Se genera una delle stringhe diminuirà il valore di howMany
                    //il valore di s2 lo genera solo se siamo nel caso di combinePerc -> prob in [alterPerc, combinePerc) == !case1
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
                        if (!case1){
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
                        //Case: se non c'è pattern oppure se uno dei valori non è stato generato dal pattern(o entrambi)
                        //il valore di howMany indica quanti valori deve ancora generate, dunque se è 0 allora sia s1 che s2 non sono vuote, altrimenti almeno una è vuota
                        /*
                        String query = Generator.getRandomRecordFrom(s.getName(), table, howMany);
                        Statement st = connection.createStatement();
                        ResultSet rs = st.executeQuery(query);
                        rs.next(); //field.getTotalValues != 0 -> rs.next() = true
*/
                        //se s1 non è stato generato dal pattern
                        if (s1.isEmpty()) {
                            //s1 = rs.getString(s.getName());
                            if (s.getType() != Type.OTHER && r.nextInt() < 50)
                                s1 = GeneratorStorage.getValue(s);
                            else
                                s1 = s.getRandomValue();
                            //System.out.println(s1);
                        } else {
                            //Se s1 è stato generato dal pattern ma non s2
                            //s2 = rs.getString(s.getName());
                            do {
                                if (s.getType() != Type.OTHER && r.nextInt() < 50)
                                    s2 = GeneratorStorage.getValue(s);
                                else
                                    s2 = s.getRandomValue();
                            } while(s1.equals(s2));
                        }
                        //Se nessuno dei due era stato generato dal pattern
                        if (s2.isEmpty() /*&& rs.next()*/){ //rs.next() -> howMany == 2
                            s2 = s.getRandomValue();
                            //System.out.println(s2);
                        }
                    }
                }

                if (case1) {
                    if (prob < alterPerc || s.isPrimaryKey())
                        generatedValue = generateAlteredValue(s, s1);
                    else
                        generatedValue = s1;
                } else
                    generatedValue = generateCombinedValue(s1, s2);
                s.addNewValue(generatedValue);

                if (generatedValue.length() > s.getColumnSize()) {
                    System.out.println("Error: ");
                    System.out.println("Generated value: " + generatedValue);
                    System.out.println("Random: " + getRandom);
                    System.out.println("Mutation: " + case1);
                    System.out.println("prob: " + prob);
                    System.out.println("s1: " + s1);
                    System.out.println("s2: " + s2);
                    throw new IllegalStateException("Out of column size value generated");
                }

                return generatedValue;
            } catch(Exception e){
                e.printStackTrace();
                System.out.println("Generated by: " + s.toString());
                throw e;
            }
        }

        generatedValue = ((ForeignField) field).getRandomForeignValue();
        return generatedValue;
    }

    /**
     * Metodo che costruisce una stringa completamente randomica
     * @param field
     * @return
     */
    public String getRandomlyGeneratedValue(StringField field) {
        Random r = new Random();
        int length =0;
        String random = "";
        if (field.getTotalValues() == 0)
            length = r.nextInt(field.getMinLength(), field.getLength());
        else {
            random = field.getRandomValue();
            length = r.nextInt(field.getLength()) + 1;
        }
        String characters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz ";
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
        try {
            if (value.length() == 1) howMany = 1;
            else if (field.getMinLength() == value.length())
                howMany = r.nextInt(0, value.length() / 2) + 1;
            else
                howMany = r.nextInt(field.getMinLength() / 2, (int) Math.ceil((double) value.length() / 2)) + 1;
        } catch(Exception e) {
            e.printStackTrace();

            System.out.println(field.toString());
            System.out.println("in random: " +field.getMinLength() / 2 + "; " + (int) Math.ceil((double) value.length() / 2));
            System.out.println("normal: " + field.getMinLength() + "; " + value.length());

            System.exit(1);
        }
        //System.out.println("altering " + howMany + " characters");
        //System.out.print("altered " + value + " in ");
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
        //System.out.print(value + "\n");
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
        //System.out.println("combining: " + s1 + " ; " + s2);
        //TODO questo if potrebbe rompere i bounds
        if (s1.length() == 1 && s2.length() ==1){
            if (r.nextInt(10) % 2 == 0)
                return s1;
            else
                return s2;
        }
        int random = r.nextInt(100);
        //Excluses combining in parts if string lengths are not enough
        //So that other possibilities still have a even probability
        /*
        if (s1.length() < 3 || s2.length() < 3)
            random = r.nextInt(66);
        else {
            random = r.nextInt(100);
        }*/
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
        if (random < 50) {
            //System.out.println("combined: " + s1 + "; " + s2);
            combined = s1.substring(0, s1Index) + (s2Index == 1 ? s2 : s2.substring(s2Index));
        }else {
            //System.out.println("combined: " + s2 + "; " + s1);
            combined = s2.substring(0, s2Index) + (s1Index == 1 ? s1 : s1.substring(s1Index));
        }
        //System.out.println("combined length: " + combined.length());
        return combined;

/*
        //Gets there only if random >= 66
        String firstPart = "";
        String center = "";
        String lastPart = "";
        int s1TwoThirdIndex = s1.length() - (int) Math.floor(s1.length() / 3);
        int s2TwoThirdIndex = s2.length() - (int) Math.floor(s2.length() / 3);
        if (random % 2 == 0) {
            //System.out.println("combining first and last part of " + s1 + ", center of " + s2);
            //separator1 empty -> separator2 empty

            firstPart = s1.substring(0, (int) Math.floor(s1.length() / 3));
            center = s2.substring((int) Math.floor(s2.length() / 3), s2TwoThirdIndex);
            lastPart = s1.substring(s1TwoThirdIndex);


            System.out.println(firstPart + "; " + center + "; " + lastPart);

        } else {
            //System.out.println("combining first and last part of " + s2 + ", center of " + s1);

            firstPart = s2.substring(0, s2.length() / 3);
            center = s1.substring(s1.length() / 3, s1TwoThirdIndex);
            lastPart = s2.substring(s2TwoThirdIndex);


            System.out.println(firstPart + "; " + center + "; " + lastPart);
        }

        return firstPart + center + lastPart;
*/
    }

    @Deprecated
    private static String findSeparator(String s1, String s2) {
        List<String> separators = new ArrayList<>();
        StringBuilder separator = new StringBuilder();
        separators.add("@");
        separators.add(".");
        separators.add(",");
        separators.add("-");
        separators.add(" ");
        separators.add("+");
        separators.add("/");
        separators.add(":");
        separators.add(";");
        separators.add("\\");


        //I due for ignorano i primi e ultimi caratteri perche se il separatore fosse in quelle posizioni sarebbe inutile
        for(int i = 1; i < s1.length() - 1; ++i){
            if (separators.contains(s1.charAt(i) + "")){
                separator = new StringBuilder(s1.charAt(i) + "");

                boolean found = false;
                for(int j = 1; j < s2.length() - 1; ++j){
                    if (separator.toString().equals(s2.charAt(j) + "")) {
                        found = true;
                        break;
                    }
                }
                if (found) {
                    ++i;
                    int j = s2.indexOf(separator.toString()) + separator.length();
                    while(s1.charAt(i) == s2.charAt(j) && i < s1.length() - 1 && j < s2.length() - 1) {
                        separator.append(s1.charAt(i));
                        ++i;
                    }
                    return separator.toString();

                }
                separator = new StringBuilder();
            }
        }
       // System.out.println("Separator found: " + separator);
        return separator.toString();
    }


    private String generateForeignKey(Field field) {
        ForeignField fk = (ForeignField) field;
        String generatedValue = "";
        //Prende un valore a caso della Fk tra i valori possibili
        String query = "SELECT " + fk.getReferencedPrimaryKey() + " " +
                "FROM " + fk.getReferenceTable() + " " +
                "ORDER BY RAND() LIMIT 1";

        try {
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery(query);
            rs.next();
            //values.add(columns.indexOf(c), "'rock'");
            generatedValue = rs.getObject(fk.getReferencedPrimaryKey()).toString();

            //values.add(columns.indexOf(column), "'" + rs.getString(fk.getReferencedPrimaryKey()) + "'");
            st.close();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }


        return generatedValue;
    }

    /*
    public String generateCombinedField(String name) {
        for(StringFieldGenerator s : stringFieldGenerators) {
            if (s.isThisField(name))
                return s.combine();
        }
        return null;
    }


    public String generateAlteredField(String name) {
        for(StringFieldGenerator s : stringFieldGenerators) {
            if (s.isThisField(name))
                return s.alter();
        }
        return null;
    }

     */

    public static String getRandomRecordFrom(String column, String table, int howMany){
        return "SELECT "+column+ " FROM " + table+ "" +
                " WHERE "+column+" IS NOT NULL " + "AND " +column+"!='' " + "ORDER BY RAND() LIMIT " + howMany;
    }

    public Field getField(String name) {
        for(Field field : fields){
            if (field.getName().equalsIgnoreCase(name)) return field;
        }
        return null;
    }

    public int getOperationBound(String name, char operation) {

        if (!(getField(name) instanceof NumberField field))
            throw new IllegalArgumentException("Method cant be used on a non-numerical Field like " + name);
        else if (operation != '+' && operation != '-' && operation != '*' && operation != '/') {
            throw new IllegalArgumentException("Invalid operation " + operation);
        }

        switch (operation) {
            case '-', '/' -> {
                return (int) field.getLowerBound();
            }
            case '+', '*' ->{
                return (int) field.getUpperBound();
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
    @Deprecated
    public void updateForeignValues() {
        for (Field f : fields) {
            if (f instanceof ForeignField fk) {
                String query = "SELECT " + fk.getReferencedPrimaryKey() + " " +
                        "FROM " + fk.getReferenceTable();
                try {
                    ResultSet rs = connection.createStatement().executeQuery(query);
                    List<String> list = new ArrayList<>();
                    while (rs.next()) {
                        list.add(rs.getString(fk.getReferencedPrimaryKey()));
                    }
                    fk.updateValues(list);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("Failed to update foreign field " + fk.getName() + " in " + table);
                }
            }
        }
    }

    public void addForeignValue(String foreignField, String value) {
        Field field = getField(foreignField);
        if (field != null) {
            ((ForeignField) field).addValue(value);
        }
    }
}

package src.Generator.field;

public enum Type {
    NAME,
    SURNAME,
    COUNTRY,//nazione
    CITY,
    NOTFOUND,
    OTHER;
    //conto i record gia in comune tra db1 e db2
    public static Type getType(String columnName) {
        if ((columnName.contains("name") && columnName.contains("first"))
                || columnName.contains("nome")
                || columnName.contains("name")
                || columnName.contains("username")
                || columnName.contains("userName"))
            return NAME;

        else if ((columnName.contains("name") && columnName.contains("last"))
                || columnName.contains("surname")
                || columnName.contains("cognome"))
            return SURNAME;

        else if (columnName.contains("country")
                || columnName.contains("nation") || columnName.contains("paese"))
            return COUNTRY;

        else if (columnName.contains("city") || columnName.contains("città"))
            return CITY;
        else
            return OTHER;
    }
}

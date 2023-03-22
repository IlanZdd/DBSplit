package src.DBMutation.Generator;

import src.DBMutation.Generator.field.StringField;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

class GeneratorStorage {
    private static List<String> names;
    private static List<String> lastNames;
    private static List<String> countries;
    private static List<String> cities;
    private static boolean initialized = false;


    public static void initialize() {
        try {

            if (!initialized) {

                File file = new File("generator_files/names.txt");
                if (file.exists())
                    names = Files.readAllLines(file.toPath());
                else {
                    names = new ArrayList<>();
                    names.add("Mario");
                }

                file = new File("generator_files/surnames.txt");
                if (file.exists())
                    lastNames = Files.readAllLines(file.toPath());
                else {
                    lastNames = new ArrayList<>();
                    lastNames.add("Rossi");
                }

                file = new File("generator_files/countries.txt");
                if (file.exists())
                    countries = Files.readAllLines(file.toPath());
                else {
                    countries = new ArrayList<>();
                    countries.add("Italy");
                }

                file = new File("generator_files/cities.txt");
                if (file.exists())
                    cities = Files.readAllLines(file.toPath());
                else {
                    cities = new ArrayList<>();
                    cities.add("Milan");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            initialized = true;
        }
    }

    public static String getValue(StringField field) {
        String value = "";
        Random r = new Random();
        do {
            switch (field.getType()) {
                case NAME -> value = names.get(r.nextInt(names.size()));
                case SURNAME -> value = lastNames.get(r.nextInt(lastNames.size()));
                case COUNTRY -> value = countries.get(r.nextInt(countries.size()));
                case CITY -> value = cities.get(r.nextInt(cities.size()));
            }
        } while (value.length() < field.getMinLength() || value.length() > field.getLength());
        return value;
    }

}

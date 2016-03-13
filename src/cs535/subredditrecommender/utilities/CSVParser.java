package cs535.subredditrecommender.utilities;

import java.util.HashMap;


public class CSVParser {
    private HashMap<String, String> csvFields = new HashMap<>();

    public CSVParser(String value, String template) {
        String[] tempcsvFields = value.split(",");

        String[] fieldNames = template.split(",");

        if (tempcsvFields.length == fieldNames.length) {
            for (int i = 0; i < tempcsvFields.length; i++)
                csvFields.put(fieldNames[i], tempcsvFields[i]);
        }
    }

    public String get(String field) {
        if (csvFields.containsKey(field)) {
            return csvFields.get(field);
        } else {
            return "Incorrect";
        }
    }
}

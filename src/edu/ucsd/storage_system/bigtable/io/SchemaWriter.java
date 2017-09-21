package edu.ucsd.storage_system.bigtable.io;

import java.io.*;
import java.util.Map;
import java.util.Set;

/**
 * Created by twincus on 6/6/17.
 */
public class SchemaWriter {
    public static void writeSchema(Map<String, Set<String>> schema, String rowKey, String dbName) {
        try {
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
                    new File("database" + File.separator + dbName + File.separator + "schema")), "UTF-8"));
            bw.write(rowKey + "\n");
            for(String family: schema.keySet()) {
                bw.write(family + '\0');
                for(String col: schema.get(family))
                    bw.write(col + '\0');
                bw.write('\n');
            }
            bw.close();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

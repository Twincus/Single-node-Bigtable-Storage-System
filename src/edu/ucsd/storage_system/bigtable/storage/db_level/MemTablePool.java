package edu.ucsd.storage_system.bigtable.storage.db_level;

import edu.ucsd.storage_system.bigtable.storage.sst_level.Index;
import edu.ucsd.storage_system.bigtable.storage.sst_level.MemTable;
import edu.ucsd.storage_system.bigtable.storage.sst_level.MyBloomFilter;
import edu.ucsd.storage_system.bigtable.storage.sst_level.ReadCache;
import edu.ucsd.storage_system.bigtable.utils.Log;

import java.io.*;
import java.util.*;

/**
 * Created by twincus on 6/5/17.
 */
public class MemTablePool {
    private Map<String, MemTable> memTablePool; //key is the name of database

    public MemTablePool(List<String> dbNames, IndexPool indexPool, BloomFilterPool bloomFilterPool) {
        memTablePool = new HashMap<>();
        for(String db: dbNames) {
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(
                        new File("database" + File.separator + db + File.separator + "schema")), "UTF-8"));
                String line = null;

                int i = 1;
                String rowKeyName = null;
                Map<String, Set<String>> schema = new HashMap<>();
                while((line = br.readLine()) != null) {
                    if(i == 1)
                        rowKeyName = line;
                    else {
                        String[] subs = line.split("\0");
                        String family = subs[0];
                        Set<String> cols = new HashSet<>();
                        for(int j = 1; j < subs.length; j++)
                            cols.add(subs[j]);
                        schema.put(family, cols);
                    }
                    i++;
                }
                br.close();
                memTablePool.put(db, new MemTable(schema, rowKeyName, db, indexPool, bloomFilterPool.getBloomFilter(db)));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void addMemTable(String db, Map<String, Set<String>> schema, String rowKey, IndexPool indexPool, MyBloomFilter myBF) {
        memTablePool.put(db, new MemTable(schema, rowKey, db, indexPool, myBF));
    }

    public MemTable getMemTable(String dbName) {
        return memTablePool.get(dbName);
    }

}

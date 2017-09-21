package edu.ucsd.storage_system.bigtable.main;

import com.sun.org.apache.xml.internal.security.Init;
import edu.ucsd.storage_system.bigtable.interaction.DBHandler;
import edu.ucsd.storage_system.bigtable.storage.db_level.BloomFilterPool;
import edu.ucsd.storage_system.bigtable.storage.db_level.IndexPool;
import edu.ucsd.storage_system.bigtable.storage.db_level.MemTablePool;
import edu.ucsd.storage_system.bigtable.storage.db_level.ReadCachePool;
import edu.ucsd.storage_system.bigtable.storage.sst_level.Index;
import edu.ucsd.storage_system.bigtable.storage.sst_level.ReadCache;
import edu.ucsd.storage_system.bigtable.storage.sst_level.MemTable;

import java.io.*;
import java.util.*;

/**
 * Created by twincus on 5/30/17.
 */
public class Initiator {
    public static Map<String, Integer> sstCount;
    public IndexPool indexPool;
    public MemTablePool memTablePool;
    public ReadCachePool readCachePool;
    public BloomFilterPool bloomFilterPool;

    public Initiator() {
        File dbDir = new File("database");
        File[] dbs = dbDir.listFiles();
        List<String> dbNames = new ArrayList<>();
        sstCount = new HashMap<>();

        for(File f: dbs) {
            if(!f.getName().equals(".DS_Store")) {
                dbNames.add(f.getName());
                File indexDir = new File("database" + File.separator + f.getName() + File.separator + "index");
                sstCount.put(f.getName(), indexDir.listFiles().length / 2);
            }
        }

        this.indexPool = new IndexPool(dbNames);
        this.readCachePool = new ReadCachePool(dbNames);
        this.bloomFilterPool = new BloomFilterPool(dbNames);
        this.memTablePool = new MemTablePool(dbNames, this.indexPool, this.bloomFilterPool);
    }
}

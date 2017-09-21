package edu.ucsd.storage_system.bigtable.storage.db_level;

import edu.ucsd.storage_system.bigtable.storage.sst_level.Index;
import edu.ucsd.storage_system.bigtable.storage.sst_level.IndexCache;
import edu.ucsd.storage_system.bigtable.utils.Log;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by twincus on 6/7/17.
 */

public class IndexPool {
    private Map<String, IndexCache> indexPool; // key is database name, value:  each element is for one SSTable, those whose index is bigger is newer

    private final String COL_INDEX_FILE = "col_index";
    private final String ROW_INDEX_FILE = "row_index";
    private final String ROOT = "database";

    //used to load column index from disk when database is started
    public IndexPool(List<String> dbNames){
        indexPool = new HashMap<>();
        for(String db: dbNames) {
            indexPool.put(db, new IndexCache());
        }
    }

    public boolean containsIndex(String db) {
        return  indexPool.containsKey(db);
    }

    public IndexCache getIndexCache(String dbName) {
        return indexPool.get(dbName);
    }

    public void createIndexCache(String dbName) {
        this.indexPool.put(dbName, new IndexCache());
    }
}

package edu.ucsd.storage_system.bigtable.storage.db_level;

import edu.ucsd.storage_system.bigtable.storage.sst_level.ReadCache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by twincus on 6/13/17.
 */
public class ReadCachePool {
    private Map<String, ReadCache> pool; //key: database

    public ReadCachePool(List<String> dbNames) {
        pool = new HashMap<>();
        for(String db: dbNames)
            pool.put(db, new ReadCache());
    }

    public void addReadCache(String db) {
        if(!pool.containsKey(db))
            pool.put(db, new ReadCache());
    }

    public ReadCache getReadCache(String dbName) {
        return pool.get(dbName);
    }

    public void createReadCache(String dbName) {
        this.pool.put(dbName, new ReadCache());
    }
}

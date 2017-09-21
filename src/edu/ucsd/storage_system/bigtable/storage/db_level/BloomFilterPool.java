package edu.ucsd.storage_system.bigtable.storage.db_level;

import edu.ucsd.storage_system.bigtable.storage.sst_level.MyBloomFilter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by twincus on 6/14/17.
 */
public class BloomFilterPool {
    private Map<String, MyBloomFilter> bloomFilterPool;

    public BloomFilterPool(List<String> dbNames) {
        bloomFilterPool = new HashMap<>();
        for(String db: dbNames)
            bloomFilterPool.put(db, new MyBloomFilter(db));
    }

    public MyBloomFilter getBloomFilter(String dbName) {
        return this.bloomFilterPool.get(dbName);
    }

    public void createBloomFilter(String dbName) {
        bloomFilterPool.put(dbName, new MyBloomFilter(dbName));
    }
}

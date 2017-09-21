package edu.ucsd.storage_system.bigtable.interaction;

import com.sun.org.apache.regexp.internal.RE;
import edu.ucsd.storage_system.bigtable.io.SSTableReader;
import edu.ucsd.storage_system.bigtable.io.SchemaWriter;
import edu.ucsd.storage_system.bigtable.main.Initiator;
import edu.ucsd.storage_system.bigtable.storage.db_level.BloomFilterPool;
import edu.ucsd.storage_system.bigtable.storage.db_level.IndexPool;
import edu.ucsd.storage_system.bigtable.storage.db_level.MemTablePool;
import edu.ucsd.storage_system.bigtable.storage.db_level.ReadCachePool;
import edu.ucsd.storage_system.bigtable.storage.sst_level.*;
import edu.ucsd.storage_system.bigtable.utils.Log;
import edu.ucsd.storage_system.bigtable.utils.ReturnValue;

import java.io.File;
import java.io.IOException;
import java.lang.management.MemoryPoolMXBean;
import java.util.*;

/**
 * Created by twincus on 5/31/17.
 */

/**
 * one DBHandler is for one database
 */
public class DBHandler{
    private String threadName;
    private Thread t;
    private String dbName;
    private MemTable memTable;
    private Map<String, Set<String>> schema;
    private String rowKey;
    private List<String> cols;
    private List<String> values;
    private String col;
    private String value;
    private int operation;
    private MyBloomFilter myBF;
    private ReadCache rdCache;
    private String family;
    private IndexCache indexCache;
    private MemTablePool memTablePool;
    private IndexPool indexPool;
    private BloomFilterPool bloomFilterPool;
    private ReadCachePool readCachePool;
    private ReturnValue ret;

    //1
    public DBHandler(String threadName, MemTablePool memTablePool, IndexPool indexPool, String dbName, Map<String, Set<String>> schema, String rowKey, int op, BloomFilterPool bloomFilterPool, ReadCachePool readCachePool) {
        this.memTablePool = memTablePool;
        this.threadName = threadName;
        this.dbName = dbName;
        this.memTable = memTablePool.getMemTable(dbName);
        this.schema = schema;
        this.rowKey = rowKey;
        this.operation = op;
        this.indexPool = indexPool;
        this.myBF = bloomFilterPool.getBloomFilter(dbName);
        this.bloomFilterPool = bloomFilterPool;
        this.readCachePool = readCachePool;
    }

    //2
    public DBHandler(String threadName, MemTablePool memTablePool, IndexPool indexPool, String dbName, String rowKey, List<String> cols, List<String> values, int op, BloomFilterPool bloomFilterPool) {
        this.threadName = threadName;
        this.dbName = dbName;
        this.memTable = memTablePool.getMemTable(dbName);
        this.rowKey = rowKey;
        this.cols = cols;
        this.values = values;
        this.operation = op;
        this.indexPool = indexPool;
        this.myBF = bloomFilterPool.getBloomFilter(dbName);
        this.bloomFilterPool = bloomFilterPool;
    }

//    //3
//    public DBHandler(String threadName, String dbName, String rowKey, String col, String value, int op, MemTablePool memTablePool, IndexPool indexPool) {
//        this.threadName = threadName;
//        this.dbName = dbName;
//        this.memTable = memTablePool.getMemTable(dbName);
//        this.col = col;
//        this.value = value;
//        this.operation = op;
//        this.indexPool = indexPool;
//        this.rowKey = rowKey;
//    }

    //4
    public DBHandler(String threadName, String family, String dbName, String col, String rowKey, int op, MemTablePool memTablePool, IndexPool indexPool, BloomFilterPool bloomFilterPool, ReadCache rdCache, IndexCache indexCache) {
        this.family = family;
        this.threadName = threadName;
        this.dbName = dbName;
        this.memTable = memTablePool.getMemTable(dbName);
        this.col = col;
        this.rowKey = rowKey;
        this.operation = op;
        this.myBF = bloomFilterPool.getBloomFilter(dbName);
        this.rdCache = rdCache;
        this.indexCache = indexCache;
        this.indexPool = indexPool;
        this.bloomFilterPool = bloomFilterPool;
    }

    //5
    public DBHandler(String threadName, String family, String dbName, String col, int op, MemTablePool memTablePool, IndexPool indexPool, BloomFilterPool bloomFilterPool, ReadCache rdCache, IndexCache indexCache) {
        this.family = family;
        this.threadName = threadName;
        this.dbName = dbName;
        this.memTable = memTablePool.getMemTable(dbName);
        this.myBF = bloomFilterPool.getBloomFilter(dbName);
        this.col = col;
        this.operation = op;
        this.rdCache = rdCache;
        this.indexCache = indexCache;
        this.indexPool = indexPool;
        this.bloomFilterPool = bloomFilterPool;
    }

    //6
    public DBHandler(String threadName,  String dbName, String row, int op, MemTablePool memTablePool, IndexPool indexPool, BloomFilterPool bloomFilterPool, ReadCache rdCache, IndexCache indexCache) {
        this.threadName = threadName;
        this.dbName = dbName;
        this.memTable = memTablePool.getMemTable(dbName);
        this.myBF = bloomFilterPool.getBloomFilter(dbName);
        this.myBF = bloomFilterPool.getBloomFilter(dbName);
        this.rowKey = row;
        this.operation = op;
        this.rdCache = rdCache;
        this.indexCache = indexCache;
        this.indexPool = indexPool;
        this.schema = this.memTable.getSchema();
    }

    //7
    public DBHandler(String threadName, String dbName, String col, String rowKey, int op, MemTablePool memTablePool, IndexPool indexPool) {
        this.threadName = threadName;
        this.dbName = dbName;
        this.memTable = memTablePool.getMemTable(dbName);
        this.col = col;
        this.rowKey = rowKey;
        this.operation = op;
        this.indexPool = indexPool;
    }

    //8
    public DBHandler(String threadName, MemTablePool memTablePool, IndexPool indexPool, String dbName, String rowKey, int op) {
        this.threadName = threadName;
        this.dbName = dbName;
        this.memTable = memTable;
        this.rowKey = rowKey;
        this.operation = op;
        this.indexPool = indexPool;
    }

    public void run() {
        switch(this.operation) {
            case 1: {
                if(!indexPool.containsIndex(dbName)) {
                    File file = new File("database" + File.separator + dbName);
                    file.mkdir();
                    file = new File("database" + File.separator + dbName + File.separator + "index");
                    file.mkdir();
                    file = new File("database" + File.separator + dbName + File.separator + "bloom_filter");
                    file.mkdir();
                    try {
                        file = new File("database" + File.separator + dbName + File.separator + "bloom_filter" + File.separator + "col_BF");
                        file.createNewFile();
                        file = new File("database" + File.separator + dbName + File.separator + "bloom_filter" + File.separator + "row_BF");
                        file.createNewFile();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    this.bloomFilterPool.createBloomFilter(dbName);
                    this.myBF = this.bloomFilterPool.getBloomFilter(dbName);
                    SchemaWriter.writeSchema(schema, rowKey, dbName);
                    Log.print("Have dumped schema onto disk...");
                    this.indexPool.createIndexCache(dbName);
                    this.readCachePool.createReadCache(dbName);
                    Initiator.sstCount.put(dbName, 0);
                    memTablePool.addMemTable(dbName, schema, rowKey, indexPool, myBF);
                    Log.printThread("Thread '" + threadName + "' completed creating new database...");
                } else {
                    Log.printThread("Thread '" + threadName + "' failed to create new database since it already exists...");
                }
                break;
            }
            case 2: {
                if(memTable.insert(rowKey, cols, values))
                    Log.printThread("Thread '" + threadName + "' completed insert...");
                break;
            }
//            case 3: {
//                if(memTable.update(rowKey, col, value))
//                    Log.printThread("Thread '" + threadName + "' completed update...");
//                break;
//            }
            case 4: {
                String retVal = memTable.getOneCell(col, rowKey);

                if(retVal.equals("\0")) {
                    Log.print("miss hit on memTable...");

                    Set<Integer> colRes = myBF.mightContainCol(col);
                    Set<Integer> rowRes = myBF.mightContainRow(rowKey);
                    List<Integer> res = new ArrayList<>();
                    for(int n: colRes) {
                        if(rowRes.contains(n))
                            res.add(n);
                    }
                    int id = 0;
                    if(rdCache.containsOneCell(family, col, rowKey)) {
                        Log.print("Desired cell hit the read cache...");
                        String[] temp = rdCache.getOneCell(family, col ,rowKey);
                        retVal = temp[0];
                        id = Integer.parseInt(temp[1]);
                    }
                    else {
                        Log.print("Desired cell missed the read cache...");
                    }
                    Collections.sort(res, Comparator.reverseOrder());

                    for(int sstId: res) {
                        if(sstId > id) {
                            Index ind = null;
                            if(indexCache.containsIndex(sstId)) {
                                ind = indexCache.getIndex(sstId);
                            } else {
                                ind = new Index(sstId, dbName);
                            }
                            if(!ind.containOneCell(rowKey, col)) //this SSTable doesn't contain the desired value
                                continue;
                            List<Block> blockList = new ArrayList<>();
                            retVal = SSTableReader.getOneCell(dbName, family, col, rowKey, ind, blockList);
                            rdCache.insertCache(blockList, family, sstId);
                            break;
                        }
                    }
                } else {
                    Log.print("hit on memTable...");
                }
                Log.printThread("Thread '" + threadName + "' completed GetOneCell... RetVal = " + retVal);
                this.ret = new ReturnValue(null, retVal);
                break;
            }
            case 5: {
                //skip read cache for read on a column
                Map<String, String> retVal = memTable.getOneColumn(col);
                if(retVal == null)
                    retVal = new HashMap<>();

                Set<Integer> colRes = myBF.mightContainCol(col);
                List<Integer> resList = new ArrayList<>(colRes);
                Collections.sort(resList, Comparator.reverseOrder());
                for(int sstId: resList) {
                    Index ind = null;
                    if(indexCache.containsIndex(sstId)) {
                        ind = indexCache.getIndex(sstId);
                    } else {
                        ind = new Index(sstId, dbName);
                    }
                    if(!ind.containOneColumn(family, col)) //this SSTable doesn't contain the desired value
                        continue;
                    List<Block> blockList = new ArrayList<>();
                    Map<String, String> temp = SSTableReader.getOneColumn(dbName, family, col, ind, blockList);
                    rdCache.insertCache(blockList, family, sstId);

                    for(String row: temp.keySet()) {
                        if(!retVal.containsKey(row))
                            retVal.put(row, temp.get(row));
                    }
                }

                Log.printThread("Thread '" + threadName + "' completed GetOneColumn... Return values are as follows:");
                for(String row: retVal.keySet()) {
                    Log.printThread(col + " : " + row + " : " + retVal.get(row));
                }
                this.ret = new ReturnValue(retVal, null);

                break;
            }
            case 6: {
                Map<String, String> retVal = memTable.getOneRow(rowKey);
                if(retVal == null)
                    retVal = new HashMap<>();

                Map<String, String> allCols = getAllColumns();

                Set<Integer> rowRes = myBF.mightContainRow(rowKey);
                List<Integer> resList = new ArrayList<>(rowRes);
                Collections.sort(resList, Comparator.reverseOrder());

                for(int sstId: resList) {
                    Index ind = null;
                    if(indexCache.containsIndex(sstId)) {
                        ind = indexCache.getIndex(sstId);
                    } else {
                        ind = new Index(sstId, dbName);
                    }
                    for(String col: allCols.keySet()) {
                        if(ind.containOneCell(rowKey, col)) {
                            List<Block> blockList = new ArrayList<>();
                            String val = SSTableReader.getOneCell(dbName, allCols.get(col), col, rowKey, ind, blockList);
                            rdCache.insertCache(blockList, allCols.get(col), sstId);

                            if(!retVal.containsKey(col))
                                retVal.put(col, val);
                        }
                    }
                }

                Log.printThread("Thread '" + threadName + "' completed GetOneRow... Return values are as follows:");
                for(String col: retVal.keySet()) {
                    Log.printThread(col + " : " + rowKey + " : " + retVal.get(col));
                }
                this.ret = new ReturnValue(retVal, null);
                break;
            }
            case 7: {
                if(memTable.deleteOneCell(rowKey, col))
                    Log.printThread("Thread '" + threadName + "' completed DeleteOneCell...");
                break;
            }
            case 8: {
                if(this.memTable.deleteOneRow(rowKey))
                    Log.printThread("Thread '" + threadName + "' completed DeleteOneRow...");
                break;
            }
        }
    }

    public ReturnValue start() {
//        if(t == null) {
//            Thread t = new Thread(this, threadName);
//            t.start();
//        }
        run();
        return ret;
    }

    private Map<String, String> getAllColumns() {
        Map<String, String> res = new HashMap<>();
        for(String family: schema.keySet()) {
            for(String col: schema.get(family)) {
                res.put(col, family);
            }
        }
        return res;
    }
}



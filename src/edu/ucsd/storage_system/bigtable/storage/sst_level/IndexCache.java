package edu.ucsd.storage_system.bigtable.storage.sst_level;

import edu.ucsd.storage_system.bigtable.io.SchemaWriter;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Created by twincus on 6/13/17.
 */
public class IndexCache {
    private Map<Integer, Index> indexCache; //key: SSTable id
    private LinkedList<Integer> fifo;
    private static int SIZE;

    static{
        try {
            InputStream input = new FileInputStream("config.properties");
            Properties pro = new Properties();
            pro.load(input);
            SIZE = Integer.parseInt(pro.getProperty("INDEX_CACHE_SIZE"));
            input.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public IndexCache() {
        this.indexCache = new HashMap<>();
        this.fifo = new LinkedList<>();
    }

    public boolean containsIndex(int sstNum) {
        return indexCache.containsKey(sstNum);
    }

    public Index getIndex(int sstNum) {
        Index ind = indexCache.get(sstNum);
        fifo.remove(new Integer(sstNum));
        fifo.addLast(sstNum);
        return ind;
    }

    public void insertIndex(int sstNum, Index ind) {
        fifo.addLast(sstNum);
        indexCache.put(sstNum, ind);
        if(fifo.size() > SIZE)
            fifo.removeFirst();
    }

}

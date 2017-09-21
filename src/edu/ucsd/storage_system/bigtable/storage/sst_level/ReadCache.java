package edu.ucsd.storage_system.bigtable.storage.sst_level;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Created by twincus on 5/31/17.
 */

/**
 * one ReadCache is for one database
 */
public class ReadCache {
    private static int SIZE = 4;
    private LinkedList<Block> cache;
    private Map<String, Map<String, Map<String, String[]>>> map; //family: column: rowkey: {value, sstNumber}
    private Map<Integer, Block> hashCodeMap;

    static {
        try {
            InputStream input = new FileInputStream("config.properties");
            Properties pro = new Properties();
            pro.load(input);
            SIZE = Integer.parseInt(pro.getProperty("READ_CACHE_SIZE"));
            input.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public ReadCache() {
        cache = new LinkedList<>();
        map = new HashMap<>();
        hashCodeMap = new HashMap<>();
    }


    public synchronized void insertCache(List<Block> blockList, String familyName, int sstNumb) {
        for(Block bl: blockList) {
            Map<String, Map<String, String>> cols = bl.cols;

            if (!map.containsKey(familyName))
                map.put(familyName, new HashMap<>());
            Map<String, Map<String, String[]>> temp1 = map.get(familyName);
            for (String col : cols.keySet()) {
                Map<String, String> temp2 = cols.get(col);
                for (String row : temp2.keySet()) {
                    if (!temp1.containsKey(col))
                        temp1.put(col, new HashMap<>());
                    if (!temp1.get(col).containsKey(row) || Integer.parseInt(temp1.get(col).get(row)[1]) < sstNumb) {   //override old value with new value
                        temp1.get(col).put(row, new String[]{temp2.get(row), String.valueOf(sstNumb)});
                        hashCodeMap.put(familyName.hashCode() + col.hashCode() + row.hashCode() + temp2.get(row).hashCode() + sstNumb
                                , bl);
                    }
                }
            }

            cache.addLast(bl);
            if (cache.size() > SIZE) {   //need to remove the head
                bl = cache.pollFirst();
                removeMapping(bl);
            }
        }
    }



    public void removeMapping(Block bl) {
        Map<String, Map<String, String[]>> cols = map.get(bl.familyName);
        for(String col: cols.keySet()) {
            Map<String, String[]> rows = cols.get(col);
            for(String row: rows.keySet()) {
                if(Integer.parseInt(rows.get(row)[1]) == Integer.parseInt(map.get(bl.familyName).get(col).get(row)[1])) //the stored value in the mapping is not the one in this block
                    map.get(bl.familyName).get(col).remove(row);
            }
        }
    }

    public boolean containsOneCell(String familyName, String col, String rowKey) {
        if(!map.containsKey(familyName))    return false;
        Map<String, Map<String, String[]>> cols = map.get(familyName);

        if(!cols.containsKey(col)
            || !cols.get(col).containsKey(rowKey))
            return false;
        return true;
    }

    /**
     * return the value stored in cache. string[0]: value; string[1]: #sstable that the value comes from
     * @param familyName
     * @param col
     * @param rowKey
     * @return
     */
    public synchronized String[] getOneCell(String familyName, String col, String rowKey) {
        String[] res = map.get(familyName).get(col).get(rowKey);
        Block bl = hashCodeMap.get(familyName.hashCode() + col.hashCode() + rowKey.hashCode() + Integer.parseInt(res[1]));
        cache.remove(bl);
        cache.addLast(bl);
        return res;
    }

}

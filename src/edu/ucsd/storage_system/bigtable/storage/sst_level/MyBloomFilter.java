package edu.ucsd.storage_system.bigtable.storage.sst_level;

/**
 * Created by twincus on 6/7/17.
 */

import edu.ucsd.storage_system.bigtable.utils.BloomFilter;

import java.io.*;
import java.util.*;

/**
 * One MyBloomFilter is for one database, there are multiple bloomfilters for multiple SSTables.
 */
public class MyBloomFilter {
    private Map<Integer, BloomFilter<String>> colBFMap;
    private Map<Integer, BloomFilter<String>> rowBFMap;
    private static int SST_MAX_COUNT;
    private static double FALSE_POS_PROB;
    private static final String ROOT = "database";
    private static final String COL_BF = "col_BF";
    private static final String ROW_BF = "row_BF";
    private static final String BLOOM_FILTER_DIR = "bloom_filter";

    static{
        try {
            InputStream input = new FileInputStream("config.properties");
            Properties pro = new Properties();
            pro.load(input);
            SST_MAX_COUNT = Integer.parseInt(pro.getProperty("SST_MAX_COUNT"));
            FALSE_POS_PROB = Double.parseDouble(pro.getProperty("FALSE_POSITIVE_PROB"));
            input.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public MyBloomFilter(String dbName) {
        colBFMap = new HashMap<>();
        rowBFMap = new HashMap<>();

        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    new FileInputStream(ROOT + File.separator + dbName + File.separator + BLOOM_FILTER_DIR + File.separator + COL_BF), "UTF-8"));
            String line = null;

            int i = 1;
            while((line = br.readLine()) != null) {
                BloomFilter<String> colBF = new BloomFilter<>(FALSE_POS_PROB, SST_MAX_COUNT);
                for(String sub: line.split("\0"))
                    colBF.add(sub);
                colBFMap.put(i, colBF);
                i++;
            }
            br.close();

            br = new BufferedReader(new InputStreamReader(
                    new FileInputStream(ROOT + File.separator + dbName + File.separator + BLOOM_FILTER_DIR + File.separator + ROW_BF), "UTF-8"));

            i = 1;
            while((line = br.readLine()) != null) {
                BloomFilter<String> rowBF = new BloomFilter<>(FALSE_POS_PROB, SST_MAX_COUNT);
                for(String sub: line.split("\0"))
                    rowBF.add(sub);
                rowBFMap.put(i, rowBF);
                i++;
            }
            br.close();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void addCol(int sstNum, String col) {
        if(!colBFMap.containsKey(sstNum))
            colBFMap.put(sstNum, new BloomFilter<String>(FALSE_POS_PROB, SST_MAX_COUNT));
        colBFMap.get(sstNum).add(col);
    }

    public void addRow(int sstNum, String row) {
        if(!rowBFMap.containsKey(sstNum))
            rowBFMap.put(sstNum, new BloomFilter<String>(FALSE_POS_PROB, SST_MAX_COUNT));
        rowBFMap.get(sstNum).add(row);
    }

    public Set<Integer> mightContainCol(String col) {
        Set<Integer> res = new HashSet<>();
        for(int sstNum: colBFMap.keySet()) {
            if(colBFMap.get(sstNum).contains(col))
                res.add(sstNum);
        }
        return res;
    }

    public Set<Integer> mightContainRow(String row) {
        Set<Integer> res = new HashSet<>();
        for(int sstNum: rowBFMap.keySet()) {
            if(rowBFMap.get(sstNum).contains(row))
                res.add(sstNum);
        }
        return res;
    }
}

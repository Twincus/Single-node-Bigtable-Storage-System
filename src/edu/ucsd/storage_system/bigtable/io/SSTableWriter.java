package edu.ucsd.storage_system.bigtable.io;

import edu.ucsd.storage_system.bigtable.main.Initiator;
import edu.ucsd.storage_system.bigtable.storage.db_level.IndexPool;
import edu.ucsd.storage_system.bigtable.storage.sst_level.MemTable;
import edu.ucsd.storage_system.bigtable.storage.sst_level.MyBloomFilter;
import edu.ucsd.storage_system.bigtable.utils.Log;

import java.io.*;
import java.util.*;

/**
 * Created by twincus on 6/4/17.
 */
public class SSTableWriter {
    private List<Map<String, Map<String, String>>> buffer;
    private Map<String, Integer> colFamilyHash;
    private String dbName;
    private String dir;
    private IndexPool indexPoolForDB;
    private MyBloomFilter bf;
    private static final String COL_BF = "col_BF";
    private static final String ROW_BF = "row_BF";
    private static final String BLOOM_FILTER_DIR = "bloom_filter";

    private static final String COLINDEX_FILENAME = "col";
    private static final String ROWINDEX_FILENAME = "row";
    private static final String SSTABLE_FILENAME = "sst.db";
    private static final String INDEX_DIR = "index";


    private static int BLOCK_SIZE;

    public SSTableWriter(String dbName, MemTable memTable, List<Map<String, Map<String, String>>> buffer,
                         Map<String, Integer> colFamilyHash, IndexPool indexPoolForDB, MyBloomFilter bf) {
        this.dbName = dbName;
        this.dir = "database/" + dbName;
        this.buffer = buffer;
        this.colFamilyHash = colFamilyHash;
        this.indexPoolForDB = indexPoolForDB;
        this.bf = bf;

        try {
            InputStream input = new FileInputStream("config.properties");
            Properties pro = new Properties();
            pro.load(input);
            BLOCK_SIZE = Integer.parseInt(pro.getProperty("BLOCK_SIZE"));
            input.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeToSSTable() throws IOException {
        File file = new File(this.dir);
        if(!file.exists() || file.isFile()) {
            if(file.mkdir()) {
                Log.print("Succeeded to create directory for database '" + dbName + "'...");
            } else {
                Log.print("Failed to create directory for database '" + dbName + "'...");
                return;
            }
        }

        File sstFile = new File(dir + File.separator + SSTABLE_FILENAME);
        FileOutputStream sstFos = new FileOutputStream(sstFile, true);
        FileOutputStream colBFFos = new FileOutputStream(dir + File.separator + BLOOM_FILTER_DIR + File.separator + COL_BF, true);
        FileOutputStream rowBFFos = new FileOutputStream(dir + File.separator + BLOOM_FILTER_DIR + File.separator + ROW_BF, true);

//        FileOutputStream colIndexFos = new FileOutputStream(dir + File.separator + COLINDEX_FILENAME, true);
//        FileOutputStream rowIndexFos = new FileOutputStream(dir + File.separator + ROWINDEX_FILENAME, true);

        long offset = sstFile.length();

        for(String familyName: colFamilyHash.keySet()) {
            Map<String, Map<String, String>> family = buffer.get(colFamilyHash.get(familyName));
            List<Map.Entry<String, Map<String, String>>> familyList = new ArrayList<>(family.entrySet());
            Collections.sort(familyList, new Comparator<Map.Entry<String, Map<String, String>>>() {
                public int compare(Map.Entry<String, Map<String, String>> en1, Map.Entry<String, Map<String, String>> en2) {
                    return en1.getKey().compareTo(en2.getKey());
                }
            });


            FileOutputStream colIndexFos = new FileOutputStream(dir + File.separator + INDEX_DIR + File.separator + COLINDEX_FILENAME + String.valueOf(Initiator.sstCount.get(dbName)+ 1));
            FileOutputStream rowIndexFos = new FileOutputStream(dir + File.separator + INDEX_DIR + File.separator + ROWINDEX_FILENAME + String.valueOf(Initiator.sstCount.get(dbName) + 1));

            colIndexFos.write(familyName.getBytes());
            colIndexFos.write('\n');

            Set<String> colBFSet = new HashSet<>();
            Set<String> rowBFSet = new HashSet<>();

            Map<String, long[]> colIndexTemp = new HashMap<>();
            Map<String, Long> rowIndex = new HashMap<>();

            for(Map.Entry<String, Map<String, String>> en: familyList) {    //whole loop is for one SSTable
                int currSize = 0;

                String col = en.getKey();
                bf.addCol(Initiator.sstCount.get(dbName) + 1, col);

                if(!colBFSet.contains(col)) {
                    colBFSet.add(col);
                    colBFFos.write(col.getBytes());
                    colBFFos.write(0);
                }

                long colIndexOffStart = offset;
                Map<String, String> rows = en.getValue();
                List<Map.Entry<String, String>> rowList = new ArrayList<>(rows.entrySet());
                Collections.sort(rowList, new Comparator<Map.Entry<String, String>>() {
                    public int compare(Map.Entry<String, String> en1, Map.Entry<String, String> en2) {
                        return en1.getKey().compareTo(en2.getKey());
                    }
                });


                for(Map.Entry<String, String> en2: rowList) {
                    String rowKey = en2.getKey();
                    bf.addRow(Initiator.sstCount.get(dbName) + 1, rowKey);

                    if(!rowBFSet.contains(rowKey)) {
                        rowBFSet.add(rowKey);
                        rowBFFos.write(rowKey.getBytes());
                        rowBFFos.write(0);
                    }

                    rowIndex.put(rowKey + "." + col, offset);
                    byte[] rowKeyBytes = rowKey.getBytes();
                    byte[] valueBytes= en2.getValue().getBytes();

                    int plusSize = rowKeyBytes.length + valueBytes.length + 2;
                    offset += plusSize;
                    if(plusSize + currSize > BLOCK_SIZE) {  //need to fill in the gap with '\0'
                        sstFos.write(new byte[BLOCK_SIZE - currSize]);
                        offset += BLOCK_SIZE - currSize;
                        currSize = plusSize;
                    }
                    else
                        currSize += plusSize;
                    sstFos.write(rowKeyBytes);
                    sstFos.write(0);
                    sstFos.write(valueBytes);
                    sstFos.write(0);
                }
                sstFos.write(new byte[BLOCK_SIZE - currSize]);
                offset += BLOCK_SIZE - currSize;

//                colIndexFos.write(familyName.getBytes());
//                colIndexFos.write(0);
                colIndexFos.write(col.getBytes());
                colIndexFos.write(0);
                colIndexFos.write(String.valueOf(colIndexOffStart).getBytes()); //starting offset of a column
                colIndexFos.write(0);
                colIndexFos.write(String.valueOf(offset-1).getBytes());    //end offset of a column
                colIndexFos.write(0);

                colIndexTemp.put(familyName + "\0" + col, new long[]{colIndexOffStart, offset-1});
            }
            colIndexFos.close();

            colBFFos.write('\n');
            rowBFFos.write('\n');

            List<Map.Entry<String, Long>> rowIndexList = new ArrayList<>(rowIndex.entrySet());
            Collections.sort(rowIndexList, new Comparator<Map.Entry<String, Long>>() {
                public int compare(Map.Entry<String, Long> en1, Map.Entry<String, Long> en2) {
                    return en1.getKey().compareTo(en2.getKey());
                }
            });

            for(Map.Entry<String, Long> en: rowIndexList) {
                rowIndexFos.write(en.getKey().getBytes());
                rowIndexFos.write(0);
                rowIndexFos.write(String.valueOf(en.getValue()).getBytes());
                rowIndexFos.write(0);
            }
            rowIndexFos.close();
            Initiator.sstCount.put(dbName, Initiator.sstCount.get(dbName) + 1);
//            this.indexPoolForDB.addIndex(dbName, colIndexTemp, rowIndex, ++Initiator.sstCount); //update index to memory
        }

        sstFos.close();
    }

}

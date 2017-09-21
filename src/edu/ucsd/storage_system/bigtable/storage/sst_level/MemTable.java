package edu.ucsd.storage_system.bigtable.storage.sst_level;

import edu.ucsd.storage_system.bigtable.io.SSTableWriter;
import edu.ucsd.storage_system.bigtable.storage.db_level.IndexPool;
import edu.ucsd.storage_system.bigtable.utils.Log;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Created by twincus on 5/30/17.
 */

/**
 * One MemTable is for one SSTqble of one database.
 */
public class MemTable {
    private List<Map<String, Map<String, String>>> buffer;  //each element is a column family.
    private int threshold;
    private Map<String, Integer> colFamilyHash; //String is for column family, Integer is for the index of the column family in <code>buffer</code>.
    private Map<String, String> col2ColFamily;  //mapping of column name to column family.
    private Map<String, Map<String, String>> rowOrienBuffer;
    private Map<String, List> colOriDBs;
    private String dbName;
    private IndexPool indexPool;
    private MyBloomFilter myBF;
    private Map<String, Set<String>> schema;
    private int currSize;

    public MemTable(Map<String, Set<String>> schema, String rowKey, String dbName, IndexPool indexPool, MyBloomFilter myBF) {
        this.buffer = new ArrayList<>();
        this.colFamilyHash = new HashMap<>();
        this.rowOrienBuffer = new HashMap<>();
        this.col2ColFamily = new HashMap<>();
        this.colOriDBs = new HashMap<>();
        this.colOriDBs.put(dbName, buffer);
        this.dbName = dbName;
        this.indexPool = indexPool;
        this.myBF = myBF;
        this.schema = schema;

        try {
            InputStream input = new FileInputStream("config.properties");
            Properties pro = new Properties();
            pro.load(input);
            threshold = Integer.parseInt(pro.getProperty("MEMTABLE_THRESHOLD"));
            input.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        for(String family: schema.keySet()) {
//            this.colFamilyHash.put(family, this.buffer.size());
//            this.buffer.add(new HashMap<>());
            for(String col: schema.get(family)) {
//                buffer.get(this.buffer.size() - 1).put(col, new HashMap<>());
                this.col2ColFamily.put(col, family);
            }
        }
    }

    public Map<String, Set<String>> getSchema() {
        return this.schema;
    }

    private void clear() {
        for(Map<String, Map<String, String>> temp1: buffer) {
            for(String col: temp1.keySet()) {
                temp1.put(col, new HashMap<>());
            }
        }
        rowOrienBuffer = new HashMap<>();
    }

    public boolean insert(String rowKey, List<String> cols, List<String> values) {
        if(rowOrienBuffer.containsKey(rowKey)) {
            Log.print("Failed to insert: row key '" + rowKey + "' already exists...");
            return false;
        }

        if(cols.size() != values.size()) {
            Log.print("Failed to delete: columns and values don't match...");
            return false;
        }
        for(int i = 0; i < cols.size(); i++) {
            String col = cols.get(i);
            String val = values.get(i);
            String family = col2ColFamily.get(col);
            if(family == null) {
                Log.print("Failed to insert: column '" + col + "' doesn't exist...");
                return false;
            }

            //start of new codes
            if(!colFamilyHash.containsKey(family)) {
                colFamilyHash.put(family, buffer.size());
                buffer.add(new HashMap<>());
            }
            Map<String, Map<String, String>> tempCols = buffer.get(colFamilyHash.get(family));
            if(!tempCols.containsKey(col)) {
                tempCols.put(col, new HashMap<>());
            }

            if(!tempCols.get(col).containsKey(rowKey)) { //first time to insert the value
                this.currSize += rowKey.getBytes().length + values.get(i).getBytes().length;
            } else {    //this cell was inserted before
                String oldVal = buffer.get(this.colFamilyHash.get(family)).get(col).get(rowKey);
                this.currSize -= (oldVal.equals("null"))? 4: oldVal.getBytes().length;
                this.currSize += val.getBytes().length;
            }

            tempCols.get(col).put(rowKey, val);
            //end of new codes

//            Map<String, String> temp = buffer.get(colFamilyHash.get(family)).get(col);
//            temp.put(rowKey, values.get(i));

            if(!this.rowOrienBuffer.containsKey(rowKey))
                this.rowOrienBuffer.put(rowKey, new HashMap<>());
            this.rowOrienBuffer.get(rowKey).put(col, values.get(i));
        }
        if(this.currSize >= this.threshold) {
            SSTableWriter sstWriter = new SSTableWriter(dbName, this, this.buffer, this.colFamilyHash, this.indexPool, this.myBF);
            try {
                sstWriter.writeToSSTable();
                clear();    //clear up memTable
                this.currSize = 0;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return true;
    }

//    public boolean update(String rowKey, String col, String value) {
//        String family = col2ColFamily.get(col);
//        if(family == null) {
//            Log.print("Column '" + col + "' doesn't exist...");
//            return false;   //no result
//        }
////        if(!this.buffer.get(this.colFamilyHash.get(family)).get(col).containsKey(rowKey)) {
////            Log.print("RowKey '" + rowKey + "' doesn't exist...");
////            return false;   //no result
////        }
//        String oldVal = buffer.get(this.colFamilyHash.get(family)).get(col).get(rowKey);
//        this.currSize -= (oldVal == null)? 0: oldVal.getBytes().length;
//        this.currSize += (oldVal == null)? rowKey.getBytes().length + value.getBytes().length: value.getBytes().length;
//
//        this.buffer.get(this.colFamilyHash.get(family)).get(col).put(rowKey, value);
//        if(!this.rowOrienBuffer.containsKey(rowKey))
//            this.rowOrienBuffer.put(rowKey, new HashMap<>());
//        this.rowOrienBuffer.get(rowKey).put(col, value);
//
//        if(this.currSize >= this.threshold) {
//            SSTableWriter sstWriter = new SSTableWriter(dbName, this, this.buffer, this.colFamilyHash, this.indexPool, this.myBF);
//            try {
//                sstWriter.writeToSSTable();
//                clear();    //clear up memTable
//                this.currSize = 0;
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//
//        return true;
//    }

    public String getOneCell(String col, String rowKey) {
        String family = col2ColFamily.get(col);
        if(family == null) {
            Log.print("Column '" + col + "' doesn't exist...");
            return "\0";   //no resultv
        }
        if(!this.colFamilyHash.containsKey(family) ||
                !this.buffer.get(colFamilyHash.get(family)).containsKey(col) ||
                !this.buffer.get(this.colFamilyHash.get(family)).get(col).containsKey(rowKey)) {
            Log.print("The desired cell doesn't exist in memTable...");
            return "\0";
        }
        return this.buffer.get(this.colFamilyHash.get(family)).get(col).get(rowKey);
    }

    public Map<String, String> getOneColumn(String col) {
        String family = this.col2ColFamily.get(col);
        if(family == null) {
            Log.print("Column '" + col + "' doesn't exist in memTable...");
            return null;    //no result
        }
        if(!this.colFamilyHash.containsKey(family) ||
                !this.buffer.get(colFamilyHash.get(family)).containsKey(col)) {
            Log.print("Column '" + col + "' doesn't exist in memTable...");
            return null;
        }
        return this.buffer.get(this.colFamilyHash.get(family)).get(col);
    }

    public Map<String, String> getOneRow(String rowKey) {
        if(!this.rowOrienBuffer.containsKey(rowKey)) {
            Log.print("RowKey '" + rowKey + "' doesn't exist in memTable...");
            return null;
        }
        return this.rowOrienBuffer.get(rowKey);
    }

    public boolean deleteOneCell(String rowKey, String col) {
        if(!this.col2ColFamily.containsKey(col)){
            Log.print("Failed to delete: column '" + col + "' doesn't exist...");
            return false;
        }
        String family = col2ColFamily.get(col);

        if(!colFamilyHash.containsKey(family)) {
            colFamilyHash.put(family, buffer.size());
            buffer.add(new HashMap<>());
        }
        Map<String, Map<String, String>> tempCols = buffer.get(colFamilyHash.get(family));
        if(!tempCols.containsKey(col)) {
            tempCols.put(col, new HashMap<>());
        }

        if(!tempCols.get(col).containsKey(rowKey)) { //first time to insert the value
            this.currSize += rowKey.getBytes().length + 4;
        } else {    //this cell was inserted before
            String oldVal = buffer.get(this.colFamilyHash.get(family)).get(col).get(rowKey);
            this.currSize -= (oldVal.equals("null"))? 4: oldVal.getBytes().length;
            this.currSize += 4;
        }

        tempCols.get(col).put(rowKey, "null");

        if(!this.rowOrienBuffer.containsKey(rowKey))
            this.rowOrienBuffer.put(rowKey, new HashMap<>());
        this.rowOrienBuffer.get(rowKey).put(col, "null");

        if(this.currSize >= this.threshold) {
            SSTableWriter sstWriter = new SSTableWriter(dbName, this, this.buffer, this.colFamilyHash, this.indexPool, this.myBF);
            try {
                sstWriter.writeToSSTable();
                clear();    //clear up memTable
                this.currSize = 0;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return true;
    }

    public boolean deleteOneRow(String rowKey) {
        if(!this.rowOrienBuffer.containsKey(rowKey)) {
            Log.print("Failed to delete: rowKey '" + rowKey + "' doesn't exist in memTable...");
            return false;
        }
        this.rowOrienBuffer.put(rowKey, null);
        return true;
    }
}
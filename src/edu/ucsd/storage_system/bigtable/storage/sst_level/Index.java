package edu.ucsd.storage_system.bigtable.storage.sst_level;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by twincus on 6/13/17.
 */
public class Index {
    private Map<String, long[]> colIndex;
    private Map<String, Long> rowIndex;
    private int sstNum;
    private String dbName;

    public Index(Map<String, long[]> colIndex, Map<String, Long> rowIndex, int sstNum) {
        this.colIndex = colIndex;
        this.rowIndex = rowIndex;
        this.sstNum = sstNum;
    }

    public Index(int sstNum, String dbName) {
        this.colIndex = new HashMap<>();
        this.rowIndex = new HashMap<>();
        this.sstNum = sstNum;
        this.dbName = dbName;

        try {
            loadIndex();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void loadIndex() throws IOException {
        File colFile = new File("database" + File.separator + dbName + File.separator + "index" + File.separator + "col" + String.valueOf(sstNum));
        File rowFile = new File("database" + File.separator + dbName + File.separator + "index" + File.separator + "row" + String.valueOf(sstNum));

        BufferedReader br = new BufferedReader(new InputStreamReader(
                new FileInputStream(colFile), "UTF-8"));
        String line = null;
        int k = 1;
        String familyName = null;
        while((line = br.readLine()) != null) {
            if(k == 1)
                familyName = line;
            else {
                String[] part = line.split("\0");
                for (int i = 0; i < part.length - 1; i += 3) {
                    String key = familyName + "." + part[i];
                    long[] val = new long[]{Long.parseLong(part[i + 1]), Long.parseLong(part[i + 2])};
                    colIndex.put(key, val);
                }
            }
            k++;
        }
        br.close();

        br = new BufferedReader(new InputStreamReader(
                new FileInputStream(rowFile), "UTF-8"));
        while((line = br.readLine()) != null) {
            String[] part = line.split("\0");
            for(int i = 0; i < part.length-1; i += 2) {
                rowIndex.put(part[i], Long.parseLong(part[i + 1]));
            }
        }
        br.close();
    }

    public boolean containOneCell(String row, String col) {
        return rowIndex.containsKey(row + "." + col);
    }

    public boolean containOneColumn(String family, String col) {
        return colIndex.containsKey(family + "." + col);
    }

    public long getOneCellOffset(String row, String col) {
        return rowIndex.get(row + "." + col);
    }

    public long[] getOneColumnOffset(String family, String col) {
        return colIndex.get(family + "." + col);
    }

    public int getSstNum() {
        return this.sstNum;
    }
}

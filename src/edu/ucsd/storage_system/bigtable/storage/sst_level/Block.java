package edu.ucsd.storage_system.bigtable.storage.sst_level;

import java.util.Map;

/**
 * Created by twincus on 6/13/17.
 */
public class Block {
    public Map<String, Map<String, String>> cols;
    public String familyName;
    public int sstNumb;
    private int hashVal;

    public Block(Map<String, Map<String, String>> cols, String familyName, int sstNumb) {
        this.cols = cols;
        this.familyName = familyName;
        this.sstNumb = sstNumb;

        hashVal = familyName.hashCode();
        for(String col: cols.keySet()) {
            hashVal += col.hashCode();
            Map<String, String> temp = cols.get(col);
            for(String row: temp.keySet()) {
                hashVal += row.hashCode() + temp.get(row).hashCode();
            }
        }
    }

    @Override
    public int hashCode() {
        return hashVal;
    }
}

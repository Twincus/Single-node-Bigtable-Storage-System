package edu.ucsd.storage_system.bigtable.io;

import edu.ucsd.storage_system.bigtable.storage.sst_level.Block;
import edu.ucsd.storage_system.bigtable.storage.sst_level.Index;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by twincus on 6/7/17.
 */
public class SSTableReader {
    private static int BLOCK_SIZE;

    static{
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

    public static String getOneCell(String dbName, String familyName, String colName, String rowKey,
                                    Index ind, List<Block> blockList) {
        try {
            long offset = ind.getOneCellOffset(rowKey, colName);
            long blockID = offset/BLOCK_SIZE;
            long blockStart = blockID * BLOCK_SIZE;

            FileInputStream fis = new FileInputStream("database" + File.separator + dbName + File.separator + "sst.db");
            fis.skip(blockStart);
            byte[] bytes = new byte[BLOCK_SIZE];
            fis.read(bytes, 0, BLOCK_SIZE);
            String[] subs = new String(bytes).split("\0");

            Map<String, Map<String, String>> vals = new HashMap<>();
            vals.put(colName, new HashMap<>());
            Map<String, String> temp = vals.get(colName);

            for(int i = 0; i < subs.length; i += 2) {
//                if(subs[i].equals("\0"))    //padding, the remaining are all '\0'
//                    break;
                temp.put(subs[i], subs[i + 1]);
            }
            Block bl = new Block(vals, familyName, ind.getSstNum());
            blockList.add(bl);
            return vals.get(colName).get(rowKey);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Map<String, String> getOneColumn(String dbName, String familyName, String colName, Index ind, List<Block> blockList) {
        try {
            Map<String, String> res = new HashMap<>(); //key: row; value: value

            long[] offset = ind.getOneColumnOffset(familyName, colName);
            long blockStartID = offset[0]/BLOCK_SIZE;
            long blockStart = blockStartID * BLOCK_SIZE;
            int blockCount = (int)(offset[1]/BLOCK_SIZE) - (int)blockStartID + 1;

            FileInputStream fis = new FileInputStream("database" + File.separator + dbName + File.separator + "sst.db");
            fis.skip(blockStart);

            for(int k = 0; k < blockCount; k++) {
                byte[] bytes = new byte[BLOCK_SIZE];
                fis.read(bytes, 0, BLOCK_SIZE);

                String[] subs = new String(bytes).split("\0");

                Map<String, Map<String, String>> vals = new HashMap<>();
                vals.put(colName, new HashMap<>());
                Map<String, String> temp = vals.get(colName);

                for(int i = 0; i < subs.length; i += 2) {
                    if(subs[i].equals("\0"))    //padding, the remaining are all '\0'
                        break;
                    temp.put(subs[i], subs[i + 1]);
                    res.put(subs[i], subs[i + 1]);
                }
                Block bl = new Block(vals, familyName, ind.getSstNum());
                blockList.add(bl);
            }
            return res;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Map<String, String> getOneRow(String dbName, String familyName, String row, Index ind, List<String> cols, List<Block> blockList) {
        Map<String, String> res = new HashMap<>(); //key: row; value: value

        for(String col: cols) {
            String val = getOneCell(dbName, familyName, col, row, ind, blockList);
            res.put(col, val);
        }
        return res;
    }

}

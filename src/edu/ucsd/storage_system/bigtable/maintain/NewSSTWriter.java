package edu.ucsd.storage_system.bigtable.maintain;

import edu.ucsd.storage_system.bigtable.main.Initiator;

import java.io.*;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Created by twincus on 6/15/17.
 */
public class NewSSTWriter {
    private FileOutputStream output;
    private FileOutputStream colBFOutput;
    private FileOutputStream rowBFOutput;
    private FileOutputStream colIndexOutput;
    private FileOutputStream rowIndexOutput;


    private String oldDbName;
    private String family;
    private static final String tempName = "temp";
    private int currSize = 0;
    private static int offset = 0;
    private int BLOCK_SIZE;
    private Set<String> colBFSet;
    private Set<String> rowBFSet;

    public NewSSTWriter(String dbName, int sstID, String family) {
        this.oldDbName = dbName;
        this.family = family;
        colBFSet = new HashSet<>();
        rowBFSet = new HashSet<>();

        File file = new File("database" + File.separator + tempName);
        file.mkdir();
        file = new File("database" + File.separator + tempName + File.separator + "index");
        file.mkdir();
        file = new File("database" + File.separator + tempName + File.separator + "bloom_filter");
        file.mkdir();

        try {
            output = new FileOutputStream("database" + File.separator + tempName + File.separator + "sst.db", true);
            colBFOutput = new FileOutputStream("database" + File.separator + tempName + File.separator + "bloom_filter" + File.separator + "col_BF", true);
            rowBFOutput = new FileOutputStream("database" + File.separator + tempName + File.separator + "bloom_filter" + File.separator + "row_BF", true);

            colIndexOutput = new FileOutputStream("database" + File.separator + tempName + File.separator + "index" + File.separator + "col" + sstID, true);
            rowIndexOutput = new FileOutputStream("database" + File.separator + tempName + File.separator + "index" + File.separator + "row" + sstID, true);

        } catch (IOException e) {
            e.printStackTrace();
        }

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

        try {
            colIndexOutput.write(family.getBytes());
            colIndexOutput.write('\n');
        } catch (IOException e) {
            e.printStackTrace();
        }

        File schema = new File("database" + File.separator + dbName + File.separator + "schema");
        schema.renameTo(new File("database" + File.separator + tempName + File.separator + schema.getName()));
    }

    public void writeSST(List<String[]> pairs, String col) throws IOException {
        int start = offset;

        for(int i = 0; i < pairs.size(); i++) {
            String[] pair = pairs.get(i);

            byte[] rowkey = pair[0].getBytes();
            byte[] val = pair[1].getBytes();


            rowIndexOutput.write(pair[0].getBytes());
            rowIndexOutput.write('.');
            rowIndexOutput.write(col.getBytes());
            rowIndexOutput.write(0);
            rowIndexOutput.write(String.valueOf(offset).getBytes());
            rowIndexOutput.write(0);

            int plus = rowkey.length + val.length + 2;
            offset += plus;

            if (currSize + plus > BLOCK_SIZE) {
                output.write(new byte[BLOCK_SIZE - currSize]);
                offset += BLOCK_SIZE - currSize;
                currSize = plus;
            } else {
                currSize += plus;
            }

            output.write(rowkey);
            output.write(0);
            output.write(val);
            output.write(0);

            if (!colBFSet.contains(col)) {
                colBFSet.add(col);
                colBFOutput.write(col.getBytes());
                colBFOutput.write(0);
            }

            String row = pair[0];
            if (!rowBFSet.contains(row)) {
                rowBFSet.add(row);
                rowBFOutput.write(row.getBytes());
                rowBFOutput.write(0);
            }

        }

        if (currSize < BLOCK_SIZE) {
            output.write(new byte[BLOCK_SIZE - currSize]);
            offset += BLOCK_SIZE - currSize;
        }
        currSize = 0;

        colIndexOutput.write(col.getBytes());
        colIndexOutput.write(0);
        colIndexOutput.write(String.valueOf(start).getBytes()); //starting offset of a column
        colIndexOutput.write(0);
        colIndexOutput.write(String.valueOf(offset - 1).getBytes());    //end offset of a column
        colIndexOutput.write(0);
    }

    public void closeAllStream() {
        try {
            output.close();
            colBFOutput.close();
            rowBFOutput.close();
            colIndexOutput.close();
            rowIndexOutput.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void modifyName(String oldDbName) {
        File temp = new File("database" + File.separator + tempName);
        temp.renameTo(new File("database" + File.separator + oldDbName));
    }

    public static void deleteDir(File file) {
        File[] contents = file.listFiles();
        if (contents != null) {
            for (File f : contents) {
                deleteDir(f);
            }
        }
        file.delete();
    }

    public void writeNewLineToBF() {
        try {
            colBFOutput.write('\n');
            rowBFOutput.write('\n');
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

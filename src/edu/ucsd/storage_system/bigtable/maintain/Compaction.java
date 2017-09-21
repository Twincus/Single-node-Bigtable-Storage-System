package edu.ucsd.storage_system.bigtable.maintain;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by twincus on 6/15/17.
 */
public class Compaction {
    private String dbName;
    private NewSSTWriter writer;

    public static void main(String[] args) {
        Compaction compaction = new Compaction("Student");
        compaction.startCompaction();
    }

    public Compaction(String dbName) {
        this.dbName = dbName;
    }

    private void startCompaction() {
        String root = "database" + File.separator + dbName + File.separator + "index";
        int count = new File(root).listFiles().length / 2;
        List<Unit> uniList = new ArrayList<>();
        for(int i = count; i >= 1; i--) {
            try {
                uniList.add(new Unit(new FileInputStream(root + File.separator + "col" + i), i));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        //group column index by column family
        Map<String, List<Unit>> families = new HashMap<>();
        for(int i = 0; i < uniList.size(); i++) {
            try {
                Unit uni = uniList.get(i);
                String family = readLine(uni.input);
                if(!families.containsKey(family)) {
                    families.put(family, new ArrayList<>());
                }
                families.get(family).add(uni);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //key: family
        int i = 0;
        for(Map.Entry<String, List<Unit>> en: families.entrySet()){
            this.writer = new NewSSTWriter(dbName, ++i, en.getKey());
            try {
                mergeSortColumn(en);
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.writer.writeNewLineToBF();
            this.writer.closeAllStream();
        }
        NewSSTWriter.deleteDir(new File("database" + File.separator + dbName));
        NewSSTWriter.modifyName(dbName);
    }

    private void mergeSortColumn(Map.Entry<String, List<Unit>> en) throws IOException {
        List<Unit> inputList = en.getValue();
        List<String[]> buffs = new ArrayList<>();

        for(Unit unit: inputList) {
            String[] tuple = new String[]{readSub(unit.input),
                    readSub(unit.input), readSub(unit.input)};
            buffs.add(tuple);
        }

        while(buffs.size() != 0) {
            List<String[]> compareList = new ArrayList<>();

            String small = getSmallest(buffs);

            for(int i = 0; i < buffs.size(); i++) {
                String[] arr = buffs.get(i);

                if(arr[0].equals(small)) {
                    compareList.add(arr);
                    Unit u = inputList.get(i);
                    String sub = readSub(u.input);

                    if(sub.equals("\0")) {
                        inputList.get(i).input.close();
                        inputList.remove(i);

                        buffs.remove(i);
                        i--;
                    } else {
                        buffs.set(i, new String[]{sub, readSub(u.input), readSub(u.input)});
                    }
                }
            }
            mergeSortRow(compareList);
        }
    }

    private String getSmallest(List<String[]> list) {
        String res = list.get(0)[0];
        for(String[] arr : list) {
            if(arr[0].compareTo(res) < 0)
                res = arr[0];
        }
        return res;
    }

    /**
     * each String[] is column : start offset : end offset
     * @param list
     * @throws IOException
     */
    private void mergeSortRow(List<String[]> list) throws IOException {

        String colName = list.get(0)[0];

        List<String[]> queues = new ArrayList<>();

        for(int i = 0; i < list.size(); i++) {
            FileInputStream fis = new FileInputStream("database" + File.separator + dbName + File.separator + "sst.db");
            fis.skip(Long.parseLong(list.get(i)[1]));
            int len = Integer.parseInt(list.get(i)[2]) - Integer.parseInt(list.get(i)[1]) + 1;
            byte[] data = new byte[len];
            fis.read(data, 0, len);

            queues.add(new String(data).split("\0"));
        }

        //to compare rowkey and call NewSSTWriter
        List<String[]> combine = new ArrayList<>();
        List<String[]> buffs = new ArrayList<>();
        List<Integer> count = new ArrayList<>();

        for(String[] que: queues) {
            buffs.add(new String[]{que[0], que[1]});
            count.add(2);
        }

        while(!buffs.isEmpty()) {
            String small = getSmallest(buffs);
            boolean flag = false;
            for (int i = 0; i < buffs.size(); i++) {

                if(buffs.get(i)[0].equals(small)) {

                    if(!flag) {
                        combine.add(buffs.get(i));
                        flag = true;
                    }

                    String[] que = queues.get(i);
                    int c = count.get(i);
                    if(c >= que.length) {   //queue is exhausted
                        queues.remove(i);
                        buffs.remove(i);
                        i--;
                    } else {
                        buffs.set(i, new String[]{que[c], que[c + 1]});
                        count.set(i, c + 2);
                    }
                }
            }
        }

        writer.writeSST(combine, colName);
    }

    private String readSub(FileInputStream input) throws IOException {
        StringBuffer sb = new StringBuffer();
        while(true) {
            int by = input.read();
            if(by == 0)
                break;
            else if(by == -1)   //end of file
                return "\0";
            sb.append((char)by);
        }
        return sb.toString();
    }

    private String readLine(FileInputStream input) throws IOException {
        StringBuffer sb = new StringBuffer();
        while(true) {
            int by = input.read();
            if(by == '\n')
                break;
            sb.append((char)by);
        }
        return sb.toString();
    }

    class Unit {
        public FileInputStream input;
        public int id;  //sst id

        public Unit(FileInputStream input, int id) {
            this.input = input;
            this.id = id;
        }
    }
}

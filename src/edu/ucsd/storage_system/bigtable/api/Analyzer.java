package edu.ucsd.storage_system.bigtable.api;

import edu.ucsd.storage_system.bigtable.interaction.DBHandler;
import edu.ucsd.storage_system.bigtable.main.Initiator;
import edu.ucsd.storage_system.bigtable.utils.ReturnValue;

import javax.xml.bind.SchemaOutputResolver;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by twincus on 6/15/17.
 */
public class Analyzer {
    public static Initiator ini = new Initiator();

    public static void main(String[] args) {
        Analyzer ana = new Analyzer();
        ana.countOneColumn();
//        ana.distinctValueOneColumn();
//        ana.sumOneColumn();
    }

    public void countOneColumn() {
        DBHandler handler = new DBHandler("countOneColumn", "PersonalInfo", "Student",
                "name", 5, ini.memTablePool, ini.indexPool, ini.bloomFilterPool,
                ini.readCachePool.getReadCache("Student"),
                ini.indexPool.getIndexCache("Student"));
        ReturnValue ret = handler.start();

        System.out.println(ret.getMap().size());
    }

    public void distinctValueOneColumn() {
        DBHandler handler = new DBHandler("countOneColumn", "PersonalInfo", "Student",
                "gender", 5, ini.memTablePool, ini.indexPool, ini.bloomFilterPool,
                ini.readCachePool.getReadCache("Student"),
                ini.indexPool.getIndexCache("Student"));
        ReturnValue ret = handler.start();
        Map<String, String> map = ret.getMap();

        Set<String> set = new HashSet<>();
        for(String v: map.values()) {
            if(!set.contains(v))
                set.add(v);
        }
        for(String v: set) {
            System.out.println(v);
        }
    }

    public void sumOneColumn() {
        String col = "age";
        DBHandler handler = new DBHandler("countOneColumn", "PersonalInfo", "Student",
                col, 5, ini.memTablePool, ini.indexPool, ini.bloomFilterPool,
                ini.readCachePool.getReadCache("Student"),
                ini.indexPool.getIndexCache("Student"));
        ReturnValue ret = handler.start();
        Map<String, String> map = ret.getMap();

        int sum = 0;
        for(String v: map.values()) {
            sum += Integer.parseInt(v);
        }
        System.out.println("sum of " + col + " = " + sum);
    }
}

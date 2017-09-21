package edu.ucsd.storage_system.bigtable.test;

import edu.ucsd.storage_system.bigtable.interaction.DBHandler;
import edu.ucsd.storage_system.bigtable.main.Initiator;

import java.util.*;

/**
 * Created by twincus on 6/14/17.
 */
public class Test {
    public static void main(String[] args) {
        Initiator ini = new Initiator();

        createDatabase1(ini);
        insert1(ini);
        insert2(ini);
        insert3(ini);
//        getOneColumn1(ini);

        //        updateOneCell(ini);
//        getOneCell1(ini);
//        deleteOneCell(ini);
//        getOneRow1(ini);
//        updateOneCell(ini);
//        getOneCell1(ini);
    }

    private static void createDatabase1(Initiator ini) {
        Map<String, Set<String>> schema = new HashMap<>();

        String family1 = "PersonalInfo";
        Set<String> cols1 = new HashSet<>();
        cols1.add("name");
        cols1.add("age");
        cols1.add("gender");

        String family2 = "PublicInfo";
        Set<String> cols2 = new HashSet<>();
        cols2.add("school");

        schema.put(family1, cols1);
        schema.put(family2, cols2);

        DBHandler t1 = new DBHandler("t1", ini.memTablePool, ini.indexPool, "Student",
                schema, "StudentID", 1, ini.bloomFilterPool, ini.readCachePool);
        t1.start();
    }

    private static void insert1(Initiator ini) {
        List<String> cols = new ArrayList<>();
        cols.add("name");
        cols.add("age");
        cols.add("gender");
        cols.add("school");

        List<String> vals = new ArrayList<>();
        vals.add("John");
        vals.add("20");
        vals.add("male");
        vals.add("UCSD");

        DBHandler t2 = new DBHandler("t2", ini.memTablePool, ini.indexPool,
                "Student", "001", cols, vals, 2, ini.bloomFilterPool);
        t2.start();
    }

    private static void insert2(Initiator ini) {
        List<String> cols = new ArrayList<>();
        cols.add("name");
        cols.add("age");
        cols.add("gender");
        cols.add("school");

        List<String> vals = new ArrayList<>();
        vals.add("Mary");
        vals.add("19");
        vals.add("female");
        vals.add("UCLA");

        DBHandler t3 = new DBHandler("t3", ini.memTablePool, ini.indexPool,
                "Student", "002", cols, vals, 2, ini.bloomFilterPool);
        t3.start();
    }

    private static void insert3(Initiator ini) {
        List<String> cols = new ArrayList<>();
        cols.add("name");
        cols.add("age");
        cols.add("gender");
        cols.add("school");

        List<String> vals = new ArrayList<>();
        vals.add("Taylor");
        vals.add("25");
        vals.add("female");
        vals.add("CMU");

        DBHandler t4 = new DBHandler("t3", ini.memTablePool, ini.indexPool,
                "Student", "003", cols, vals, 2, ini.bloomFilterPool);
        t4.start();
    }

    private static void getOneCell1(Initiator ini) {
        DBHandler t4 = new DBHandler("t4", "PersonalInfo",
                "Student", "age", "001", 4, ini.memTablePool,
                ini.indexPool, ini.bloomFilterPool, ini.readCachePool.getReadCache("Student"),
                ini.indexPool.getIndexCache("Student"));
        t4.start();
    }

    private static void getOneColumn1(Initiator ini) {
        DBHandler t5 = new DBHandler("t5", "PersonalInfo", "Student",
                "age", 5, ini.memTablePool, ini.indexPool, ini.bloomFilterPool,
                ini.readCachePool.getReadCache("Student"),
                ini.indexPool.getIndexCache("Student"));
        t5.start();
    }

    private static void getOneRow1(Initiator ini) {
        DBHandler t6 = new DBHandler("t6", "Student", "001", 6, ini.memTablePool,
                ini.indexPool, ini.bloomFilterPool, ini.readCachePool.getReadCache("Student"),
                ini.indexPool.getIndexCache("Student"));
        t6.start();
    }

    private static void updateOneCell(Initiator ini) {
//        DBHandler t7 = new DBHandler("t7", "Student", "001", "age",
//                "22", 3, ini.memTablePool, ini.indexPool);
//        t7.start();

        List<String> cols = new ArrayList<>();
        cols.add("age");

        List<String> vals = new ArrayList<>();
        vals.add("27");

        DBHandler t7 = new DBHandler("t7", ini.memTablePool, ini.indexPool,
                "Student", "001", cols, vals, 2, ini.bloomFilterPool);
        t7.start();
    }

    private static void deleteOneCell(Initiator ini) {
        DBHandler t8 = new DBHandler("t8", "Student", "age", "001",
                7, ini.memTablePool, ini.indexPool);
        t8.start();
    }
}

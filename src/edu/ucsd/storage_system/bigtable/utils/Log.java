package edu.ucsd.storage_system.bigtable.utils;

/**
 * Created by twincus on 6/3/17.
 */
public class Log {
    private static boolean debug = true;
    private static boolean thread = true;

    public static void print(String str) {
        if(debug)
           System.out.println(str);
    }

    public static void printThread(String str) {
        if(thread)
            System.out.println(str);
    }

}

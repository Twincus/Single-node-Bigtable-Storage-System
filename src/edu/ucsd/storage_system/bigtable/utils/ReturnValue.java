package edu.ucsd.storage_system.bigtable.utils;

import java.util.Map;

/**
 * Created by twincus on 6/15/17.
 */
public class ReturnValue {
    private Map<String, String> map;
    String str;

    public ReturnValue(Map<String, String> map, String str) {
        this.map = map;
        this.str = str;
    }

    public Map<String, String> getMap() {
        return this.map;
    }

    public String getStr() {
        return  this.str;
    }
}

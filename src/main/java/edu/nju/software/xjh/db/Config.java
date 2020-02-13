package edu.nju.software.xjh.db;

import java.util.HashMap;

public class Config {

    private HashMap<ConfigVar, String> vals = new HashMap<>();

    public Config() {
        for (ConfigVar value : ConfigVar.values()) {
            vals.put(value, value.getDevaultValue());
        }
    }

    public void setVal(ConfigVar var, String val) {
        vals.put(var, val);
    }

    public String getVal(ConfigVar var) {
        return vals.get(var);
    }

    public enum ConfigVar {
        SEGMENT_MAX_SIZE("4,10,40,100"),
        RECORD_LIST_SIZE("2000"),
        IO_THREADS("4"),
        SOLUTION_VERSION("V1");

        private String defaultVal;

        ConfigVar(String value) {
            this.defaultVal = value;
        }
        public String getDevaultValue() {
            return defaultVal;
        }
    }
}

package edu.nju.software.xjh.db;

import java.util.HashMap;
import java.util.Map;

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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Config{");
        for (Map.Entry<ConfigVar, String> entry : vals.entrySet()) {
            sb.append(String.format("'%s':'%s' ", entry.getKey().name(), entry.getValue()));
        }
        sb.append('}');

        return sb.toString();
    }

    public enum ConfigVar {
        LEVEL_COUNT("4"),
        LEVEL_MIN_SIZE("5,5,10,4"),
        COMPACTION_MAX_PICK_COUNT("5,5,3,2"), // 一次compaction最多使用上层文件数
        COMPACTION_MAX_PICK_SIZE("999,40,100,180"), // 一次compaction最多使用上层文件大小，单位MB，其中最上与最下层不限制
        SEGMENT_MAX_SIZE("4,10,40,100"), // 单位为MB
        RECORD_LIST_SIZE("2000"),
        IO_THREADS("4"),
        SOLUTION_VERSION("V1"),
        FILE_BASE_PATH("g:/data/");

        private String defaultVal;

        ConfigVar(String value) {
            this.defaultVal = value;
        }
        public String getDevaultValue() {
            return defaultVal;
        }
    }
}

package edu.nju.software.xjh.db;

public enum Config {
    SEGMENT_MAX_SIZE("4,10,40,100");

    private String val;

    Config(String value) {
      this.val = value;
    }
    public String getVal() {
        return val;
    }
}

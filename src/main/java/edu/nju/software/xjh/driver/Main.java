package edu.nju.software.xjh.driver;

import edu.nju.software.xjh.db.Config;
import edu.nju.software.xjh.db.DB;
import edu.nju.software.xjh.model.Record;
import edu.nju.software.xjh.model.RecordFactory;
import edu.nju.software.xjh.model.RecordImpl;
import edu.nju.software.xjh.util.VarInt;

public class Main {

    public static void main(String[] args) {
        DB db = new DB(new Config(), "test1");
        db.init();
        for (int i = 1; i < 100000; i++) {
            Record record = RecordFactory.createEmpty();
            byte[] bytes = VarInt.encodeSignedVarInt(i);
            record.setId(i);
            record.setKey(bytes);
            record.setValue(bytes);

            db.write(record);
        }
    }

}

package edu.nju.software.xjh.driver;

import edu.nju.software.xjh.db.Config;
import edu.nju.software.xjh.db.DB;
import edu.nju.software.xjh.model.Record;
import edu.nju.software.xjh.model.RecordFactory;
import edu.nju.software.xjh.model.RecordImpl;
import edu.nju.software.xjh.util.VarInt;

public class Main {

    public static void main(String[] args) {
        Config conf = new Config();

        conf.setVal(Config.ConfigVar.SOLUTION_VERSION, "V2");

        DB db = new DB(conf, "test1");
        db.init();
//        for (int i = 1; i < 100000; i++) {
//            Record record = RecordFactory.createEmpty();
//            byte[] bytes = VarInt.encodeSignedVarInt(i);
//            record.setId(i);
//            record.setKey(bytes);
//            record.setValue(bytes);
//
//            db.write(record);
//        }
        byte[] digits = new byte[]{'0','1','2','3','4','5','6','7','8','9'};
        byte[] k = new byte[7];

            for (int prefix = 1; prefix < 5000; prefix ++) {
                for (int suffix = 1; suffix < 300; suffix++) {

                Record record = RecordFactory.createEmpty();

                k[6] = digits[suffix%10];
                k[5] = digits[(suffix%100)/10];
                k[4] = digits[suffix/100];

                k[3] = digits[prefix%10];
                k[2] = digits[(prefix%100)/10];
                k[1] = digits[(prefix%1000)/100];
                k[0] = digits[prefix/1000];

                record.setId(prefix * 1000 + suffix);
                byte[] kv = k.clone();
                record.setKey(kv);
                record.setValue(kv);

                db.write(record);
            }
            System.out.println("prefix " + prefix + " finish");
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("All finished writing");
    }

}

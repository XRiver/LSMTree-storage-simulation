package edu.nju.software.xjh.flush;

import edu.nju.software.xjh.db.Config;
import edu.nju.software.xjh.db.DB;
import edu.nju.software.xjh.db.SolutionVersion;

public class FlushHandlerFactory {
    public static FlushHandler createFlushHandler(SolutionVersion version, DB db) {
        switch (version) {
            case V1:
                return new FlushHandlerV1(db);
            case V2:
                return new FlushHandlerV2(db);
            default:
                throw new IllegalArgumentException("Unknown version:" + version);
        }
    }
}

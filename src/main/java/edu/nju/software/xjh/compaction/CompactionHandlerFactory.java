package edu.nju.software.xjh.compaction;

import edu.nju.software.xjh.db.DB;
import edu.nju.software.xjh.db.SolutionVersion;

public class CompactionHandlerFactory {
    public static CompactionHandler createCompactionHandler(SolutionVersion version, DB db) {
        switch (version) {
            case V1:
                return new CompactionHandlerV1(db);
            case V2:
                return new CompactionHandlerV2(db);
            default:
                throw new IllegalArgumentException("Unknown version:" + version);
        }
    }
}

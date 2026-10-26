package group.gnometrading.merger;

import group.gnometrading.schemas.StreamRecord;
import java.util.List;
import java.util.Map;

public interface MergeStrategy {

    /**
     * Merges collectors' copies of one stream-minute, keyed by each copy's id, into one.
     * @return the list of merged records
     */
    List<StreamRecord> mergeRecords(Map<String, List<StreamRecord>> entries);
}

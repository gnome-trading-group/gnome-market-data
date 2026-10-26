package group.gnometrading.merger;

import group.gnometrading.schemas.StreamRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Keeps the collector copy with the most records, whatever the record type: every copy of a stream-minute holds the
 * same records from the same venue, so the fullest one lost least.
 */
public final class FullestCopyMergeStrategy implements MergeStrategy {

    private static final Logger logger = LogManager.getLogger(FullestCopyMergeStrategy.class);

    @Override
    public List<StreamRecord> mergeRecords(Map<String, List<StreamRecord>> entries) {
        if (entries.isEmpty()) {
            logger.info("No records to process");
            return List.of();
        }

        // Find the collector with the most records
        String winningCollector = null;
        int maxRecords = -1;
        for (var entry : entries.entrySet()) {
            if (entry.getValue().size() > maxRecords) {
                maxRecords = entry.getValue().size();
                winningCollector = entry.getKey();
            }
        }

        // Log differences for other collectors (only when there's actually a difference)
        for (var entry : entries.entrySet()) {
            if (!entry.getKey().equals(winningCollector)) {
                int missing = maxRecords - entry.getValue().size();
                if (missing > 0) {
                    logger.warn(String.format(
                            "Collector %s has %d fewer records than %s (%d vs %d)",
                            entry.getKey(),
                            missing,
                            winningCollector,
                            entry.getValue().size(),
                            maxRecords));
                }
            }
        }

        return new ArrayList<>(entries.get(winningCollector));
    }
}

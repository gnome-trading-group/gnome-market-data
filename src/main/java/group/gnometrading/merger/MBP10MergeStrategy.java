package group.gnometrading.merger;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import group.gnometrading.schemas.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Merge strategy for MBP10 schema type.
 * Produces output entries in sequence order.
 */
public class MBP10MergeStrategy implements SchemaMergeStrategy {

    @Override
    public List<Schema> mergeRecords(LambdaLogger logger, Map<String, List<Schema>> entries) {
        if (entries.isEmpty()) {
            logger.log("No records to process");
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
                    logger.log(
                            String.format("Collector %s has %d fewer records than %s (%d vs %d)",
                                    entry.getKey(), missing, winningCollector, entry.getValue().size(), maxRecords)
                    );
                }
            }
        }

        return new ArrayList<>(entries.get(winningCollector));
    }
}


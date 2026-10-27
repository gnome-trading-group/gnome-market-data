package group.gnometrading.inventory;

/**
 * Utility class for building DynamoDB partition and sort keys for coverage records.
 */
public class CoverageKey {

    private CoverageKey() {}

    // Partition key builders

    /**
     * Global partition key for overall summary.
     * @return "GLOBAL"
     */
    public static String globalKey() {
        return "GLOBAL";
    }

    /**
     * Stream partition key for one stream's coverage.
     * @param streamId The stream ID
     * @return "STREAM#{streamId}"
     */
    public static String streamKey(int streamId) {
        return "STREAM#" + streamId;
    }

    // Sort key builders

    /**
     * Summary sort key for aggregated summary data.
     * @return "SUMMARY"
     */
    public static String summaryKey() {
        return "SUMMARY";
    }

    /**
     * Date sort key for date-specific coverage.
     * @param date Date in YYYY-MM-DD format
     * @return "DATE#{date}"
     */
    public static String dateKey(String date) {
        return "DATE#" + date;
    }
}

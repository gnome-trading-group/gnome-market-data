package group.gnometrading.coverage;

/**
 * Utility class for building DynamoDB partition and sort keys for coverage records.
 */
public class CoverageKey {
    
    // Partition key builders
    
    /**
     * Global partition key for overall summary.
     * @return "GLOBAL"
     */
    public static String globalKey() {
        return "GLOBAL";
    }
    
    /**
     * Security partition key for security-level aggregation.
     * @param securityId The security ID
     * @return "SEC#{securityId}"
     */
    public static String securityKey(int securityId) {
        return "SEC#" + securityId;
    }
    
    /**
     * Security+Exchange partition key for specific security-exchange pair.
     * @param securityId The security ID
     * @param exchangeId The exchange ID
     * @return "SEC#{securityId}#EX#{exchangeId}"
     */
    public static String securityExchangeKey(int securityId, int exchangeId) {
        return "SEC#" + securityId + "#EX#" + exchangeId;
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


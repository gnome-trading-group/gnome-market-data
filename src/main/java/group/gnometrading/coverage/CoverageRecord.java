package group.gnometrading.coverage;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

import java.util.Map;

/**
 * DynamoDB bean for coverage records.
 * 
 * Table schema:
 * - PK: pk (string) - "GLOBAL" | "SEC#{securityId}" | "SEC#{securityId}#EX#{exchangeId}"
 * - SK: sk (string) - "SUMMARY" | "DATE#{YYYY-MM-DD}" | "SCHEMA#{schemaType}"
 * - data: Map<String, Object> - Flexible JSON data containing coverage metrics
 * - lastUpdated: Long - Unix timestamp
 * - version: String - Schema version for migrations
 */
@DynamoDbBean
public class CoverageRecord {

    private String pk;
    private String sk;
    private Map<String, Object> data;
    private Long lastUpdated;
    private String version;

    public CoverageRecord() {
        // Required no-arg constructor for DynamoDB mapper
    }

    public CoverageRecord(String pk, String sk, Map<String, Object> data) {
        this.pk = pk;
        this.sk = sk;
        this.data = data;
        this.lastUpdated = System.currentTimeMillis() / 1000;
        this.version = "1.0";
    }

    @DynamoDbPartitionKey
    @DynamoDbAttribute("pk")
    public String getPk() {
        return pk;
    }

    public void setPk(String pk) {
        this.pk = pk;
    }

    @DynamoDbSortKey
    @DynamoDbAttribute("sk")
    public String getSk() {
        return sk;
    }

    public void setSk(String sk) {
        this.sk = sk;
    }

    @DynamoDbAttribute("data")
    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    @DynamoDbAttribute("lastUpdated")
    public Long getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Long lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    @DynamoDbAttribute("version")
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return "CoverageRecord{" +
                "pk='" + pk + '\'' +
                ", sk='" + sk + '\'' +
                ", version='" + version + '\'' +
                ", lastUpdated=" + lastUpdated +
                '}';
    }
}


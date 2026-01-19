package group.gnometrading.coverage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.enhanced.dynamodb.AttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.AttributeValueType;
import software.amazon.awssdk.enhanced.dynamodb.EnhancedType;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.*;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

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

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> TYPE = new TypeReference<>() {};

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
    @DynamoDbConvertedBy(CoverageDataConverter.class)
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

    public static class CoverageDataConverter implements AttributeConverter<Map<String, Object>> {
        @Override
        public AttributeValue transformFrom(Map<String, Object> input) {
            if (input == null) {
                return AttributeValue.builder().nul(true).build();
            }

            try {
                return AttributeValue.builder()
                        .s(MAPPER.writeValueAsString(input))
                        .build();
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Failed to serialize map to JSON", e);
            }
        }

        @Override
        public Map<String, Object> transformTo(AttributeValue input) {
            if (input == null || input.nul() != null && input.nul()) {
                return null;
            }

            try {
                return MAPPER.readValue(input.s(), TYPE);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Failed to deserialize JSON to map", e);
            }
        }

        @Override
        public EnhancedType<Map<String, Object>> type() {
            return EnhancedType.mapOf(
                    EnhancedType.of(String.class),
                    EnhancedType.of(Object.class)
            );
        }

        @Override
        public AttributeValueType attributeValueType() {
            return AttributeValueType.S;
        }
    }
}


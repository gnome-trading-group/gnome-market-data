package group.gnometrading.transformer;

import group.gnometrading.schemas.SchemaType;
import software.amazon.awssdk.enhanced.dynamodb.AttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.AttributeValueType;
import software.amazon.awssdk.enhanced.dynamodb.EnhancedType;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.*;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * DynamoDB bean for transformation jobs.
 * 
 * Table schema:
 * - PK: jobId (string)
 * - SK: timestamp (LocalDateTime)
 * - listingId: int
 * - schemaType: string
 * - status: TransformationStatus enum
 * - createdAt: LocalDateTime
 * - processedAt: LocalDateTime (nullable)
 * - errorMessage: String (nullable)
 * - expiresAt: Long (TTL, nullable)
 */
@DynamoDbBean
public class TransformationJob {

    private JobId jobId;
    private LocalDateTime timestamp;
    private Integer listingId;
    private SchemaType schemaType;
    private TransformationStatus status;
    private LocalDateTime createdAt;
    private LocalDateTime processedAt;
    private String errorMessage;
    private Long expiresAt;

    public TransformationJob() {
        // Required no-arg constructor for DynamoDB mapper
    }

    @DynamoDbPartitionKey
    @DynamoDbAttribute("jobId")
    @DynamoDbConvertedBy(JobId.JobIdConverter.class)
    public JobId getJobId() {
        return jobId;
    }

    public void setJobId(JobId jobId) {
        this.jobId = jobId;
    }

    @DynamoDbSortKey
    @DynamoDbAttribute("timestamp")
    @DynamoDbConvertedBy(LocalDateTimeConverter.class)
    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }


    @DynamoDbAttribute("listingId")
    public Integer getListingId() {
        return listingId;
    }

    public void setListingId(Integer listingId) {
        this.listingId = listingId;
    }

    @DynamoDbAttribute("schemaType")
    @DynamoDbConvertedBy(SchemaTypeConverter.class)
    public SchemaType getSchemaType() {
        return schemaType;
    }

    public void setSchemaType(SchemaType schemaType) {
        this.schemaType = schemaType;
    }

    @DynamoDbAttribute("status")
    @DynamoDbConvertedBy(TransformationStatusConverter.class)
    public TransformationStatus getStatus() {
        return status;
    }

    public void setStatus(TransformationStatus status) {
        this.status = status;
    }

    @DynamoDbAttribute("createdAt")
    @DynamoDbConvertedBy(LocalDateTimeConverter.class)
    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    @DynamoDbAttribute("processedAt")
    @DynamoDbConvertedBy(LocalDateTimeConverter.class)
    public LocalDateTime getProcessedAt() {
        return processedAt;
    }

    public void setProcessedAt(LocalDateTime processedAt) {
        this.processedAt = processedAt;
    }

    @DynamoDbAttribute("errorMessage")
    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @DynamoDbAttribute("expiresAt")
    public Long getExpiresAt() {
        return expiresAt;
    }

    public void setExpiresAt(Long expiresAt) {
        this.expiresAt = expiresAt;
    }

    public static class SchemaTypeConverter implements AttributeConverter<SchemaType> {
        @Override
        public AttributeValue transformFrom(SchemaType input) {
            if (input == null) {
                return AttributeValue.builder().nul(true).build();
            }
            return AttributeValue.builder().s(input.getIdentifier()).build();
        }

        @Override
        public SchemaType transformTo(AttributeValue input) {
            if (input == null || input.nul() != null && input.nul()) {
                return null;
            }
            return SchemaType.findById(input.s());
        }

        @Override
        public EnhancedType<SchemaType> type() {
            return EnhancedType.of(SchemaType.class);
        }

        @Override
        public AttributeValueType attributeValueType() {
            return AttributeValueType.S;
        }
    }

    /**
     * Converter for LocalDateTime to/from DynamoDB (stored as epoch seconds).
     */
    public static class LocalDateTimeConverter implements AttributeConverter<LocalDateTime> {
        @Override
        public AttributeValue transformFrom(LocalDateTime input) {
            if (input == null) {
                return AttributeValue.builder().nul(true).build();
            }
            return AttributeValue.builder()
                    .n(String.valueOf(input.toEpochSecond(ZoneOffset.UTC)))
                    .build();
        }

        @Override
        public LocalDateTime transformTo(AttributeValue input) {
            if (input == null || input.nul() != null && input.nul()) {
                return null;
            }
            return LocalDateTime.ofEpochSecond(Long.parseLong(input.n()), 0, ZoneOffset.UTC);
        }

        @Override
        public EnhancedType<LocalDateTime> type() {
            return EnhancedType.of(LocalDateTime.class);
        }

        @Override
        public AttributeValueType attributeValueType() {
            return AttributeValueType.N;
        }
    }

    /**
     * Converter for TransformationStatus enum to/from DynamoDB (stored as string).
     */
    public static class TransformationStatusConverter implements AttributeConverter<TransformationStatus> {
        @Override
        public AttributeValue transformFrom(TransformationStatus input) {
            if (input == null) {
                return AttributeValue.builder().nul(true).build();
            }
            return AttributeValue.builder().s(input.name()).build();
        }

        @Override
        public TransformationStatus transformTo(AttributeValue input) {
            if (input == null || input.nul() != null && input.nul()) {
                return null;
            }
            return TransformationStatus.valueOf(input.s());
        }

        @Override
        public EnhancedType<TransformationStatus> type() {
            return EnhancedType.of(TransformationStatus.class);
        }

        @Override
        public AttributeValueType attributeValueType() {
            return AttributeValueType.S;
        }
    }
}

package group.gnometrading.quality.model;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import software.amazon.awssdk.enhanced.dynamodb.AttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.AttributeValueType;
import software.amazon.awssdk.enhanced.dynamodb.EnhancedType;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbConvertedBy;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * DynamoDB bean for quality issue records.
 *
 * Table schema:
 * - PK: listingId (number)
 * - SK: issueId (string) - composite key: "{timestamp_epoch}#{ruleType}" for uniqueness per minute per rule
 * - ruleType: QualityRuleType enum
 * - status: QualityIssueStatus enum
 * - timestamp: LocalDateTime - the minute being checked
 * - s3Key: String - the S3 key of the file that triggered the issue
 * - details: String - human-readable summary of what was found
 * - recordCount: Integer - number of records in the file
 * - note: String (nullable) - reviewer notes
 * - createdAt: LocalDateTime
 */
@DynamoDbBean
public final class QualityIssue {

    private Integer listingId;
    private String issueId;
    private QualityRuleType ruleType;
    private QualityIssueStatus status;
    private LocalDateTime timestamp;
    private String s3Key;
    private String details;
    private Integer recordCount;
    private String note;
    private LocalDateTime createdAt;

    public QualityIssue() {}

    @DynamoDbPartitionKey
    @DynamoDbAttribute("listingId")
    public Integer getListingId() {
        return listingId;
    }

    public void setListingId(Integer listingId) {
        this.listingId = listingId;
    }

    @DynamoDbSortKey
    @DynamoDbAttribute("issueId")
    public String getIssueId() {
        return issueId;
    }

    public void setIssueId(String issueId) {
        this.issueId = issueId;
    }

    @DynamoDbAttribute("ruleType")
    @DynamoDbConvertedBy(QualityRuleTypeConverter.class)
    public QualityRuleType getRuleType() {
        return ruleType;
    }

    public void setRuleType(QualityRuleType ruleType) {
        this.ruleType = ruleType;
    }

    @DynamoDbAttribute("status")
    @DynamoDbConvertedBy(QualityIssueStatusConverter.class)
    public QualityIssueStatus getStatus() {
        return status;
    }

    public void setStatus(QualityIssueStatus status) {
        this.status = status;
    }

    @DynamoDbAttribute("timestamp")
    @DynamoDbConvertedBy(LocalDateTimeConverter.class)
    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    @DynamoDbAttribute("s3Key")
    public String getS3Key() {
        return s3Key;
    }

    public void setS3Key(String s3Key) {
        this.s3Key = s3Key;
    }

    @DynamoDbAttribute("details")
    public String getDetails() {
        return details;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    @DynamoDbAttribute("recordCount")
    public Integer getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(Integer recordCount) {
        this.recordCount = recordCount;
    }

    @DynamoDbAttribute("note")
    public String getNote() {
        return note;
    }

    public void setNote(String note) {
        this.note = note;
    }

    @DynamoDbAttribute("createdAt")
    @DynamoDbConvertedBy(LocalDateTimeConverter.class)
    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public static final class QualityIssueStatusConverter implements AttributeConverter<QualityIssueStatus> {
        @Override
        public AttributeValue transformFrom(QualityIssueStatus input) {
            if (input == null) {
                return AttributeValue.builder().nul(true).build();
            }
            return AttributeValue.builder().s(input.name()).build();
        }

        @Override
        public QualityIssueStatus transformTo(AttributeValue input) {
            if (input == null || input.nul() != null && input.nul()) {
                return null;
            }
            return QualityIssueStatus.valueOf(input.s());
        }

        @Override
        public EnhancedType<QualityIssueStatus> type() {
            return EnhancedType.of(QualityIssueStatus.class);
        }

        @Override
        public AttributeValueType attributeValueType() {
            return AttributeValueType.S;
        }
    }

    public static final class QualityRuleTypeConverter implements AttributeConverter<QualityRuleType> {
        @Override
        public AttributeValue transformFrom(QualityRuleType input) {
            if (input == null) {
                return AttributeValue.builder().nul(true).build();
            }
            return AttributeValue.builder().s(input.name()).build();
        }

        @Override
        public QualityRuleType transformTo(AttributeValue input) {
            if (input == null || input.nul() != null && input.nul()) {
                return null;
            }
            return QualityRuleType.valueOf(input.s());
        }

        @Override
        public EnhancedType<QualityRuleType> type() {
            return EnhancedType.of(QualityRuleType.class);
        }

        @Override
        public AttributeValueType attributeValueType() {
            return AttributeValueType.S;
        }
    }

    public static final class LocalDateTimeConverter implements AttributeConverter<LocalDateTime> {
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
}

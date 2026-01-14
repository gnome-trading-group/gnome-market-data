package group.gnometrading.gap;

import software.amazon.awssdk.enhanced.dynamodb.AttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.AttributeValueType;
import software.amazon.awssdk.enhanced.dynamodb.EnhancedType;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.*;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * DynamoDB bean for gap records.
 * 
 * Table schema:
 * - PK: listingId (number)
 * - SK: timestamp (LocalDateTime) - the timestamp of the missing minute
 * - gapReason: GapReason enum
 * - expected: boolean
 * - note: String (nullable)
 * - createdAt: LocalDateTime
 */
@DynamoDbBean
public class Gap {

    private Integer listingId;
    private LocalDateTime timestamp;
    private GapReason gapReason;
    private boolean expected;
    private String note;
    private LocalDateTime createdAt;

    public Gap() {
        // Required no-arg constructor for DynamoDB mapper
    }

    @DynamoDbPartitionKey
    @DynamoDbAttribute("listingId")
    public Integer getListingId() {
        return listingId;
    }

    public void setListingId(Integer listingId) {
        this.listingId = listingId;
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

    @DynamoDbAttribute("gapReason")
    @DynamoDbConvertedBy(GapReasonConverter.class)
    public GapReason getGapReason() {
        return gapReason;
    }

    public void setGapReason(GapReason gapReason) {
        this.gapReason = gapReason;
    }

    @DynamoDbAttribute("expected")
    public boolean isExpected() {
        return expected;
    }

    public void setExpected(boolean expected) {
        this.expected = expected;
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

    public static class GapReasonConverter implements AttributeConverter<GapReason> {
        @Override
        public AttributeValue transformFrom(GapReason input) {
            if (input == null) {
                return AttributeValue.builder().nul(true).build();
            }
            return AttributeValue.builder().s(input.name()).build();
        }

        @Override
        public GapReason transformTo(AttributeValue input) {
            if (input == null || input.nul() != null && input.nul()) {
                return null;
            }
            return GapReason.valueOf(input.s());
        }

        @Override
        public EnhancedType<GapReason> type() {
            return EnhancedType.of(GapReason.class);
        }

        @Override
        public AttributeValueType attributeValueType() {
            return AttributeValueType.S;
        }
    }

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
}


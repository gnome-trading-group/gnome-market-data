package group.gnometrading.transformer;

import group.gnometrading.schemas.SchemaType;
import software.amazon.awssdk.enhanced.dynamodb.AttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.AttributeValueType;
import software.amazon.awssdk.enhanced.dynamodb.EnhancedType;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public record JobId(
        int listingId,
        SchemaType schemaType
) {
    public String toString() {
        return listingId + "-" + schemaType.getIdentifier();
    }

    public static JobId fromString(String jobId) {
        String[] parts = jobId.split("-", 2);
        return new JobId(Integer.parseInt(parts[0]), SchemaType.findById(parts[1]));
    }

    public static class JobIdConverter implements AttributeConverter<JobId> {
        @Override
        public AttributeValue transformFrom(JobId input) {
            if (input == null) {
                return AttributeValue.builder().nul(true).build();
            }
            return AttributeValue.builder().s(input.toString()).build();
        }

        @Override
        public JobId transformTo(AttributeValue input) {
            if (input == null || input.nul() != null && input.nul()) {
                return null;
            }
            return JobId.fromString(input.s());
        }

        @Override
        public EnhancedType<JobId> type() {
            return EnhancedType.of(JobId.class);
        }

        @Override
        public AttributeValueType attributeValueType() {
            return AttributeValueType.S;
        }
    }
}

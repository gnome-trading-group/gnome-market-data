package group.gnometrading;

import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Lightweight dependency injection container for Lambda functions.
 * Uses singleton pattern to ensure AWS clients are reused across Lambda invocations
 * within the same execution environment, improving performance and reducing cold start overhead.
 */
public class Dependencies {
    private static volatile Dependencies instance;
    
    private final S3Client s3Client;
    private final DynamoDbClient dynamoDbClient;
    private final ObjectMapper objectMapper;

    private final String rawBucketName;
    private final String mergedBucketName;
    private final String finalBucketName;
    private final String gapsTableName;
    private final String transformJobsTableName;
    
    /**
     * Private constructor for singleton pattern.
     * Initializes all AWS clients and reads environment variables.
     */
    private Dependencies() {
        this.s3Client = S3Client.create();
        this.dynamoDbClient = DynamoDbClient.create();
        this.objectMapper = new ObjectMapper();

        this.rawBucketName = System.getenv("RAW_BUCKET_NAME");
        this.mergedBucketName = System.getenv("MERGED_BUCKET_NAME");
        this.finalBucketName = System.getenv("FINAL_BUCKET_NAME");
        this.gapsTableName = System.getenv("GAPS_TABLE_NAME");
        this.transformJobsTableName = System.getenv("TRANSFORM_JOBS_TABLE_NAME");
    }
    
    /**
     * Constructor for testing with custom dependencies.
     * This constructor is package-private to allow testing while preventing
     * external instantiation.
     */
    Dependencies(
            S3Client s3Client,
            DynamoDbClient dynamoDbClient,
            ObjectMapper objectMapper,
            String rawBucketName,
            String mergedBucketName,
            String finalBucketName,
            String gapsTableName,
            String transformJobsTableName
    ) {
        this.s3Client = s3Client;
        this.dynamoDbClient = dynamoDbClient;
        this.objectMapper = objectMapper;
        this.rawBucketName = rawBucketName;
        this.mergedBucketName = mergedBucketName;
        this.finalBucketName = finalBucketName;
        this.gapsTableName = gapsTableName;
        this.transformJobsTableName = transformJobsTableName;
    }
    
    /**
     * Get the singleton instance of Dependencies.
     * Uses double-checked locking for thread-safe lazy initialization.
     * 
     * @return the singleton Dependencies instance
     */
    public static Dependencies getInstance() {
        if (instance == null) {
            synchronized (Dependencies.class) {
                if (instance == null) {
                    instance = new Dependencies();
                }
            }
        }
        return instance;
    }
    
    public S3Client getS3Client() {
        return s3Client;
    }
    
    public DynamoDbClient getDynamoDbClient() {
        return dynamoDbClient;
    }
    
    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }
    
    public String getRawBucketName() {
        return rawBucketName;
    }
    
    public String getMergedBucketName() {
        return mergedBucketName;
    }
    
    public String getFinalBucketName() {
        return finalBucketName;
    }
    
    public String getGapsTableName() {
        return gapsTableName;
    }
    
    public String getTransformJobsTableName() {
        return transformJobsTableName;
    }
}


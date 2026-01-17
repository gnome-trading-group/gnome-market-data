package group.gnometrading;

import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.constants.Stage;
import group.gnometrading.coverage.CoverageRecord;
import group.gnometrading.gap.Gap;
import group.gnometrading.resources.Properties;
import group.gnometrading.transformer.TransformationJob;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Clock;

/**
 * Lightweight dependency injection container for Lambda functions.
 * Uses singleton pattern to ensure AWS clients are reused across Lambda invocations
 * within the same execution environment, improving performance and reducing cold start overhead.
 */
public class Dependencies {
    private static volatile Dependencies instance;

    private final Stage stage;
    private final Properties properties;
    private final S3Client s3Client;
    private final DynamoDbEnhancedClient dynamoDbEnhancedClient;
    private final DynamoDbClient dynamoDbClient;
    private final ObjectMapper objectMapper;
    private final Clock clock;
    private final DynamoDbTable<TransformationJob> transformJobsTable;
    private final DynamoDbTable<Gap> gapsTable;
    private final DynamoDbTable<CoverageRecord> coverageTable;
    private final SecurityMaster securityMaster;

    private final String rawBucketName;
    private final String mergedBucketName;
    private final String finalBucketName;
    private final String gapsTableName;
    private final String transformJobsTableName;
    private final String coverageTableName;
    private final String metadataBucketName;
    
    /**
     * Private constructor for singleton pattern.
     * Initializes all AWS clients and reads environment variables.
     */
    private Dependencies() {
        this.stage = Stage.fromStageName(System.getenv("STAGE"));
        this.properties = createProperties();
        this.securityMaster = createSecurityMaster();
        this.s3Client = S3Client.create();
        this.dynamoDbEnhancedClient = DynamoDbEnhancedClient.create();
        this.dynamoDbClient = DynamoDbClient.create();
        this.objectMapper = new ObjectMapper();
        this.clock = Clock.systemUTC();

        this.rawBucketName = System.getenv("RAW_BUCKET_NAME");
        this.mergedBucketName = System.getenv("MERGED_BUCKET_NAME");
        this.finalBucketName = System.getenv("FINAL_BUCKET_NAME");
        this.gapsTableName = System.getenv("GAPS_TABLE_NAME");
        this.transformJobsTableName = System.getenv("TRANSFORM_JOBS_TABLE_NAME");
        this.coverageTableName = System.getenv("COVERAGE_TABLE_NAME");
        this.metadataBucketName = System.getenv("METADATA_BUCKET_NAME");

        this.transformJobsTable = dynamoDbEnhancedClient.table(transformJobsTableName, TableSchema.fromBean(TransformationJob.class));
        this.gapsTable = dynamoDbEnhancedClient.table(gapsTableName, TableSchema.fromBean(Gap.class));
        this.coverageTable = dynamoDbEnhancedClient.table(coverageTableName, TableSchema.fromBean(CoverageRecord.class));
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
    
    public DynamoDbEnhancedClient getDynamoDbEnhancedClient() {
        return dynamoDbEnhancedClient;
    }

    public DynamoDbClient getDynamoDbClient() {
        return dynamoDbClient;
    }

    public SecurityMaster getSecurityMaster() {
        return securityMaster;
    }
    
    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public Clock getClock() {
        return clock;
    }

    public DynamoDbTable<TransformationJob> getTransformJobsTable() {
        return transformJobsTable;
    }

    public DynamoDbTable<Gap> getGapsTable() {
        return gapsTable;
    }

    public DynamoDbTable<CoverageRecord> getCoverageTable() {
        return coverageTable;
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

    public String getCoverageTableName() {
        return coverageTableName;
    }

    public String getMetadataBucketName() {
        return metadataBucketName;
    }

    private Properties createProperties() {
        try {
            return new Properties("market-data.%s.properties".formatted(stage.getStageName()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private SecurityMaster createSecurityMaster() {
        final String url = this.properties.getStringProperty("registry.url");
        final String apiKey = this.properties.getStringProperty("registry.api_key");
        return new SecurityMaster(new RegistryConnection(url, apiKey));
    }
}


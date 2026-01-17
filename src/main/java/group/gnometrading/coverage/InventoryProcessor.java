package group.gnometrading.coverage;

import com.amazonaws.services.lambda.runtime.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.example.data.Group;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Processes S3 inventory Parquet files and writes coverage data to DynamoDB.
 */
public class InventoryProcessor {
    
    private final S3Client s3Client;
    private final DynamoDbTable<CoverageRecord> coverageTable;
    private final DynamoDbEnhancedClient dynamoDbClient;
    private final String metadataBucket;
    
    public InventoryProcessor(
        S3Client s3Client,
        DynamoDbEnhancedClient dynamoDbClient,
        DynamoDbTable<CoverageRecord> coverageTable,
        String metadataBucket
    ) {
        this.s3Client = s3Client;
        this.coverageTable = coverageTable;
        this.dynamoDbClient = dynamoDbClient;
        this.metadataBucket = metadataBucket;
    }
    
    public void processInventoryFile(String inventoryKey, Context context) {
        context.getLogger().log("Processing inventory file: " + inventoryKey);
        
        try {
            List<InventoryRecord> records = readParquetFile(inventoryKey, context);

            CoverageAggregator aggregator = new CoverageAggregator();
            aggregator.aggregate(records);
            
            List<CoverageRecord> coverageRecords = aggregator.buildCoverageRecords();
            context.getLogger().log("Built " + coverageRecords.size() + " coverage records");
            
            batchWriteToDynamoDB(coverageRecords, context);
            
            context.getLogger().log("Successfully processed inventory file");
        } catch (Exception e) {
            context.getLogger().log("Error processing inventory: " + e.getMessage());
            throw new RuntimeException("Failed to process inventory file", e);
        }
    }
    
    private List<InventoryRecord> readParquetFile(String key, Context context) throws IOException {
        List<InventoryRecord> records = new ArrayList<>();
        
        File tempFile = File.createTempFile("inventory-", ".parquet");
        tempFile.deleteOnExit();

        context.getLogger().log("Downloading Parquet file from S3: " + key);
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(metadataBucket)
                .key(key)
                .build();

        var s3Object = s3Client.getObject(getObjectRequest);
        Files.copy(s3Object, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

        context.getLogger().log("Downloaded to temp file: " + tempFile.getAbsolutePath());

        Configuration conf = new Configuration();
        Path path = new Path(tempFile.toURI());

        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
                .withConf(conf)
                .build()) {

            Group group;
            while ((group = reader.read()) != null) {
                // S3 inventory columns: Bucket, Key, Size, LastModifiedDate, ETag, etc.
                String objectKey = group.getString("Key", 0);
                long size = group.getLong("Size", 0);

                records.add(new InventoryRecord(objectKey, size));
            }
        }
        context.getLogger().log("Parsed " + records.size() + " records from Parquet file");

        return records;
    }
    
    private void batchWriteToDynamoDB(List<CoverageRecord> records, Context context) {
        int batchSize = 25; // DynamoDB batch write limit
        int totalBatches = (int) Math.ceil((double) records.size() / batchSize);
        
        context.getLogger().log("Writing " + records.size() + " records in " + totalBatches + " batches");
        
        for (int i = 0; i < records.size(); i += batchSize) {
            int end = Math.min(i + batchSize, records.size());
            List<CoverageRecord> batch = records.subList(i, end);

            WriteBatch.Builder<CoverageRecord> batchBuilder = WriteBatch.builder(CoverageRecord.class)
                    .mappedTableResource(coverageTable);

            for (CoverageRecord record : batch) {
                batchBuilder.addPutItem(record);
            }

            BatchWriteItemEnhancedRequest batchRequest = BatchWriteItemEnhancedRequest.builder()
                    .writeBatches(batchBuilder.build())
                    .build();

            dynamoDbClient.batchWriteItem(batchRequest);
            context.getLogger().log("Wrote batch " + ((i / batchSize) + 1) + " of " + totalBatches);
        }

        context.getLogger().log("Completed writing all records to DynamoDB");
    }
}


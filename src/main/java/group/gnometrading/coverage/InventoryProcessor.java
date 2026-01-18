package group.gnometrading.coverage;

import com.amazonaws.services.lambda.runtime.Context;
import com.opencsv.CSVReader;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Processes S3 inventory CSV files and writes coverage data to DynamoDB.
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
            List<InventoryRecord> records = readCsvFile(inventoryKey, context);

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
    
    private List<InventoryRecord> readCsvFile(String key, Context context) {
        List<InventoryRecord> records = new ArrayList<>();

        context.getLogger().log("Downloading CSV file from S3: " + key);
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(metadataBucket)
                .key(key)
                .build();

        var s3Object = s3Client.getObject(getObjectRequest);

        try (GZIPInputStream gzipInputStream = new GZIPInputStream(s3Object);
             CSVReader reader = new CSVReader(new InputStreamReader(gzipInputStream))) {
            List<String[]> lines = reader.readAll();
            for (String[] line : lines) {
                if (line.length < 5) {
                    continue;
                }

                String objectKey = line[1];
                long size = 0;
                if (line.length >= 6) {
                    size = Long.parseLong(line[5]);
                }

                records.add(new InventoryRecord(objectKey, size));
            }
        } catch (Exception e) {
            context.getLogger().log("Error reading CSV file: " + e.getMessage());
            throw new RuntimeException("Failed to read CSV file", e);
        }

        context.getLogger().log("Parsed " + records.size() + " records from CSV file");
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


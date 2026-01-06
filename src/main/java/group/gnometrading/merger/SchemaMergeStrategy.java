package group.gnometrading.merger;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import group.gnometrading.schemas.Schema;

import java.util.List;
import java.util.Map;

public interface SchemaMergeStrategy {

    /**
     * Merges the records for a given schema type into a consolidated output.
     * @return the list of merged records
     */
    List<Schema> mergeRecords(LambdaLogger logger, Map<String, List<Schema>> entries);
}


package group.gnometrading.common;

import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.data.StreamEntry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Runs an SQS batch of S3 notifications as one unit of work per distinct key, and reports the messages behind every
 * unit that failed as the batch's item failures (the event source's {@code reportBatchItemFailures}): only those are
 * retried, and after the queue's receive limit they go to its dead-letter queue alone.
 *
 * <p>Several messages can name files of the same unit (raw copies of one minute), so a failed unit fails every
 * message that contributed to it.
 */
public final class SqsBatch {

    private static final Logger logger = LogManager.getLogger(SqsBatch.class);

    private SqsBatch() {}

    /**
     * @param unitOf the unit a message's entry belongs to (the entry itself, or e.g. the minute a raw file merges into)
     * @param parallel whether units run in parallel
     * @param work processes one unit; throwing fails it
     */
    public static <K> SQSBatchResponse forEachUnit(
            SQSEvent event,
            ObjectMapper objectMapper,
            Function<StreamEntry, K> unitOf,
            boolean parallel,
            Consumer<K> work) {
        Map<K, Set<String>> messagesByUnit = new LinkedHashMap<>();
        for (SQSEvent.SQSMessage message : event.getRecords()) {
            for (StreamEntry entry : S3Utils.extractKeysFromS3Event(message, objectMapper)) {
                messagesByUnit
                        .computeIfAbsent(unitOf.apply(entry), k -> new LinkedHashSet<>())
                        .add(message.getMessageId());
            }
        }
        logger.info("{} messages name {} units", event.getRecords().size(), messagesByUnit.size());

        Set<String> failed = ConcurrentHashMap.newKeySet();
        var units = parallel ? messagesByUnit.entrySet().parallelStream() : messagesByUnit.entrySet().stream();
        units.forEach(unit -> {
            try {
                work.accept(unit.getKey());
            } catch (RuntimeException e) {
                logger.error("Failed {}: {}", unit.getKey(), causeChain(e));
                failed.addAll(unit.getValue());
            }
        });

        List<SQSBatchResponse.BatchItemFailure> failures = new ArrayList<>();
        for (String messageId : failed) {
            failures.add(new SQSBatchResponse.BatchItemFailure(messageId));
        }
        if (!failures.isEmpty()) {
            logger.warn(
                    "{} of {} messages failed and will be retried",
                    failures.size(),
                    event.getRecords().size());
        }
        return new SQSBatchResponse(failures);
    }

    private static String causeChain(Throwable error) {
        StringBuilder chain = new StringBuilder(String.valueOf(error));
        for (Throwable cause = error.getCause(); cause != null && cause != error; cause = cause.getCause()) {
            chain.append(" <- ").append(cause);
        }
        return chain.toString();
    }
}

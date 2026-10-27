package group.gnometrading.collection;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import group.gnometrading.collection.aws.DynamoStore;
import group.gnometrading.collection.aws.EcsServices;
import group.gnometrading.collection.aws.EventBridgeAlerts;
import group.gnometrading.collection.aws.RegistryAdapter;
import group.gnometrading.collection.aws.S3Statuses;
import group.gnometrading.collection.aws.ServiceConfigSettings;
import group.gnometrading.collection.reconcile.Reconciler;
import group.gnometrading.common.Dependencies;
import java.security.SecureRandom;
import java.time.Clock;
import java.util.HexFormat;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The collection reconciler. Its FIFO queue (one message group, concurrency 1) carries wake-ups: rule changes,
 * {@code POST /collection/triggers}, later session leases, and a tick every minute from a schedule. Whatever woke it, a
 * batch is one full tick, so a burst of events costs one tick and a lost one at most a minute. A tick that throws fails
 * the batch, which the queue redelivers; the next tick finishes what it left.
 */
public final class ReconcilerLambdaHandler implements RequestHandler<SQSEvent, Void> {

    private static final Logger LOGGER = LogManager.getLogger(ReconcilerLambdaHandler.class);
    private static final Map<String, String> REGION_PREFIXES =
            Map.of("us-east-1", "use1", "ap-northeast-1", "apne1", "eu-west-1", "euw1");

    private final Reconciler reconciler;
    private final Clock clock;

    public ReconcilerLambdaHandler() {
        this(Dependencies.getInstance());
    }

    ReconcilerLambdaHandler(final Dependencies dependencies) {
        this.clock = dependencies.getClock();
        final SecureRandom random = new SecureRandom();
        final Supplier<String> latestVersion = () -> dependencies
                .getSsmClient()
                .getParameter(request -> request.name("/gnome/orchestrator/latest-version"))
                .parameter()
                .value();
        this.reconciler = new Reconciler(
                new DynamoStore(dependencies.getDynamoDbClient(), dependencies.getCollectionTables()),
                new RegistryAdapter(dependencies.getSecurityMaster()),
                new S3Statuses(
                        dependencies.getS3Client(),
                        dependencies.getMetadataBucketName(),
                        dependencies.getObjectMapper(),
                        clock),
                new EcsServices(dependencies::getEcsClient),
                new EventBridgeAlerts(dependencies.getEventBridgeClient(), dependencies.getObjectMapper(), clock),
                new ServiceConfigSettings(dependencies.getServiceConfig(), latestVersion),
                partition -> REGION_PREFIXES.getOrDefault(partition.region(), partition.region()) + "-"
                        + HexFormat.of().toHexDigits((byte) random.nextInt())
                        + HexFormat.of().toHexDigits((short) random.nextInt()));
    }

    @Override
    public Void handleRequest(final SQSEvent event, final Context context) {
        LOGGER.info(
                "Tick for {} wake-ups",
                event.getRecords() == null ? 0 : event.getRecords().size());
        final long nowNanos = clock.millis() * 1_000_000L;
        reconciler.tick(nowNanos);
        return null;
    }
}

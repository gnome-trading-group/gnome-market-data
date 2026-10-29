package group.gnometrading.collection.aws;

import group.gnometrading.collection.model.Interval;
import group.gnometrading.collection.model.Lease;
import group.gnometrading.collection.model.Partition;
import group.gnometrading.collection.model.Priority;
import group.gnometrading.collection.model.Rule;
import group.gnometrading.collection.model.RuleStatus;
import group.gnometrading.collection.model.ShardRecord;
import group.gnometrading.collection.model.Target;
import group.gnometrading.collection.ports.Store;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

/**
 * The control plane's tables in DynamoDB. A shard's item is also its assignment, read by the shard
 * (gnome-orchestrator {@code DynamoAssignment}): {@code streams}, {@code draining}, {@code publish} and {@code cuts} are big-endian int32s packed into
 * one binary attribute each, {@code accounts} a map of source to label. A coverage interval carries
 * {@code open_shard_id} only while open, so a shard's open intervals are one query of that index. The reconciler's own
 * state (alerts it left firing) is the rules table's {@code _reconciler} row, which is never a rule. A lease's
 * {@code streams} are packed like a shard's; the lease API writes leases and the reconciler only releases and extends
 * them.
 */
public final class DynamoStore implements Store {

    private static final Logger LOGGER = LogManager.getLogger(DynamoStore.class);
    private static final String RECONCILER_ROW = "_reconciler";

    /** The table names, from the Lambda's environment. */
    public record Tables(String rules, String targets, String shards, String coverage, String gaps, String leases) {}

    /**
     * A released lease stays readable this long (GET by its session), then DynamoDB's TTL removes it. The lease API's
     * RELEASED_TTL_S; lease-constants.json (test resources) holds the value both sides test against.
     */
    public static final long RELEASED_TTL_SECONDS = TimeUnit.DAYS.toSeconds(7);

    private final DynamoDbClient dynamo;
    private final Tables tables;

    public DynamoStore(final DynamoDbClient dynamo, final Tables tables) {
        this.dynamo = dynamo;
        this.tables = tables;
    }

    // ----- rules -----

    @Override
    public List<Rule> activeRules() {
        final List<Rule> rules = new ArrayList<>();
        for (Map<String, AttributeValue> item : scan(tables.rules())) {
            if (!"ACTIVE".equals(string(item, "status"))) {
                continue;
            }
            final Rule rule = RuleItems.parse(item);
            if (rule == null) {
                LOGGER.warn(
                        "Skipping rule {}: unknown rule_type {}", string(item, "rule_id"), string(item, "rule_type"));
                continue;
            }
            rules.add(rule);
        }
        return rules;
    }

    @Override
    public void removeEventIds(final String ruleId, final Set<Integer> eventIds) {
        try {
            dynamo.updateItem(UpdateItemRequest.builder()
                    .tableName(tables.rules())
                    .key(Map.of("rule_id", AttributeValue.fromS(ruleId)))
                    .conditionExpression("attribute_exists(rule_id)")
                    .updateExpression("DELETE event_ids :ids")
                    .expressionAttributeValues(Map.of(
                            ":ids",
                            AttributeValue.fromNs(
                                    eventIds.stream().map(String::valueOf).toList())))
                    .build());
        } catch (ConditionalCheckFailedException deleted) {
            LOGGER.info("Rule {} was deleted before its resolved events were removed", ruleId);
        }
    }

    @Override
    public void putRuleStatus(final String ruleId, final RuleStatus status) {
        try {
            dynamo.updateItem(UpdateItemRequest.builder()
                    .tableName(tables.rules())
                    .key(Map.of("rule_id", AttributeValue.fromS(ruleId)))
                    .conditionExpression("attribute_exists(rule_id)")
                    .updateExpression("SET resolved_events = :events, resolved_streams = :streams, "
                            + "waiting_events = :waiting, capped = :capped, resolved_at_ns = :at")
                    .expressionAttributeValues(Map.of(
                            ":events", AttributeValue.fromN(Integer.toString(status.events())),
                            ":streams", AttributeValue.fromN(Integer.toString(status.streams())),
                            ":waiting", AttributeValue.fromN(Integer.toString(status.waitingEvents())),
                            ":capped", AttributeValue.fromBool(status.capped()),
                            ":at", AttributeValue.fromN(Long.toString(status.resolvedAtNanos()))))
                    .build());
        } catch (ConditionalCheckFailedException deleted) {
            LOGGER.info("Rule {} was deleted before its status was written", ruleId);
        }
    }

    @Override
    public Map<String, String> firingAlerts() {
        final Map<String, AttributeValue> item = dynamo.getItem(request -> request.tableName(tables.rules())
                        .key(Map.of("rule_id", AttributeValue.fromS(RECONCILER_ROW)))
                        .consistentRead(true))
                .item();
        final Map<String, String> firing = new HashMap<>();
        if (item != null && item.containsKey("firing_alerts")) {
            item.get("firing_alerts").m().forEach((key, type) -> firing.put(key, type.s()));
        }
        return firing;
    }

    @Override
    public void putFiringAlerts(final Map<String, String> typeByKey) {
        final Map<String, AttributeValue> firing = new HashMap<>();
        typeByKey.forEach((key, type) -> firing.put(key, AttributeValue.fromS(type)));
        dynamo.putItem(PutItemRequest.builder()
                .tableName(tables.rules())
                .item(Map.of(
                        "rule_id", AttributeValue.fromS(RECONCILER_ROW),
                        "status", AttributeValue.fromS("INTERNAL"),
                        "firing_alerts", AttributeValue.fromM(firing)))
                .build());
    }

    // ----- leases -----

    @Override
    public List<Lease> leases() {
        final List<Lease> leases = new ArrayList<>();
        for (Map<String, AttributeValue> item : scan(tables.leases())) {
            final Lease lease = toLease(item);
            if (lease != null) {
                leases.add(lease);
            }
        }
        return leases;
    }

    @Override
    public void releaseLease(final String sessionId, final String reason, final long nowSeconds) {
        try {
            dynamo.updateItem(UpdateItemRequest.builder()
                    .tableName(tables.leases())
                    .key(Map.of("lease_id", AttributeValue.fromS(sessionId)))
                    .conditionExpression("#status = :active")
                    .updateExpression(
                            "SET #status = :released, released_at = :now, release_reason = :reason, " + "#ttl = :ttl")
                    .expressionAttributeNames(Map.of("#status", "status", "#ttl", "ttl"))
                    .expressionAttributeValues(Map.of(
                            ":active", AttributeValue.fromS("ACTIVE"),
                            ":released", AttributeValue.fromS("RELEASED"),
                            ":now", AttributeValue.fromN(Long.toString(nowSeconds)),
                            ":reason", AttributeValue.fromS(reason),
                            ":ttl", AttributeValue.fromN(Long.toString(nowSeconds + RELEASED_TTL_SECONDS))))
                    .build());
        } catch (ConditionalCheckFailedException gone) {
            LOGGER.info("Lease {} was released or deleted before this tick released it", sessionId);
        }
    }

    @Override
    public void recordLeaseSession(
            final String sessionId, final String sessionStatus, final long seenAtSeconds, final long expiresAtSeconds) {
        try {
            dynamo.updateItem(UpdateItemRequest.builder()
                    .tableName(tables.leases())
                    .key(Map.of("lease_id", AttributeValue.fromS(sessionId)))
                    .conditionExpression("#status = :active")
                    .updateExpression("SET session_status = :session, session_seen_at = :seen, expires_at = :expires")
                    .expressionAttributeNames(Map.of("#status", "status"))
                    .expressionAttributeValues(Map.of(
                            ":active", AttributeValue.fromS("ACTIVE"),
                            ":session", AttributeValue.fromS(sessionStatus),
                            ":seen", AttributeValue.fromN(Long.toString(seenAtSeconds)),
                            ":expires", AttributeValue.fromN(Long.toString(expiresAtSeconds))))
                    .build());
        } catch (ConditionalCheckFailedException gone) {
            LOGGER.info("Lease {} was released before this tick recorded its session", sessionId);
        }
    }

    /** Null (logged) for an item the lease API wouldn't write, so one bad item can't stop every tick. */
    static Lease toLease(final Map<String, AttributeValue> item) {
        try {
            if (string(item, "lease_id") == null) {
                throw new IllegalArgumentException("no lease_id");
            }
            return new Lease(
                    string(item, "lease_id"),
                    Set.copyOf(unpack(item.get("streams"))),
                    Priority.valueOf(string(item, "priority")),
                    string(item, "tier"),
                    string(item, "mode"),
                    Lease.Status.valueOf(string(item, "status")),
                    number(item, "created_at"),
                    number(item, "expires_at"),
                    number(item, "released_at"),
                    string(item, "session_status"),
                    number(item, "session_seen_at"));
        } catch (RuntimeException malformed) {
            LOGGER.warn("Skipping malformed lease {}: {}", string(item, "lease_id"), malformed.toString());
            return null;
        }
    }

    // ----- targets -----

    @Override
    public Map<Integer, Target> targets() {
        final Map<Integer, Target> targets = new HashMap<>();
        for (Map<String, AttributeValue> item : scan(tables.targets())) {
            final Target target = toTarget(item);
            targets.put(target.streamId(), target);
        }
        return targets;
    }

    @Override
    public void putTarget(final Target target) {
        dynamo.putItem(PutItemRequest.builder()
                .tableName(tables.targets())
                .item(fromTarget(target))
                .build());
    }

    @Override
    public void deleteTarget(final int streamId) {
        dynamo.deleteItem(request -> request.tableName(tables.targets())
                .key(Map.of("stream_id", AttributeValue.fromN(Integer.toString(streamId)))));
    }

    static Target toTarget(final Map<String, AttributeValue> item) {
        final AttributeValue wantedBy = item.get("wanted_by");
        return new Target(
                (int) number(item, "stream_id"),
                string(item, "source"),
                new Partition(
                        string(item, "region"),
                        string(item, "source_kind"),
                        Priority.valueOf(string(item, "priority")),
                        string(item, "tier")),
                wantedBy == null ? Set.of() : Set.copyOf(wantedBy.ss()),
                item.containsKey("retire_grace_minutes") ? (int) number(item, "retire_grace_minutes") : null,
                Target.State.valueOf(string(item, "state")),
                string(item, "shard_id"),
                number(item, "retire_after_ns"),
                number(item, "uncovered_since_ns"),
                string(item, "uncovered_reason"),
                string(item, "draining_from_shard_id"),
                number(item, "handoff_covered_since_ns"),
                (int) number(item, "cut_minute"));
    }

    static Map<String, AttributeValue> fromTarget(final Target target) {
        final Map<String, AttributeValue> item = new HashMap<>();
        item.put("stream_id", AttributeValue.fromN(Integer.toString(target.streamId())));
        item.put("source", AttributeValue.fromS(target.source()));
        item.put("region", AttributeValue.fromS(target.partition().region()));
        item.put("source_kind", AttributeValue.fromS(target.partition().sourceKind()));
        item.put("priority", AttributeValue.fromS(target.partition().priority().name()));
        item.put("tier", AttributeValue.fromS(target.partition().tier()));
        // DynamoDB refuses an empty set: a target nobody wants any more has none.
        if (!target.wantedBy().isEmpty()) {
            item.put("wanted_by", AttributeValue.fromSs(List.copyOf(target.wantedBy())));
        }
        putNumber(item, "retire_grace_minutes", target.retireGraceMinutes());
        item.put("state", AttributeValue.fromS(target.state().name()));
        putString(item, "shard_id", target.shardId());
        putNumber(item, "retire_after_ns", target.retireAfterNanos());
        putNumber(item, "uncovered_since_ns", target.uncoveredSinceNanos());
        putString(item, "uncovered_reason", target.uncoveredReason());
        putString(item, "draining_from_shard_id", target.drainingFromShardId());
        putNumber(item, "handoff_covered_since_ns", target.handoffCoveredSinceNanos());
        putNumber(item, "cut_minute", target.cutMinute());
        return item;
    }

    // ----- shards -----

    @Override
    public List<ShardRecord> shards() {
        final List<ShardRecord> shards = new ArrayList<>();
        for (Map<String, AttributeValue> item : scan(tables.shards())) {
            final ShardRecord shard = toShard(item);
            if (shard.state() != ShardRecord.State.DELETED) {
                shards.add(shard);
            }
        }
        return shards;
    }

    @Override
    public boolean putShard(final ShardRecord shard, final long expectedGeneration) {
        final PutItemRequest.Builder request =
                PutItemRequest.builder().tableName(tables.shards()).item(fromShard(shard));
        if (expectedGeneration < 0) {
            request.conditionExpression("attribute_not_exists(shard_id)");
        } else {
            request.conditionExpression("generation = :expected")
                    .expressionAttributeValues(
                            Map.of(":expected", AttributeValue.fromN(Long.toString(expectedGeneration))));
        }
        try {
            dynamo.putItem(request.build());
            return true;
        } catch (ConditionalCheckFailedException moved) {
            return false;
        }
    }

    static ShardRecord toShard(final Map<String, AttributeValue> item) {
        final Map<String, String> accounts = new HashMap<>();
        final AttributeValue bound = item.get("accounts");
        if (bound != null && bound.hasM()) {
            bound.m().forEach((source, label) -> accounts.put(source, label.s()));
        }
        return new ShardRecord(
                string(item, "shard_id"),
                new Partition(
                        string(item, "region"),
                        string(item, "source_kind"),
                        Priority.valueOf(string(item, "priority")),
                        string(item, "tier")),
                number(item, "generation"),
                unpack(item.get("streams")),
                unpack(item.get("draining")),
                Map.copyOf(accounts),
                unpackCuts(item.get("cuts")),
                unpack(item.get("publish")),
                string(item, "version"),
                ShardRecord.State.valueOf(string(item, "state")),
                number(item, "created_at_ns"),
                number(item, "empty_since_ns"),
                number(item, "mergeable_since_ns"));
    }

    static Map<String, AttributeValue> fromShard(final ShardRecord shard) {
        final Map<String, AttributeValue> item = new HashMap<>();
        item.put("shard_id", AttributeValue.fromS(shard.shardId()));
        item.put("region", AttributeValue.fromS(shard.partition().region()));
        item.put("source_kind", AttributeValue.fromS(shard.partition().sourceKind()));
        item.put("priority", AttributeValue.fromS(shard.partition().priority().name()));
        item.put("tier", AttributeValue.fromS(shard.partition().tier()));
        item.put("generation", AttributeValue.fromN(Long.toString(shard.generation())));
        item.put("streams", AttributeValue.fromB(pack(shard.streams())));
        item.put("draining", AttributeValue.fromB(pack(shard.draining())));
        final Map<String, AttributeValue> accounts = new HashMap<>();
        shard.accounts().forEach((source, label) -> accounts.put(source, AttributeValue.fromS(label)));
        item.put("accounts", AttributeValue.fromM(accounts));
        item.put("cuts", AttributeValue.fromB(packCuts(shard.cuts())));
        item.put("publish", AttributeValue.fromB(pack(shard.publish())));
        putString(item, "version", shard.version());
        item.put("state", AttributeValue.fromS(shard.state().name()));
        item.put("created_at_ns", AttributeValue.fromN(Long.toString(shard.createdAtNanos())));
        item.put("empty_since_ns", AttributeValue.fromN(Long.toString(shard.emptySinceNanos())));
        item.put("mergeable_since_ns", AttributeValue.fromN(Long.toString(shard.mergeableSinceNanos())));
        return item;
    }

    static SdkBytes pack(final List<Integer> ids) {
        final ByteBuffer bytes = ByteBuffer.allocate(ids.size() * Integer.BYTES);
        ids.forEach(bytes::putInt);
        return SdkBytes.fromByteArray(bytes.array());
    }

    /** Cuts as (stream id, minute) int32 pairs, packed like the stream lists. */
    static SdkBytes packCuts(final Map<Integer, Integer> cuts) {
        final ByteBuffer bytes = ByteBuffer.allocate(cuts.size() * 2 * Integer.BYTES);
        cuts.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(cut -> {
            bytes.putInt(cut.getKey());
            bytes.putInt(cut.getValue());
        });
        return SdkBytes.fromByteArray(bytes.array());
    }

    static Map<Integer, Integer> unpackCuts(final AttributeValue value) {
        final List<Integer> pairs = unpack(value);
        final Map<Integer, Integer> cuts = new HashMap<>();
        for (int i = 0; i + 1 < pairs.size(); i += 2) {
            cuts.put(pairs.get(i), pairs.get(i + 1));
        }
        return Map.copyOf(cuts);
    }

    static List<Integer> unpack(final AttributeValue value) {
        if (value == null || value.b() == null) {
            return List.of();
        }
        final ByteBuffer bytes = value.b().asByteBuffer();
        final List<Integer> ids = new ArrayList<>(bytes.remaining() / Integer.BYTES);
        while (bytes.remaining() >= Integer.BYTES) {
            ids.add(bytes.getInt());
        }
        return List.copyOf(ids);
    }

    // ----- coverage and gaps -----

    @Override
    public List<Interval> openIntervals(final String shardId) {
        final List<Interval> intervals = new ArrayList<>();
        Map<String, AttributeValue> start = null;
        do {
            final var response = dynamo.query(QueryRequest.builder()
                    .tableName(tables.coverage())
                    .indexName("open_shard_id-index")
                    .keyConditionExpression("open_shard_id = :shard")
                    .expressionAttributeValues(Map.of(":shard", AttributeValue.fromS(shardId)))
                    .exclusiveStartKey(start)
                    .build());
            response.items().forEach(item -> intervals.add(toInterval(item)));
            start = response.hasLastEvaluatedKey()
                            && !response.lastEvaluatedKey().isEmpty()
                    ? response.lastEvaluatedKey()
                    : null;
        } while (start != null);
        return intervals;
    }

    @Override
    public void putInterval(final Interval interval) {
        dynamo.putItem(PutItemRequest.builder()
                .tableName(tables.coverage())
                .item(fromInterval(interval))
                .build());
    }

    @Override
    public void putGap(final int streamId, final long fromNanos, final long toNanos, final String reason) {
        dynamo.putItem(PutItemRequest.builder()
                .tableName(tables.gaps())
                .item(Map.of(
                        "stream_id", AttributeValue.fromN(Integer.toString(streamId)),
                        "from_ns", AttributeValue.fromN(Long.toString(fromNanos)),
                        "to_ns", AttributeValue.fromN(Long.toString(toNanos)),
                        "reason", AttributeValue.fromS(reason == null ? "UNKNOWN" : reason)))
                .build());
    }

    static Interval toInterval(final Map<String, AttributeValue> item) {
        return new Interval(
                (int) number(item, "stream_id"),
                number(item, "from_ns"),
                number(item, "to_ns"),
                string(item, "shard_id"),
                string(item, "task_id"),
                item.containsKey("open_shard_id"),
                string(item, "close_reason"));
    }

    static Map<String, AttributeValue> fromInterval(final Interval interval) {
        final Map<String, AttributeValue> item = new HashMap<>();
        item.put("stream_id", AttributeValue.fromN(Integer.toString(interval.streamId())));
        item.put("from_ns", AttributeValue.fromN(Long.toString(interval.fromNanos())));
        item.put("to_ns", AttributeValue.fromN(Long.toString(interval.toNanos())));
        item.put("shard_id", AttributeValue.fromS(interval.shardId()));
        item.put("task_id", AttributeValue.fromS(interval.taskId()));
        if (interval.open()) {
            item.put("open_shard_id", AttributeValue.fromS(interval.shardId()));
        }
        putString(item, "close_reason", interval.closeReason());
        return item;
    }

    // ----- helpers -----

    private List<Map<String, AttributeValue>> scan(final String table) {
        final List<Map<String, AttributeValue>> items = new ArrayList<>();
        Map<String, AttributeValue> start = null;
        do {
            final var response = dynamo.scan(ScanRequest.builder()
                    .tableName(table)
                    .consistentRead(true)
                    .exclusiveStartKey(start)
                    .build());
            items.addAll(response.items());
            start = response.hasLastEvaluatedKey()
                            && !response.lastEvaluatedKey().isEmpty()
                    ? response.lastEvaluatedKey()
                    : null;
        } while (start != null);
        return items;
    }

    private static String string(final Map<String, AttributeValue> item, final String name) {
        final AttributeValue value = item.get(name);
        return value == null ? null : value.s();
    }

    private static long number(final Map<String, AttributeValue> item, final String name) {
        final AttributeValue value = item.get(name);
        return value == null || value.n() == null ? 0 : Long.parseLong(value.n());
    }

    private static void putString(final Map<String, AttributeValue> item, final String name, final String value) {
        if (value != null) {
            item.put(name, AttributeValue.fromS(value));
        }
    }

    private static void putNumber(final Map<String, AttributeValue> item, final String name, final Number value) {
        if (value != null) {
            item.put(name, AttributeValue.fromN(value.toString()));
        }
    }
}

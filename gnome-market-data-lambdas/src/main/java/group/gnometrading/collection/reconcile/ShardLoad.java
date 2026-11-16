package group.gnometrading.collection.reconcile;

import group.gnometrading.collection.model.Partition;
import java.util.HashMap;
import java.util.Map;

/**
 * What placement counts on one shard: its streams per source, how many it publishes and their summed records per
 * second (draining ones included, as the shard still collects and publishes them), and the account each signed source
 * is bound to. Placement adds to it as it assigns.
 */
public final class ShardLoad {

    private final String shardId;
    private final Partition partition;
    private final boolean accepting;
    private final Map<String, String> accounts;
    private final Map<String, Integer> streamsBySource = new HashMap<>();
    private int streams;
    private int published;
    private double recordsPerSecond;

    /** @param accepting whether new streams may go on it: false while it drains or is replaced in a roll */
    public ShardLoad(
            final String shardId,
            final Partition partition,
            final boolean accepting,
            final Map<String, String> accounts) {
        this.shardId = shardId;
        this.partition = partition;
        this.accepting = accepting;
        this.accounts = new HashMap<>(accounts);
    }

    public String shardId() {
        return shardId;
    }

    public Partition partition() {
        return partition;
    }

    public boolean accepting() {
        return accepting;
    }

    /** Source to account label; a signed source gets one the first time a stream of it is placed here. */
    public Map<String, String> accounts() {
        return accounts;
    }

    public int streams() {
        return streams;
    }

    public int streams(final String source) {
        return streamsBySource.getOrDefault(source, 0);
    }

    public int published() {
        return published;
    }

    public double recordsPerSecond() {
        return recordsPerSecond;
    }

    /**
     * Counts a stream already on the shard, or one placement just put there.
     *
     * @param isPublished whether a session leases it, so the shard offers it over Aeron
     */
    public void add(final String source, final boolean isPublished, final double rate) {
        streamsBySource.merge(source, 1, Integer::sum);
        streams++;
        if (isPublished) {
            published++;
        }
        recordsPerSecond += rate;
    }

    void bind(final String source, final String label) {
        accounts.put(source, label);
    }
}

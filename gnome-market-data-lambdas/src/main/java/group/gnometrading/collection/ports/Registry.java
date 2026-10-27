package group.gnometrading.collection.ports;

import group.gnometrading.collection.model.Account;
import group.gnometrading.collection.model.SourceLimits;
import group.gnometrading.collection.model.StreamInfo;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/** What the registry says about streams, sources and market-data accounts. */
public interface Registry {
    Map<Integer, StreamInfo> streams(Collection<Integer> streamIds);

    Map<String, SourceLimits> sources();

    Map<String, List<Account>> accounts();
}

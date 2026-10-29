package group.gnometrading.collection.reconcile;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.collection.aws.DynamoStore;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** The lease timings the lease API (common layer {@code leases.py}) also uses; its tests read the same file. */
class LeaseConstantsTest {

    @Test
    void theReconcilerAgreesWithTheLeaseApi() throws Exception {
        JsonNode constants;
        try (InputStream in = getClass().getResourceAsStream("/lease-constants.json")) {
            constants = new ObjectMapper().readTree(in);
        }

        assertEquals(constants.get("lifetime_seconds").asLong(), LeaseStep.LIFETIME_SECONDS);
        assertEquals(constants.get("released_ttl_seconds").asLong(), DynamoStore.RELEASED_TTL_SECONDS);
        assertEquals(
                TimeUnit.SECONDS.toNanos(constants.get("status_fresh_seconds").asLong()),
                CoverageStep.STATUS_FRESH_NANOS);
    }
}

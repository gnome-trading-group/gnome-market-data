package group.gnometrading.migration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;

class LegacyKeysTest {

    @Test
    void legacyAggregatedKeysParseToTheirListingAndMinute() {
        final LegacyKeys.Aggregated file =
                LegacyKeys.aggregated("97983/5/2026/8/24/0/7/mbp-10.zst").orElseThrow();

        assertEquals(5, file.exchangeId());
        assertEquals(97_983L, file.securityId());
        assertEquals(LocalDateTime.of(2026, 8, 24, 0, 7), file.minute());
        assertEquals("mbp-10", file.record());
    }

    @Test
    void streamKeyedKeysAreNotLegacy() {
        assertTrue(LegacyKeys.aggregated("7/book/mbp-10/2026/08/24/00/07.zst").isEmpty());
        assertFalse(LegacyKeys.isRaw("7/book/mbp-10/2026/08/24/00/07/abcd.zst"));
        assertFalse(LegacyKeys.isRaw("7/raw/2026/08/24/00/07/abcd.zst"));
    }

    @Test
    void legacyRawKeysAreRecognisedAndAreNotAggregated() {
        final String raw = "1116373/5/2026/10/2/15/17/mbp-10/7787bafc.zst";

        assertTrue(LegacyKeys.isRaw(raw));
        assertTrue(LegacyKeys.aggregated(raw).isEmpty());
    }
}

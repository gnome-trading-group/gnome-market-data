package group.gnometrading.inventory;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class CoverageAggregatorTest {

    @Test
    void aggregatesPerStreamAndSkipsKeysThatAreNotStreamFiles() {
        CoverageAggregator aggregator = new CoverageAggregator();

        aggregator.aggregate(List.of(
                new InventoryRecord("7/book/mbp-10/2026/04/05/09/07.zst", 100),
                new InventoryRecord("7/book/mbp-10/2026/04/05/09/08.zst", 50),
                new InventoryRecord("7/book/trades/2026/04/06/00/00.zst", 10),
                new InventoryRecord("9/book/mbp-10/2026/04/05/09/07.zst", 5),
                new InventoryRecord("532/151/2025/4/15/10/30/mbp-10.zst", 1),
                new InventoryRecord("7/book/mbp-10/2026/04/05/09/07/abcd.zst", 1)));

        Map<String, CoverageRecord> records = aggregator.buildCoverageRecords().stream()
                .collect(Collectors.toMap(r -> r.getPk() + "|" + r.getSk(), r -> r));
        assertEquals(2, aggregator.skippedKeys());
        assertEquals(
                List.of(
                        "GLOBAL|SUMMARY",
                        "STREAM#7|DATE#2026-04-05",
                        "STREAM#7|DATE#2026-04-06",
                        "STREAM#7|SUMMARY",
                        "STREAM#9|DATE#2026-04-05",
                        "STREAM#9|SUMMARY"),
                records.keySet().stream().sorted().toList());
        Map<String, Object> global = records.get("GLOBAL|SUMMARY").getData();
        assertEquals(4, global.get("totalFiles"));
        assertEquals(2, global.get("streamCount"));
        Map<String, Object> stream7 = records.get("STREAM#7|SUMMARY").getData();
        assertEquals(160L, stream7.get("totalSizeBytes"));
        assertEquals(2, stream7.get("totalDays"));
        assertEquals(List.of("mbp-10", "trades"), stream7.get("recordTypes"));
    }
}

package group.gnometrading.quality.rules.statistics;

import static org.junit.jupiter.api.Assertions.*;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.schemas.Schema;
import java.util.List;
import org.junit.jupiter.api.Test;

class QualityStatisticDefaultsTest {

    private static QualityStatistic makeStatistic(AnomalyDirection direction, double zThreshold, double fallback) {
        return new QualityStatistic() {
            @Override
            public String name() {
                return "test";
            }

            @Override
            public QualityRuleType ruleType() {
                return QualityRuleType.TICK_COUNT_ANOMALY;
            }

            @Override
            public double compute(MarketDataEntry e, List<Schema> r) {
                return 0;
            }

            @Override
            public AnomalyDirection anomalyDirection() {
                return direction;
            }

            @Override
            public double zThreshold() {
                return zThreshold;
            }

            @Override
            public double fallbackThreshold() {
                return fallback;
            }
        };
    }

    // --- detectionEnabled default ---

    @Test
    void testDetectionEnabledDefaultsFalse() {
        QualityStatistic s = makeStatistic(AnomalyDirection.LOW, 3.0, 0.10);
        assertFalse(s.detectionEnabled());
    }

    // --- LOW direction ---

    @Test
    void testLowAnomalousWhenBelowZThreshold() {
        QualityStatistic s = makeStatistic(AnomalyDirection.LOW, 3.0, 0.10);
        // z = (1000 - 50) / 100 = 9.5 > 3.0
        assertTrue(s.isAnomalous(50.0, 1000.0, 100.0));
    }

    @Test
    void testLowNotAnomalousWhenWithinZThreshold() {
        QualityStatistic s = makeStatistic(AnomalyDirection.LOW, 3.0, 0.10);
        // z = (1000 - 750) / 100 = 2.5 < 3.0
        assertFalse(s.isAnomalous(750.0, 1000.0, 100.0));
    }

    @Test
    void testLowFallbackAnomalousWhenStddevZero() {
        QualityStatistic s = makeStatistic(AnomalyDirection.LOW, 3.0, 0.10);
        // stddev=0, fallback: 5 < 1000 * 0.10 = 100
        assertTrue(s.isAnomalous(5.0, 1000.0, 0.0));
    }

    @Test
    void testLowFallbackNotAnomalousWhenStddevZeroAndAboveFloor() {
        QualityStatistic s = makeStatistic(AnomalyDirection.LOW, 3.0, 0.10);
        // stddev=0, fallback: 200 < 1000 * 0.10 = 100 → false
        assertFalse(s.isAnomalous(200.0, 1000.0, 0.0));
    }

    // --- HIGH direction ---

    @Test
    void testHighAnomalousWhenAboveZThreshold() {
        QualityStatistic s = makeStatistic(AnomalyDirection.HIGH, 4.0, 10.0);
        // z = (1000 - 100) / 10 = 90 > 4.0
        assertTrue(s.isAnomalous(1000.0, 100.0, 10.0));
    }

    @Test
    void testHighNotAnomalousWhenWithinZThreshold() {
        QualityStatistic s = makeStatistic(AnomalyDirection.HIGH, 4.0, 10.0);
        // z = (130 - 100) / 10 = 3.0 < 4.0
        assertFalse(s.isAnomalous(130.0, 100.0, 10.0));
    }

    @Test
    void testHighFallbackAnomalousWhenStddevZero() {
        QualityStatistic s = makeStatistic(AnomalyDirection.HIGH, 4.0, 10.0);
        // stddev=0, fallback: 1000 >= 100 * 10 = 1000 → true
        assertTrue(s.isAnomalous(1000.0, 100.0, 0.0));
    }

    @Test
    void testHighFallbackNotAnomalousWhenStddevZeroAndBelowCeiling() {
        QualityStatistic s = makeStatistic(AnomalyDirection.HIGH, 4.0, 10.0);
        // stddev=0, fallback: 500 >= 100 * 10 = 1000 → false
        assertFalse(s.isAnomalous(500.0, 100.0, 0.0));
    }

    // --- BOTH direction ---

    @Test
    void testBothAnomalousAboveMean() {
        QualityStatistic s = makeStatistic(AnomalyDirection.BOTH, 4.0, 0.0);
        // z = |125 - 100| / 5 = 5.0 > 4.0
        assertTrue(s.isAnomalous(125.0, 100.0, 5.0));
    }

    @Test
    void testBothAnomalousBelowMean() {
        QualityStatistic s = makeStatistic(AnomalyDirection.BOTH, 4.0, 0.0);
        // z = |75 - 100| / 5 = 5.0 > 4.0
        assertTrue(s.isAnomalous(75.0, 100.0, 5.0));
    }

    @Test
    void testBothNotAnomalousWithinThreshold() {
        QualityStatistic s = makeStatistic(AnomalyDirection.BOTH, 4.0, 0.0);
        // z = |110 - 100| / 5 = 2.0 < 4.0
        assertFalse(s.isAnomalous(110.0, 100.0, 5.0));
    }

    @Test
    void testBothFallbackReturnsFalseWhenStddevZero() {
        QualityStatistic s = makeStatistic(AnomalyDirection.BOTH, 4.0, 0.0);
        assertFalse(s.isAnomalous(200.0, 100.0, 0.0));
    }

    // --- Guard conditions ---

    @Test
    void testNotAnomalousWhenMeanIsZero() {
        QualityStatistic s = makeStatistic(AnomalyDirection.LOW, 3.0, 0.10);
        assertFalse(s.isAnomalous(0.0, 0.0, 0.0));
    }

    @Test
    void testNotAnomalousWhenMeanIsNegative() {
        QualityStatistic s = makeStatistic(AnomalyDirection.LOW, 3.0, 0.10);
        assertFalse(s.isAnomalous(0.0, -1.0, 0.0));
    }

    // --- describeAnomaly ---

    @Test
    void testDescribeAnomalyIncludesZScore() {
        QualityStatistic s = makeStatistic(AnomalyDirection.LOW, 3.0, 0.10);
        String desc = s.describeAnomaly(50.0, 1000.0, 100.0);
        assertTrue(desc.contains("9.5"), "Expected z-score in description: " + desc);
        assertTrue(desc.contains("below"), desc);
    }

    @Test
    void testDescribeAnomalyFallbackMentionsZeroVariance() {
        QualityStatistic s = makeStatistic(AnomalyDirection.LOW, 3.0, 0.10);
        String desc = s.describeAnomaly(5.0, 1000.0, 0.0);
        assertTrue(desc.contains("zero variance"), desc);
    }
}

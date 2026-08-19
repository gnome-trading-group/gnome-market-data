package group.gnometrading.gap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import group.gnometrading.quality.model.HourlyListingStatistic;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;

@ExtendWith(MockitoExtension.class)
class GapToleranceCalculatorTest {

    @Mock
    private DynamoDbTable<HourlyListingStatistic> statisticsTable;

    private final List<HourlyListingStatistic> queryResults = new ArrayList<>();
    private static final LocalDate DATE = LocalDate.of(2024, 1, 15);
    private static final int HOUR = 10;
    private static final int LISTING_ID = 42;

    @BeforeEach
    void setUp() {
        queryResults.clear();
        PageIterable<HourlyListingStatistic> mockPages = mock(PageIterable.class);
        SdkIterable<HourlyListingStatistic> mockItems = mock(SdkIterable.class);
        lenient().when(mockItems.iterator()).thenAnswer(inv -> queryResults.iterator());
        lenient().when(mockPages.items()).thenReturn(mockItems);
        lenient().when(statisticsTable.query(any(QueryConditional.class))).thenReturn(mockPages);
    }

    @Test
    void testNullStatisticsTableReturnsDefault() {
        GapToleranceCalculator calculator = new GapToleranceCalculator(null);
        assertEquals(
                GapToleranceCalculator.DEFAULT_TOLERANCE_MINUTES,
                calculator.computeToleranceMinutes(LISTING_ID, HOUR, DATE));
        verifyNoInteractions(statisticsTable);
    }

    @Test
    void testEmptyBaselineReturnsDefault() {
        GapToleranceCalculator calculator = new GapToleranceCalculator(statisticsTable);
        assertEquals(
                GapToleranceCalculator.DEFAULT_TOLERANCE_MINUTES,
                calculator.computeToleranceMinutes(LISTING_ID, HOUR, DATE));
    }

    @Test
    void testInsufficientSamplesReturnsDefault() {
        // 2 samples < MINIMUM_SAMPLES=3
        queryResults.add(buildStats(HOUR, "2024-01-13", 1.0, 500.0, 250000.0));
        queryResults.add(buildStats(HOUR, "2024-01-14", 1.0, 500.0, 250000.0));

        GapToleranceCalculator calculator = new GapToleranceCalculator(statisticsTable);
        assertEquals(
                GapToleranceCalculator.DEFAULT_TOLERANCE_MINUTES,
                calculator.computeToleranceMinutes(LISTING_ID, HOUR, DATE));
    }

    @Test
    void testHighFrequencyListingReturnsTolerance1() {
        // mean = 500 ticks/min: tolerance = ceil(1/500 * 3) = 1
        queryResults.add(buildStats(HOUR, "2024-01-10", 1.0, 500.0, 250000.0));
        queryResults.add(buildStats(HOUR, "2024-01-11", 1.0, 500.0, 250000.0));
        queryResults.add(buildStats(HOUR, "2024-01-12", 1.0, 500.0, 250000.0));

        GapToleranceCalculator calculator = new GapToleranceCalculator(statisticsTable);
        assertEquals(1, calculator.computeToleranceMinutes(LISTING_ID, HOUR, DATE));
    }

    @Test
    void testActiveMarketReturnsTolerance1() {
        // mean = 10 ticks/min: tolerance = ceil(1/10 * 3) = 1
        queryResults.add(buildStats(HOUR, "2024-01-10", 1.0, 10.0, 100.0));
        queryResults.add(buildStats(HOUR, "2024-01-11", 1.0, 10.0, 100.0));
        queryResults.add(buildStats(HOUR, "2024-01-12", 1.0, 10.0, 100.0));

        GapToleranceCalculator calculator = new GapToleranceCalculator(statisticsTable);
        assertEquals(1, calculator.computeToleranceMinutes(LISTING_ID, HOUR, DATE));
    }

    @Test
    void testModerateListingReturnsTolerance6() {
        // mean = 0.5 ticks/min: tolerance = ceil(2 * 3) = 6
        queryResults.add(buildStats(HOUR, "2024-01-10", 1.0, 0.5, 0.25));
        queryResults.add(buildStats(HOUR, "2024-01-11", 1.0, 0.5, 0.25));
        queryResults.add(buildStats(HOUR, "2024-01-12", 1.0, 0.5, 0.25));

        GapToleranceCalculator calculator = new GapToleranceCalculator(statisticsTable);
        assertEquals(6, calculator.computeToleranceMinutes(LISTING_ID, HOUR, DATE));
    }

    @Test
    void testSparseListingReturnsTolerance30() {
        // mean = 0.1 ticks/min: tolerance = ceil(10 * 3) = 30
        queryResults.add(buildStats(HOUR, "2024-01-10", 1.0, 0.1, 0.01));
        queryResults.add(buildStats(HOUR, "2024-01-11", 1.0, 0.1, 0.01));
        queryResults.add(buildStats(HOUR, "2024-01-12", 1.0, 0.1, 0.01));

        GapToleranceCalculator calculator = new GapToleranceCalculator(statisticsTable);
        assertEquals(30, calculator.computeToleranceMinutes(LISTING_ID, HOUR, DATE));
    }

    @Test
    void testVerySparseListing_cappedAtMax() {
        // mean = 0.001 ticks/min: uncapped would be ceil(1000 * 3) = 3000, capped at 1440
        queryResults.add(buildStats(HOUR, "2024-01-10", 1.0, 0.001, 0.000001));
        queryResults.add(buildStats(HOUR, "2024-01-11", 1.0, 0.001, 0.000001));
        queryResults.add(buildStats(HOUR, "2024-01-12", 1.0, 0.001, 0.000001));

        GapToleranceCalculator calculator = new GapToleranceCalculator(statisticsTable);
        assertEquals(
                GapToleranceCalculator.MAX_TOLERANCE_MINUTES,
                calculator.computeToleranceMinutes(LISTING_ID, HOUR, DATE));
    }

    @Test
    void testZeroMeanReturnsMax() {
        // sum=0 → mean=0 → returns MAX
        queryResults.add(buildStats(HOUR, "2024-01-10", 1.0, 0.0, 0.0));
        queryResults.add(buildStats(HOUR, "2024-01-11", 1.0, 0.0, 0.0));
        queryResults.add(buildStats(HOUR, "2024-01-12", 1.0, 0.0, 0.0));

        GapToleranceCalculator calculator = new GapToleranceCalculator(statisticsTable);
        assertEquals(
                GapToleranceCalculator.MAX_TOLERANCE_MINUTES,
                calculator.computeToleranceMinutes(LISTING_ID, HOUR, DATE));
    }

    @Test
    void testHighVarianceWidensTolerance() {
        // mean=1, stddev=5 → CV=5 → multiplier=max(3, 3 + 2*5)=13 → tolerance=ceil(1*13)=13
        // 3 rows each with count=1, sum=1, sumOfSquares=25 (so mean=1, stddev=sqrt(25-1)~=4.9)
        // To get a clean result: use 3 identical rows with high sumOfSquares
        // count=3, sum=3, sumOfSquares=75 → mean=1, variance=(75/3)-(3/3)^2=25-1=24, stddev~4.899
        // CV=4.899, multiplier=max(3, 3+9.8)=12.8, tolerance=ceil(1*12.8)=13
        queryResults.add(buildStats(HOUR, "2024-01-10", 1.0, 1.0, 25.0));
        queryResults.add(buildStats(HOUR, "2024-01-11", 1.0, 1.0, 25.0));
        queryResults.add(buildStats(HOUR, "2024-01-12", 1.0, 1.0, 25.0));

        GapToleranceCalculator calculator = new GapToleranceCalculator(statisticsTable);
        int tolerance = calculator.computeToleranceMinutes(LISTING_ID, HOUR, DATE);
        // Tolerance must be higher than the non-variance case (mean=1 → baseline tolerance=3)
        assertTrue(tolerance > 3, "High variance should widen tolerance beyond baseline");
    }

    private HourlyListingStatistic buildStats(int hour, String date, double count, double sum, double sumOfSquares) {
        HourlyListingStatistic stat = new HourlyListingStatistic();
        stat.setListingId(LISTING_ID);
        stat.setSk(HourlyListingStatistic.buildSk(hour, date, "tickCount"));
        stat.setCount(count);
        stat.setSum(sum);
        stat.setSumOfSquares(sumOfSquares);
        return stat;
    }
}

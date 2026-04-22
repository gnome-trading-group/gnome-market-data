package group.gnometrading.quality.rules;

import group.gnometrading.quality.model.HourlyListingStatistic;
import group.gnometrading.quality.rules.statistics.MidPriceStatistic;
import group.gnometrading.quality.rules.statistics.SpreadStatistic;
import group.gnometrading.quality.rules.statistics.TickCountStatistic;
import group.gnometrading.quality.rules.statistics.TradeFrequencyStatistic;
import group.gnometrading.quality.rules.statistics.TradeVolumeStatistic;
import group.gnometrading.quality.rules.statistics.VolatilityStatistic;
import java.util.ArrayList;
import java.util.List;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public final class QualityRuleFactory {

    private QualityRuleFactory() {}

    public static List<QualityRule> buildStaticOnly() {
        return List.of(new TimestampAlignmentRule(), new SequenceMonotonicityRule(), new BadDataFlagsRule());
    }

    public static List<QualityRule> buildAll(
            DynamoDbTable<HourlyListingStatistic> statisticsTable,
            DynamoDbClient dynamoDbClient,
            String statisticsTableName) {
        List<QualityRule> rules = new ArrayList<>(buildStaticOnly());
        rules.add(statisticalRule(statisticsTable, dynamoDbClient, statisticsTableName, true, true));
        return rules;
    }

    public static List<QualityRule> buildStatisticsOnly(
            DynamoDbTable<HourlyListingStatistic> statisticsTable,
            DynamoDbClient dynamoDbClient,
            String statisticsTableName) {
        return List.of(statisticalRule(statisticsTable, dynamoDbClient, statisticsTableName, true, false));
    }

    public static List<QualityRule> buildIssueDetection(
            DynamoDbTable<HourlyListingStatistic> statisticsTable,
            DynamoDbClient dynamoDbClient,
            String statisticsTableName) {
        List<QualityRule> rules = new ArrayList<>(buildStaticOnly());
        rules.add(statisticalRule(statisticsTable, dynamoDbClient, statisticsTableName, false, true));
        return rules;
    }

    private static StatisticalQualityRule statisticalRule(
            DynamoDbTable<HourlyListingStatistic> statisticsTable,
            DynamoDbClient dynamoDbClient,
            String statisticsTableName,
            boolean updateStatistics,
            boolean detectAnomalies) {
        return new StatisticalQualityRule(
                statisticsTable,
                dynamoDbClient,
                statisticsTableName,
                List.of(
                        new TickCountStatistic(),
                        new SpreadStatistic(),
                        new MidPriceStatistic(),
                        new TradeVolumeStatistic(),
                        new TradeFrequencyStatistic(),
                        new VolatilityStatistic()),
                updateStatistics,
                detectAnomalies);
    }
}

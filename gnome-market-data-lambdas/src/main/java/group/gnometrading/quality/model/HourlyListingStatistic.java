package group.gnometrading.quality.model;

import java.time.DayOfWeek;
import java.time.LocalDate;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

/**
 * DynamoDB bean for per-listing, per-hour, per-metric sufficient statistics.
 *
 * Table schema:
 * - PK: listingId (NUMBER)
 * - SK: "HH#YYYY-MM-DD#metricName" — zero-padded hour first so a range query on
 *   "HH#startDate#" .. "HH#endDate#~" is cleanly scoped to one hour across a date range
 * - count, sum, sumOfSquares: atomically incremented via DynamoDB ADD (no read step)
 */
@DynamoDbBean
public final class HourlyListingStatistic {

    private Integer listingId;
    private String sk;
    private Double count;
    private Double sum;
    private Double sumOfSquares;

    public HourlyListingStatistic() {}

    @DynamoDbPartitionKey
    @DynamoDbAttribute("listingId")
    public Integer getListingId() {
        return listingId;
    }

    public void setListingId(Integer listingId) {
        this.listingId = listingId;
    }

    @DynamoDbSortKey
    @DynamoDbAttribute("sk")
    public String getSk() {
        return sk;
    }

    public void setSk(String sk) {
        this.sk = sk;
    }

    @DynamoDbAttribute("count")
    public Double getCount() {
        return count;
    }

    public void setCount(Double count) {
        this.count = count;
    }

    @DynamoDbAttribute("sum")
    public Double getSum() {
        return sum;
    }

    public void setSum(Double sum) {
        this.sum = sum;
    }

    @DynamoDbAttribute("sumOfSquares")
    public Double getSumOfSquares() {
        return sumOfSquares;
    }

    public void setSumOfSquares(Double sumOfSquares) {
        this.sumOfSquares = sumOfSquares;
    }

    public String getMetric() {
        int last = sk.lastIndexOf('#');
        return sk.substring(last + 1);
    }

    public String getDate() {
        int firstHash = sk.indexOf('#');
        int lastHash = sk.lastIndexOf('#');
        return sk.substring(firstHash + 1, lastHash);
    }

    public DayOfWeek getDayOfWeek() {
        return LocalDate.parse(getDate()).getDayOfWeek();
    }

    public static boolean isWeekend(DayOfWeek dow) {
        return dow == DayOfWeek.SATURDAY || dow == DayOfWeek.SUNDAY;
    }

    public static String buildSk(int hour, String date, String metric) {
        return String.format("%02d#%s#%s", hour, date, metric);
    }
}

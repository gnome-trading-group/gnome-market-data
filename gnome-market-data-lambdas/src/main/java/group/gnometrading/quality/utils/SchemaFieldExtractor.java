package group.gnometrading.quality.utils;

import group.gnometrading.schemas.MboSchema;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.Mbp1Schema;
import group.gnometrading.schemas.RecordType;
import group.gnometrading.schemas.StreamRecord;
import group.gnometrading.schemas.book.Action;
import group.gnometrading.schemas.book.MboDecoder;
import group.gnometrading.schemas.book.Mbp10Decoder;
import group.gnometrading.schemas.book.Mbp1Decoder;
import java.util.Set;

public final class SchemaFieldExtractor {

    public static final Set<RecordType> TRADE_SCHEMAS = Set.of(RecordType.MBO, RecordType.MBP_1, RecordType.MBP_10);
    public static final Set<RecordType> BBO_SCHEMAS = Set.of(RecordType.MBP_1, RecordType.MBP_10);

    private SchemaFieldExtractor() {}

    public record TradeFields(long price, long size, Action action, long priceNullVal) {
        public boolean isPriceNull() {
            return price == priceNullVal;
        }
    }

    public record BboFields(long bidPrice, long askPrice, long bidPriceNullVal, long askPriceNullVal) {
        public boolean isBidPriceNull() {
            return bidPrice == bidPriceNullVal;
        }

        public boolean isAskPriceNull() {
            return askPrice == askPriceNullVal;
        }
    }

    public record FlagFields(boolean badData, boolean badTimestampRecv) {}

    public static TradeFields extractTradeFields(StreamRecord record, RecordType type) {
        return switch (type) {
            case MBO -> {
                MboSchema mbo = (MboSchema) record;
                yield new TradeFields(
                        mbo.decoder.price(), mbo.decoder.size(), mbo.decoder.action(), MboDecoder.priceNullValue());
            }
            case MBP_1 -> {
                Mbp1Schema mbp1 = (Mbp1Schema) record;
                yield new TradeFields(
                        mbp1.decoder.price(), mbp1.decoder.size(), mbp1.decoder.action(), Mbp1Decoder.priceNullValue());
            }
            case MBP_10 -> {
                Mbp10Schema mbp10 = (Mbp10Schema) record;
                yield new TradeFields(
                        mbp10.decoder.price(),
                        mbp10.decoder.size(),
                        mbp10.decoder.action(),
                        Mbp10Decoder.priceNullValue());
            }
            default -> null;
        };
    }

    public static BboFields extractBboFields(StreamRecord record, RecordType type) {
        return switch (type) {
            case MBP_1 -> {
                Mbp1Schema mbp1 = (Mbp1Schema) record;
                yield new BboFields(
                        mbp1.decoder.bidPrice0(),
                        mbp1.decoder.askPrice0(),
                        Mbp1Decoder.bidPrice0NullValue(),
                        Mbp1Decoder.askPrice0NullValue());
            }
            case MBP_10 -> {
                Mbp10Schema mbp10 = (Mbp10Schema) record;
                yield new BboFields(
                        mbp10.decoder.bidPrice0(),
                        mbp10.decoder.askPrice0(),
                        Mbp10Decoder.bidPrice0NullValue(),
                        Mbp10Decoder.askPrice0NullValue());
            }
            default -> null;
        };
    }

    public static FlagFields extractFlagFields(StreamRecord record, RecordType type) {
        return switch (type) {
            case MBO -> {
                MboSchema mbo = (MboSchema) record;
                yield new FlagFields(
                        mbo.decoder.flags().badData(), mbo.decoder.flags().badTimestampRecv());
            }
            case MBP_1 -> {
                Mbp1Schema mbp1 = (Mbp1Schema) record;
                yield new FlagFields(
                        mbp1.decoder.flags().badData(), mbp1.decoder.flags().badTimestampRecv());
            }
            case MBP_10 -> {
                Mbp10Schema mbp10 = (Mbp10Schema) record;
                yield new FlagFields(
                        mbp10.decoder.flags().badData(), mbp10.decoder.flags().badTimestampRecv());
            }
            default -> null;
        };
    }
}

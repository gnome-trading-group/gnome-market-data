import com.github.luben.zstd.ZstdInputStream;
import group.gnometrading.schemas.Mbp10Encoder;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.Statics;
import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/** Emits newline-delimited JSON for normalized MBP-10 acceptance checks. */
public final class NormalizedDataInspector {

    private NormalizedDataInspector() {}

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 1) {
            throw new IllegalArgumentException("Expected a directory containing zstd files");
        }
        List<Path> files;
        try (var paths = Files.walk(Path.of(arguments[0]))) {
            files = paths.filter(path -> path.toString().endsWith(".zst"))
                    .sorted(Comparator.comparing(Path::toString))
                    .toList();
        }
        Mbp10Schema schema = new Mbp10Schema();
        int messageSize = schema.totalMessageSize();
        for (Path file : files) {
            try {
                byte[] compressed = Files.readAllBytes(file);
                byte[] data;
                try (var input = new ZstdInputStream(new ByteArrayInputStream(compressed))) {
                    data = input.readAllBytes();
                }
                if (data.length % messageSize != 0) {
                    emitFileError();
                    continue;
                }
                for (int offset = 0; offset < data.length; offset += messageSize) {
                    schema.buffer.putBytes(0, data, offset, messageSize);
                    emitRecord(schema);
                }
            } catch (RuntimeException exception) {
                emitFileError();
            }
        }
    }

    private static void emitRecord(Mbp10Schema schema) {
        long bid = schema.decoder.bidPrice0();
        long ask = schema.decoder.askPrice0();
        boolean hasBid = bid != Mbp10Encoder.bidPrice0NullValue();
        boolean hasAsk = ask != Mbp10Encoder.askPrice0NullValue();
        boolean crossed = hasBid && hasAsk && bid > ask;
        boolean nonNullSequence = schema.decoder.sequence() != Mbp10Encoder.sequenceNullValue();
        short depth = schema.decoder.depth();
        boolean invalidDepth = depth != Mbp10Encoder.depthNullValue() && (depth < 0 || depth > 9);
        int populatedBidLevels = populatedBidLevels(schema);
        int populatedAskLevels = populatedAskLevels(schema);
        boolean invalidPrice = (hasBid && (bid < 0 || bid > Statics.PRICE_SCALING_FACTOR))
                || (hasAsk && (ask < 0 || ask > Statics.PRICE_SCALING_FACTOR));
        System.out.printf(
                Locale.ROOT,
                "{\"kind\":\"record\",\"eventTimestamp\":%d,\"receiveTimestamp\":%d,"
                        + "\"exchangeId\":%d,\"securityId\":%d,\"action\":\"%s\","
                        + "\"sequence\":%d,\"depth\":%d,\"invalidDepth\":%s,"
                        + "\"populatedBidLevels\":%d,\"populatedAskLevels\":%d,"
                        + "\"crossedBook\":%s,\"invalidPrice\":%s,\"nonNullSequence\":%s}%n",
                schema.decoder.timestampEvent(),
                schema.decoder.timestampRecv(),
                schema.decoder.exchangeId(),
                schema.decoder.securityId(),
                schema.decoder.action().name(),
                schema.decoder.sequence(),
                depth,
                invalidDepth,
                populatedBidLevels,
                populatedAskLevels,
                crossed,
                invalidPrice,
                nonNullSequence);
    }

    private static int populatedBidLevels(Mbp10Schema schema) {
        int populated = 0;
        populated += schema.decoder.bidPrice0() == Mbp10Encoder.bidPrice0NullValue() ? 0 : 1;
        populated += schema.decoder.bidPrice1() == Mbp10Encoder.bidPrice1NullValue() ? 0 : 1;
        populated += schema.decoder.bidPrice2() == Mbp10Encoder.bidPrice2NullValue() ? 0 : 1;
        populated += schema.decoder.bidPrice3() == Mbp10Encoder.bidPrice3NullValue() ? 0 : 1;
        populated += schema.decoder.bidPrice4() == Mbp10Encoder.bidPrice4NullValue() ? 0 : 1;
        populated += schema.decoder.bidPrice5() == Mbp10Encoder.bidPrice5NullValue() ? 0 : 1;
        populated += schema.decoder.bidPrice6() == Mbp10Encoder.bidPrice6NullValue() ? 0 : 1;
        populated += schema.decoder.bidPrice7() == Mbp10Encoder.bidPrice7NullValue() ? 0 : 1;
        populated += schema.decoder.bidPrice8() == Mbp10Encoder.bidPrice8NullValue() ? 0 : 1;
        populated += schema.decoder.bidPrice9() == Mbp10Encoder.bidPrice9NullValue() ? 0 : 1;
        return populated;
    }

    private static int populatedAskLevels(Mbp10Schema schema) {
        int populated = 0;
        populated += schema.decoder.askPrice0() == Mbp10Encoder.askPrice0NullValue() ? 0 : 1;
        populated += schema.decoder.askPrice1() == Mbp10Encoder.askPrice1NullValue() ? 0 : 1;
        populated += schema.decoder.askPrice2() == Mbp10Encoder.askPrice2NullValue() ? 0 : 1;
        populated += schema.decoder.askPrice3() == Mbp10Encoder.askPrice3NullValue() ? 0 : 1;
        populated += schema.decoder.askPrice4() == Mbp10Encoder.askPrice4NullValue() ? 0 : 1;
        populated += schema.decoder.askPrice5() == Mbp10Encoder.askPrice5NullValue() ? 0 : 1;
        populated += schema.decoder.askPrice6() == Mbp10Encoder.askPrice6NullValue() ? 0 : 1;
        populated += schema.decoder.askPrice7() == Mbp10Encoder.askPrice7NullValue() ? 0 : 1;
        populated += schema.decoder.askPrice8() == Mbp10Encoder.askPrice8NullValue() ? 0 : 1;
        populated += schema.decoder.askPrice9() == Mbp10Encoder.askPrice9NullValue() ? 0 : 1;
        return populated;
    }

    private static void emitFileError() {
        System.out.println("{\"kind\":\"fileError\"}");
    }
}

import group.gnometrading.codecs.json.JsonDecoder;
import group.gnometrading.gateways.inbound.JsonWebSocketReader;
import group.gnometrading.gateways.inbound.exchanges.polymarket.PolymarketSocketReader;
import group.gnometrading.logging.NullLogger;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.sequencer.GlobalSequence;
import group.gnometrading.sequencer.SequencedRingBuffer;
import group.gnometrading.sm.AssetClass;
import group.gnometrading.sm.ContractType;
import group.gnometrading.sm.Exchange;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.Security;
import group.gnometrading.sm.SecurityType;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** Replays GNOMERAW frames through the production Polymarket reader. */
public final class PolymarketReplayHarness {

    private static final byte[] MAGIC = "GNOMERAW".getBytes(StandardCharsets.US_ASCII);

    private PolymarketReplayHarness() {}

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 3) {
            throw new IllegalArgumentException("Expected capture path, token ID, and expected output count");
        }
        Path capture = Path.of(arguments[0]);
        String tokenId = arguments[1];
        int expectedOutputs = Integer.parseInt(arguments[2]);
        List<Mbp10Schema> captured = new CopyOnWriteArrayList<>();
        SequencedRingBuffer<Mbp10Schema> ringBuffer =
                new SequencedRingBuffer<>(Mbp10Schema::new, new GlobalSequence());
        ringBuffer.handleEventsWith((globalSequence, templateId, buffer, length) -> {
            Mbp10Schema copy = new Mbp10Schema();
            copy.buffer.putBytes(0, buffer, 0, length);
            copy.wrap(copy.buffer);
            captured.add(copy);
        });
        ringBuffer.start();

        Listing listing = new Listing(
                7,
                new Exchange(2, "Polymarket", "global", SchemaType.MBP_10),
                new Security(
                        3,
                        "REPLAY",
                        SecurityType.EVENT_CONTRACT,
                        ContractType.BINARY,
                        AssetClass.PREDICTION,
                        "",
                        "",
                        "",
                        false,
                        false,
                        0,
                        0,
                        true,
                        0),
                "condition-replay:" + tokenId,
                "REPLAY-YES");
        PolymarketSocketReader reader = new PolymarketSocketReader(
                new NullLogger(), ringBuffer, () -> 0L, null, listing, null, new JsonDecoder());
        reader.buffer = false;
        reader.pause = false;
        Method handle = JsonWebSocketReader.class.getDeclaredMethod("handleGatewayMessage", ByteBuffer.class);
        handle.setAccessible(true);

        try (var input = new DataInputStream(new BufferedInputStream(Files.newInputStream(capture)))) {
            byte[] magic = input.readNBytes(MAGIC.length);
            if (!java.util.Arrays.equals(MAGIC, magic) || input.readInt() != 1) {
                throw new IllegalArgumentException("Unsupported GNOMERAW header");
            }
            input.readInt();
            while (input.available() > 0) {
                long receiveTimestamp = input.readLong();
                int length = input.readInt();
                byte[] payload = input.readNBytes(length);
                if (payload.length != length) {
                    throw new IllegalArgumentException("Truncated replay payload");
                }
                reader.recvTimestamp = receiveTimestamp;
                handle.invoke(reader, ByteBuffer.wrap(payload));
            }
        }

        long deadline = System.currentTimeMillis() + 2_000;
        while (captured.size() < expectedOutputs && System.currentTimeMillis() < deadline) {
            Thread.yield();
        }
        ringBuffer.shutdown();
        emit(captured);
        if (captured.size() != expectedOutputs) {
            throw new IllegalStateException(
                    "Expected " + expectedOutputs + " output messages, received " + captured.size());
        }
    }

    private static void emit(List<Mbp10Schema> captured) throws Exception {
        List<String> actions = new ArrayList<>();
        List<Long> eventTimestamps = new ArrayList<>();
        List<Long> receiveTimestamps = new ArrayList<>();
        List<Long> bestBids = new ArrayList<>();
        List<Long> bestAsks = new ArrayList<>();
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        for (Mbp10Schema schema : captured) {
            actions.add(schema.decoder.action().name());
            eventTimestamps.add(schema.decoder.timestampEvent());
            receiveTimestamps.add(schema.decoder.timestampRecv());
            bestBids.add(schema.decoder.bidPrice0());
            bestAsks.add(schema.decoder.askPrice0());
            byte[] bytes = new byte[schema.totalMessageSize()];
            schema.buffer.getBytes(0, bytes);
            digest.update(bytes);
        }
        System.out.printf(
                java.util.Locale.ROOT,
                "{\"messageCount\":%d,\"actions\":%s,\"eventTimestamps\":%s,"
                        + "\"receiveTimestamps\":%s,\"bestBids\":%s,\"bestAsks\":%s,"
                        + "\"outputSha256\":\"%s\"}%n",
                captured.size(),
                jsonStrings(actions),
                eventTimestamps,
                receiveTimestamps,
                bestBids,
                bestAsks,
                HexFormat.of().formatHex(digest.digest()));
    }

    private static String jsonStrings(List<String> values) {
        return values.stream().map(value -> "\"" + value + "\"").toList().toString();
    }
}

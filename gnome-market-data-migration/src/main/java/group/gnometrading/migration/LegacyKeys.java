package group.gnometrading.migration;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Keys of the layout before streams: {security}/{exchange}/{Y}/{M}/{D}/{h}/{m}/{record}.zst in the merged and final
 * buckets, and the same minute plus /{record}/{uuid}.zst in the raw bucket. No stream-keyed key matches: those have
 * the family or "raw" second.
 */
final class LegacyKeys {

    private static final Pattern AGGREGATED =
            Pattern.compile("^(\\d+)/(\\d+)/(\\d{4})/(\\d{1,2})/(\\d{1,2})/(\\d{1,2})/(\\d{1,2})/([a-z0-9-]+)\\.zst$");
    private static final Pattern RAW =
            Pattern.compile("^\\d+/\\d+/\\d{4}/\\d{1,2}/\\d{1,2}/\\d{1,2}/\\d{1,2}/[a-z0-9-]+/[^/]+\\.zst$");
    private static final int SECURITY = 1;
    private static final int EXCHANGE = 2;
    private static final int YEAR = 3;
    private static final int MONTH = 4;
    private static final int DAY = 5;
    private static final int HOUR = 6;
    private static final int MINUTE = 7;
    private static final int RECORD = 8;

    private LegacyKeys() {}

    /** One legacy merged or final file's key, parsed. */
    record Aggregated(String key, int exchangeId, long securityId, LocalDateTime minute, String record) {}

    static Optional<Aggregated> aggregated(final String key) {
        final Matcher m = AGGREGATED.matcher(key);
        if (!m.matches()) {
            return Optional.empty();
        }
        return Optional.of(new Aggregated(
                key,
                Integer.parseInt(m.group(EXCHANGE)),
                Long.parseLong(m.group(SECURITY)),
                LocalDateTime.of(
                        Integer.parseInt(m.group(YEAR)),
                        Integer.parseInt(m.group(MONTH)),
                        Integer.parseInt(m.group(DAY)),
                        Integer.parseInt(m.group(HOUR)),
                        Integer.parseInt(m.group(MINUTE))),
                m.group(RECORD)));
    }

    static boolean isRaw(final String key) {
        return RAW.matcher(key).matches();
    }
}

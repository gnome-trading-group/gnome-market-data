package group.gnometrading.migration;

import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.SbeMessage;
import group.gnometrading.schemas.book.BookFlagsEncoder;
import group.gnometrading.schemas.book.Mbp10Encoder;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Converts market data stored by core schema v0 or v1 (book records in schema 1, keyed by exchange and security) to
 * the book family's records (schema 2, keyed by stream). Only MBP-10 was ever stored: gateways publish it, the merged
 * bucket holds nothing else, and the final bucket's other records are rebuilt from it by the transformer.
 *
 * <p>Fields share their names across the layouts, so they are copied by name through the generated codecs. What
 * differs: identity becomes the stream; v0 sizes were uint32 with their own null; the flag bits were renumbered and
 * maybeBadBook became badData.
 */
final class LegacyMbp10 {

    static final int LEGACY_SCHEMA_ID = 1;
    static final int LEGACY_MBP10_TEMPLATE_ID = 2;
    private static final int FIRST_LEGACY_BOOK_TEMPLATE = 1;
    private static final int LAST_LEGACY_BOOK_TEMPLATE = 9;
    private static final int LEVELS = 10;
    private static final long V0_SIZE_NULL = group.gnometrading.schemas.v0.Mbp10Decoder.sizeNullValue();
    private static final List<String> COPIED = copiedFields();
    private static final EncoderSetters ENCODER = new EncoderSetters();

    private final group.gnometrading.schemas.v0.Mbp10Decoder v0 = new group.gnometrading.schemas.v0.Mbp10Decoder();
    private final group.gnometrading.schemas.v1.Mbp10Decoder v1 = new group.gnometrading.schemas.v1.Mbp10Decoder();
    private final Accessors v0Accessors = new Accessors(group.gnometrading.schemas.v0.Mbp10Decoder.class);
    private final Accessors v1Accessors = new Accessors(group.gnometrading.schemas.v1.Mbp10Decoder.class);
    private final Mbp10Schema converted = new Mbp10Schema();

    /** Whether the message at {@code offset} is a book record of core schema v0 or v1. */
    static boolean isLegacyBook(final DirectBuffer buffer, final int offset) {
        final int templateId = SbeMessage.templateId(buffer, offset);
        return SbeMessage.schemaId(buffer, offset) == LEGACY_SCHEMA_ID
                && SbeMessage.version(buffer, offset) <= 1
                && templateId >= FIRST_LEGACY_BOOK_TEMPLATE
                && templateId <= LAST_LEGACY_BOOK_TEMPLATE;
    }

    /** The legacy message's length: its header and block (book records have no variable-length fields). */
    static int length(final DirectBuffer buffer, final int offset) {
        return SbeMessage.HEADER_LENGTH + SbeMessage.blockLength(buffer, offset);
    }

    /** The legacy book record's exchange id; every book template began with it. */
    int exchangeId(final DirectBuffer buffer, final int offset) {
        if (SbeMessage.version(buffer, offset) == 0) {
            return wrapV0(buffer, offset).exchangeId();
        }
        return wrapV1(buffer, offset).exchangeId();
    }

    /** The legacy book record's security id, which follows its exchange id. */
    long securityId(final DirectBuffer buffer, final int offset) {
        if (SbeMessage.version(buffer, offset) == 0) {
            return wrapV0(buffer, offset).securityId();
        }
        return wrapV1(buffer, offset).securityId();
    }

    private group.gnometrading.schemas.v0.Mbp10Decoder wrapV0(final DirectBuffer buffer, final int offset) {
        return this.v0.wrap(
                buffer,
                offset + SbeMessage.HEADER_LENGTH,
                SbeMessage.blockLength(buffer, offset),
                SbeMessage.version(buffer, offset));
    }

    private group.gnometrading.schemas.v1.Mbp10Decoder wrapV1(final DirectBuffer buffer, final int offset) {
        return this.v1.wrap(
                buffer,
                offset + SbeMessage.HEADER_LENGTH,
                SbeMessage.blockLength(buffer, offset),
                SbeMessage.version(buffer, offset));
    }

    /**
     * Writes the legacy MBP-10 at {@code offset} into {@code dst} as a book MBP-10 of {@code streamId}.
     *
     * @return the number of bytes written
     * @throws IllegalStateException for any other legacy book record, which no stored data holds
     */
    int convert(
            final DirectBuffer src,
            final int offset,
            final int streamId,
            final MutableDirectBuffer dst,
            final int dstOffset) {
        final int templateId = SbeMessage.templateId(src, offset);
        if (templateId != LEGACY_MBP10_TEMPLATE_ID) {
            throw new IllegalStateException("Legacy book template " + templateId + " at byte " + offset
                    + ": only MBP-10 is stored, so no conversion for it exists");
        }
        final Mbp10Encoder encoder = this.converted.encoder;
        encoder.streamId(streamId);
        if (SbeMessage.version(src, offset) == 0) {
            wrapV0(src, offset);
            copyFields(this.v0, this.v0Accessors, encoder, true);
            final group.gnometrading.schemas.v0.MarketUpdateFlagsDecoder flags = this.v0.flags();
            copyFlags(
                    encoder.flags(),
                    flags.maybeBadBook(),
                    flags.snapshot(),
                    flags.lastMessage(),
                    flags.badTimestampRecv(),
                    flags.marketByPrice(),
                    flags.topOfBook());
        } else {
            wrapV1(src, offset);
            copyFields(this.v1, this.v1Accessors, encoder, false);
            final group.gnometrading.schemas.v1.MarketUpdateFlagsDecoder flags = this.v1.flags();
            copyFlags(
                    encoder.flags(),
                    flags.maybeBadBook(),
                    flags.snapshot(),
                    flags.lastMessage(),
                    flags.badTimestampRecv(),
                    flags.marketByPrice(),
                    flags.topOfBook());
        }
        final int length = this.converted.totalMessageSize();
        dst.putBytes(dstOffset, this.converted.buffer, 0, length);
        return length;
    }

    /** The template id converted records carry. */
    int convertedTemplateId() {
        return this.converted.templateId();
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    private static void copyFlags(
            final BookFlagsEncoder flags,
            final boolean badData,
            final boolean snapshot,
            final boolean lastMessage,
            final boolean badTimestampRecv,
            final boolean marketByPrice,
            final boolean topOfBook) {
        flags.clear();
        flags.badData(badData);
        flags.snapshot(snapshot);
        flags.lastMessage(lastMessage);
        flags.badTimestampRecv(badTimestampRecv);
        flags.marketByPrice(marketByPrice);
        flags.topOfBook(topOfBook);
    }

    private static void copyFields(
            final Object decoder, final Accessors accessors, final Mbp10Encoder encoder, final boolean v0Sizes) {
        for (int i = 0; i < COPIED.size(); i++) {
            final String field = COPIED.get(i);
            Object value = accessors.get(i, decoder);
            if (v0Sizes && isSize(field) && (Long) value == V0_SIZE_NULL) {
                value = Mbp10Encoder.sizeNullValue();
            } else if (value instanceof Enum<?> legacy) {
                value = bookEnum(field, legacy.name());
            }
            ENCODER.set(i, encoder, value);
        }
    }

    private static Object bookEnum(final String field, final String name) {
        return switch (field) {
            case "action" -> group.gnometrading.schemas.book.Action.valueOf(name);
            case "side" -> group.gnometrading.schemas.book.Side.valueOf(name);
            default -> throw new IllegalStateException("No book enum for field " + field);
        };
    }

    private static boolean isSize(final String field) {
        return field.equals("size") || field.startsWith("bidSize") || field.startsWith("askSize");
    }

    private static List<String> copiedFields() {
        final List<String> fields = new ArrayList<>(List.of(
                "timestampEvent",
                "timestampSent",
                "timestampRecv",
                "price",
                "size",
                "action",
                "side",
                "sequence",
                "depth"));
        for (int level = 0; level < LEVELS; level++) {
            for (final String prefix : List.of("bidPrice", "askPrice", "bidSize", "askSize", "bidCount", "askCount")) {
                fields.add(prefix + level);
            }
        }
        return List.copyOf(fields);
    }

    /** A decoder class's getters for {@link #COPIED}, resolved once. */
    private static final class Accessors {
        private final Method[] getters;

        Accessors(final Class<?> decoderClass) {
            this.getters = new Method[COPIED.size()];
            for (int i = 0; i < COPIED.size(); i++) {
                try {
                    this.getters[i] = decoderClass.getMethod(COPIED.get(i));
                } catch (NoSuchMethodException e) {
                    throw new IllegalStateException(decoderClass.getName() + " has no " + COPIED.get(i), e);
                }
            }
        }

        Object get(final int index, final Object decoder) {
            try {
                return this.getters[index].invoke(decoder);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException("Reading " + COPIED.get(index), e);
            }
        }
    }

    /** The book encoder's setters for {@link #COPIED}, resolved once. */
    private static final class EncoderSetters {
        private final Method[] setters;

        EncoderSetters() {
            this.setters = new Method[COPIED.size()];
            for (int i = 0; i < COPIED.size(); i++) {
                this.setters[i] = setter(COPIED.get(i));
            }
        }

        private static Method setter(final String field) {
            for (final Method method : Mbp10Encoder.class.getMethods()) {
                if (method.getName().equals(field)
                        && method.getParameterCount() == 1
                        && method.getReturnType() == Mbp10Encoder.class) {
                    return method;
                }
            }
            throw new IllegalStateException("Book Mbp10Encoder has no setter " + field);
        }

        void set(final int index, final Mbp10Encoder encoder, final Object value) {
            try {
                this.setters[index].invoke(encoder, value);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException("Writing " + COPIED.get(index) + " = " + value, e);
            }
        }
    }
}

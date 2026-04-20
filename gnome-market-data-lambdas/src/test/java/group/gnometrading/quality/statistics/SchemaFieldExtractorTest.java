package group.gnometrading.quality.statistics;

import static org.junit.jupiter.api.Assertions.*;

import group.gnometrading.quality.utils.SchemaFieldExtractor;
import group.gnometrading.schemas.Action;
import group.gnometrading.schemas.MboDecoder;
import group.gnometrading.schemas.MboSchema;
import group.gnometrading.schemas.Mbp10Decoder;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.Mbp1Decoder;
import group.gnometrading.schemas.Mbp1Schema;
import group.gnometrading.schemas.SchemaType;
import org.junit.jupiter.api.Test;

class SchemaFieldExtractorTest {

    @Test
    void testExtractTradeFieldsFromMbp10() {
        Mbp10Schema s = new Mbp10Schema();
        s.encoder.price(1000);
        s.encoder.size(5);
        s.encoder.action(Action.Trade);

        SchemaFieldExtractor.TradeFields fields = SchemaFieldExtractor.extractTradeFields(s, SchemaType.MBP_10);

        assertEquals(1000, fields.price());
        assertEquals(5, fields.size());
        assertEquals(Action.Trade, fields.action());
    }

    @Test
    void testExtractTradeFieldsFromMbp1() {
        Mbp1Schema s = new Mbp1Schema();
        s.encoder.price(2000);
        s.encoder.size(3);
        s.encoder.action(Action.Add);

        SchemaFieldExtractor.TradeFields fields = SchemaFieldExtractor.extractTradeFields(s, SchemaType.MBP_1);

        assertEquals(2000, fields.price());
        assertEquals(3, fields.size());
        assertEquals(Action.Add, fields.action());
    }

    @Test
    void testExtractTradeFieldsFromMbo() {
        MboSchema s = new MboSchema();
        s.encoder.price(3000);
        s.encoder.size(10);
        s.encoder.action(Action.Cancel);

        SchemaFieldExtractor.TradeFields fields = SchemaFieldExtractor.extractTradeFields(s, SchemaType.MBO);

        assertEquals(3000, fields.price());
        assertEquals(10, fields.size());
        assertEquals(Action.Cancel, fields.action());
    }

    @Test
    void testExtractTradeFieldsReturnsNullForUnsupported() {
        assertNull(SchemaFieldExtractor.extractTradeFields(new Mbp10Schema(), SchemaType.BBO_1S));
    }

    @Test
    void testExtractBboFieldsFromMbp10() {
        Mbp10Schema s = new Mbp10Schema();
        s.encoder.bidPrice0(100);
        s.encoder.askPrice0(102);

        SchemaFieldExtractor.BboFields fields = SchemaFieldExtractor.extractBboFields(s, SchemaType.MBP_10);

        assertEquals(100, fields.bidPrice());
        assertEquals(102, fields.askPrice());
    }

    @Test
    void testExtractBboFieldsFromMbp1() {
        Mbp1Schema s = new Mbp1Schema();
        s.encoder.bidPrice0(200);
        s.encoder.askPrice0(204);

        SchemaFieldExtractor.BboFields fields = SchemaFieldExtractor.extractBboFields(s, SchemaType.MBP_1);

        assertEquals(200, fields.bidPrice());
        assertEquals(204, fields.askPrice());
    }

    @Test
    void testExtractBboFieldsReturnsNullForMbo() {
        assertNull(SchemaFieldExtractor.extractBboFields(new MboSchema(), SchemaType.MBO));
    }

    @Test
    void testExtractFlagFieldsFromMbp10() {
        Mbp10Schema s = new Mbp10Schema();
        s.encoder.flags().maybeBadBook(true);
        s.encoder.flags().badTimestampRecv(false);

        SchemaFieldExtractor.FlagFields fields = SchemaFieldExtractor.extractFlagFields(s, SchemaType.MBP_10);

        assertTrue(fields.maybeBadBook());
        assertFalse(fields.badTimestampRecv());
    }

    @Test
    void testExtractFlagFieldsFromMbp1() {
        Mbp1Schema s = new Mbp1Schema();
        s.encoder.flags().maybeBadBook(false);
        s.encoder.flags().badTimestampRecv(true);

        SchemaFieldExtractor.FlagFields fields = SchemaFieldExtractor.extractFlagFields(s, SchemaType.MBP_1);

        assertFalse(fields.maybeBadBook());
        assertTrue(fields.badTimestampRecv());
    }

    @Test
    void testExtractFlagFieldsFromMbo() {
        MboSchema s = new MboSchema();
        s.encoder.flags().maybeBadBook(true);
        s.encoder.flags().badTimestampRecv(true);

        SchemaFieldExtractor.FlagFields fields = SchemaFieldExtractor.extractFlagFields(s, SchemaType.MBO);

        assertTrue(fields.maybeBadBook());
        assertTrue(fields.badTimestampRecv());
    }

    @Test
    void testTradeSchemasContainsMboMbp1Mbp10() {
        assertTrue(SchemaFieldExtractor.TRADE_SCHEMAS.contains(SchemaType.MBO));
        assertTrue(SchemaFieldExtractor.TRADE_SCHEMAS.contains(SchemaType.MBP_1));
        assertTrue(SchemaFieldExtractor.TRADE_SCHEMAS.contains(SchemaType.MBP_10));
        assertFalse(SchemaFieldExtractor.TRADE_SCHEMAS.contains(SchemaType.BBO_1S));
    }

    @Test
    void testBboSchemasContainsMbp1Mbp10NotMbo() {
        assertTrue(SchemaFieldExtractor.BBO_SCHEMAS.contains(SchemaType.MBP_1));
        assertTrue(SchemaFieldExtractor.BBO_SCHEMAS.contains(SchemaType.MBP_10));
        assertFalse(SchemaFieldExtractor.BBO_SCHEMAS.contains(SchemaType.MBO));
    }

    @Test
    void testTradeFieldsCarriesPriceNullValue() {
        Mbp10Schema s = new Mbp10Schema();
        s.encoder.price(Mbp10Decoder.priceNullValue());

        SchemaFieldExtractor.TradeFields fields = SchemaFieldExtractor.extractTradeFields(s, SchemaType.MBP_10);

        assertTrue(fields.isPriceNull());
        assertEquals(Mbp10Decoder.priceNullValue(), fields.priceNullVal());
    }

    @Test
    void testTradeFieldsMbp1CarriesPriceNullValue() {
        Mbp1Schema s = new Mbp1Schema();
        s.encoder.price(100);

        SchemaFieldExtractor.TradeFields fields = SchemaFieldExtractor.extractTradeFields(s, SchemaType.MBP_1);

        assertFalse(fields.isPriceNull());
        assertEquals(Mbp1Decoder.priceNullValue(), fields.priceNullVal());
    }

    @Test
    void testTradeFieldsMboCarriesPriceNullValue() {
        MboSchema s = new MboSchema();
        s.encoder.price(MboDecoder.priceNullValue());

        SchemaFieldExtractor.TradeFields fields = SchemaFieldExtractor.extractTradeFields(s, SchemaType.MBO);

        assertTrue(fields.isPriceNull());
        assertEquals(MboDecoder.priceNullValue(), fields.priceNullVal());
    }

    @Test
    void testBboFieldsMbp10CarriesNullValues() {
        Mbp10Schema s = new Mbp10Schema();
        s.encoder.bidPrice0(Mbp10Decoder.bidPrice0NullValue());
        s.encoder.askPrice0(100);

        SchemaFieldExtractor.BboFields fields = SchemaFieldExtractor.extractBboFields(s, SchemaType.MBP_10);

        assertTrue(fields.isBidPriceNull());
        assertFalse(fields.isAskPriceNull());
        assertEquals(Mbp10Decoder.bidPrice0NullValue(), fields.bidPriceNullVal());
        assertEquals(Mbp10Decoder.askPrice0NullValue(), fields.askPriceNullVal());
    }

    @Test
    void testBboFieldsMbp1CarriesNullValues() {
        Mbp1Schema s = new Mbp1Schema();
        s.encoder.bidPrice0(200);
        s.encoder.askPrice0(Mbp1Decoder.askPrice0NullValue());

        SchemaFieldExtractor.BboFields fields = SchemaFieldExtractor.extractBboFields(s, SchemaType.MBP_1);

        assertFalse(fields.isBidPriceNull());
        assertTrue(fields.isAskPriceNull());
    }
}

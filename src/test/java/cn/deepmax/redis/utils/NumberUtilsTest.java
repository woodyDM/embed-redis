package cn.deepmax.redis.utils;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author wudi
 * @date 2021/12/31
 */
public class NumberUtilsTest {

    @Test
    public void shouldParseRange() {
        Range<Double> r = NumberUtils.parseScoreRange("-inf", "inf");

        assertTrue(r.start.isInfinite());
        assertTrue(r.end.isInfinite());
        assertFalse(r.startOpen);
        assertFalse(r.endOpen);

    }

    @Test
    public void shouldParseRange2() {
        Range<Double> r = NumberUtils.parseScoreRange("-1.2", "2");

        assertEquals(NumberUtils.formatDouble(r.start), "-1.2");
        assertEquals(NumberUtils.formatDouble(r.end), "2");
        assertFalse(r.startOpen);
        assertFalse(r.endOpen);
    }

    @Test
    public void shouldParseRange2Open() {
        Range<Double> r = NumberUtils.parseScoreRange("(-1.2", "(2");

        assertEquals(NumberUtils.formatDouble(r.start), "-1.2");
        assertEquals(NumberUtils.formatDouble(r.end), "2");
        assertTrue(r.startOpen);
        assertTrue(r.endOpen);
    }

    @Test
    public void testInf() {
        assertTrue(Double.NEGATIVE_INFINITY < 0D);
        assertTrue(Double.NEGATIVE_INFINITY < Double.POSITIVE_INFINITY);
        assertEquals(Double.valueOf(Double.POSITIVE_INFINITY), Double.valueOf(Double.POSITIVE_INFINITY));
        assertEquals(Double.valueOf(Double.NEGATIVE_INFINITY), Double.valueOf(Double.NEGATIVE_INFINITY));
    }
}
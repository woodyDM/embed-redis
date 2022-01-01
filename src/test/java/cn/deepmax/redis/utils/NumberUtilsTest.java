package cn.deepmax.redis.utils;

import cn.deepmax.redis.api.RedisServerException;
import cn.deepmax.redis.core.Key;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

/**
 * @author wudi
 * @date 2021/12/31
 */
public class NumberUtilsTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

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

    @Test
    public void shouldParseKeyNormal() {
        Tuple<Key, Boolean> t = NumberUtils.parseKey("-".getBytes(StandardCharsets.UTF_8));

        assertEquals(t.a, Key.NEG_INF);
    }

    @Test
    public void shouldParseKeyNormal2() {
        Tuple<Key, Boolean> t = NumberUtils.parseKey("+".getBytes(StandardCharsets.UTF_8));

        assertEquals(t.a, Key.INF);
    }

    @Test
    public void shouldParseKeyNormal3() {
        Tuple<Key, Boolean> t = NumberUtils.parseKey("(2.45".getBytes(StandardCharsets.UTF_8));

        assertEquals(t.a, new Key("2.45".getBytes()));
        assertTrue(t.b);
    }

    @Test
    public void shouldParseKeyNormal4() {
        Tuple<Key, Boolean> t = NumberUtils.parseKey("[哈哈".getBytes(StandardCharsets.UTF_8));

        assertEquals(t.a, new Key("哈哈".getBytes()));
        assertFalse(t.b);
    }

    @Test
    public void shouldParseKeyError() {
        expectedException.expect(RedisServerException.class);
        expectedException.expectMessage("ERR syntax error");

        NumberUtils.parseKey("(".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void shouldParseKeyError2() {
        expectedException.expect(RedisServerException.class);
        expectedException.expectMessage("ERR syntax error");

        NumberUtils.parseKey("owef".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void shouldParseKeyErrorNil() {
        expectedException.expect(RedisServerException.class);
        expectedException.expectMessage("ERR syntax error");

        NumberUtils.parseKey(null);
    }

    @Test
    public void shouldParseKeyErrorEmpty() {
        expectedException.expect(RedisServerException.class);
        expectedException.expectMessage("ERR syntax error");

        NumberUtils.parseKey(new byte[]{});
    }
}
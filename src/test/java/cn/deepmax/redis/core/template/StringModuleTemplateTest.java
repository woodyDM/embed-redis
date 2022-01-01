package cn.deepmax.redis.core.template;

import cn.deepmax.redis.base.BasePureTemplateTest;
import cn.deepmax.redis.utils.NumberUtils;
import org.junit.Test;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class StringModuleTemplateTest extends BasePureTemplateTest {
    public StringModuleTemplateTest(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    // ------ setnx ------
    @Test
    public void shouldSetNx() {
        Boolean ok = v().setIfAbsent("key", "hello你好");

        assertTrue(ok);
        assertThat(v().get("key"), is("hello你好"));
    }

    @Test
    public void shouldSetNxWithExist() {
        v().set("key", "OK");
        Boolean ok = v().setIfAbsent("key", "hello你好");

        assertFalse(ok);
        assertThat(v().get("key"), is("OK"));
    }

    /*  setex */
    @Test
    public void shouldSetExT() {
        v().set("key", "hello你好", 15, TimeUnit.SECONDS);

        assertThat(v().get("key"), is("hello你好"));
        assertThat(t().getExpire("key"), is(15L));
    }

    @Test
    public void shouldSetxxNotExist() {
        Boolean ok = v().setIfPresent("key", "OK", 11, TimeUnit.MINUTES);

        assertFalse(ok);
        assertNull(v().get("key"));
    }

    @Test
    public void shouldSetxxExist() {
        v().set("key", "any");
        Boolean ok = v().setIfPresent("key", "OK2", 1, TimeUnit.MINUTES);

        assertTrue(ok);
        assertThat(v().get("key"), is("OK2"));
        assertThat(t().getExpire("key"), is(60L));
    }

    @Test
    public void shouldBitOps() {
        Boolean old = v().setBit("key", 5, true);
        assertFalse(old);
        Boolean ex = v().getBit("key", 5);
        assertTrue(ex);

        old = v().setBit("key", 5, false);
        assertTrue(old);
        ex = v().getBit("key", 5);
        assertFalse(ex);
    }

    @Test
    public void shouldBitCount() {
        v().setBit("key", 0, true);
        v().setBit("key", 6, true);
        v().setBit("key", 14, true);
        v().setBit("key", 17, true);
        v().setBit("key", 23, true);

        byte[] keys = bytes("key");
        Long value = bitCount(keys);
        Long value2 = bitCount(keys, 1, 2);
        Long value3 = bitCount(keys, 2, -1);
        assertThat(value, is(5L));
        assertThat(value2, is(3L));
        assertThat(value3, is(2L));
    }

    @Test
    public void shouldBitopAndNotExist() {
        v().setBit("key", 19, true);

        Long v = t().execute((RedisCallback<Long>) con -> con.bitOp(RedisStringCommands.BitOperation.AND, bytes("dest"),
                bytes("not-exist"),
                bytes("key")));

        assertEquals(v.longValue(), 3L);
        byte[] bs = t().execute((RedisCallback<byte[]>) con -> con.get(bytes("dest")));
        assertTrue(Arrays.equals(bs, new byte[]{0, 0, 0}));
    }

    @Test
    public void shouldBitopOrNotExist() {
        v().setBit("key", 19, true);

        Long v = t().execute((RedisCallback<Long>) con -> con.bitOp(RedisStringCommands.BitOperation.OR, bytes("dest"),
                bytes("not-exist"),
                bytes("key")));

        assertEquals(v.longValue(), 3L);
        assertTrue(v().getBit("dest", 19));
        assertEquals(bitCount(bytes("dest")).longValue(), 1L);
    }

    @Test
    public void shouldBitopXorNotExist() {
        v().setBit("key", 19, true);

        Long v = t().execute((RedisCallback<Long>) con -> con.bitOp(RedisStringCommands.BitOperation.XOR, bytes("dest"),
                bytes("not-exist"),
                bytes("key")));

        assertEquals(v.longValue(), 3L);
        assertTrue(v().getBit("dest", 19));
        assertEquals(bitCount(bytes("dest")).longValue(), 1L);
    }

    @Test
    public void shouldBitopNotNotExist() {
        v().setBit("dest", 19, true);

        Long v = t().execute((RedisCallback<Long>) con -> con.bitOp(RedisStringCommands.BitOperation.NOT, bytes("dest"),
                bytes("not-exist")));

        assertEquals(v.longValue(), 0L);
        assertNull(v().get("dest"));
    }

    @Test
    public void shouldBitopAndNormal() {
        v().setBit("key1", 5, true);
        v().setBit("key2", 5, true);
        v().setBit("key1", 1, true);
        v().setBit("key2", 19, true);

        Long v = t().execute((RedisCallback<Long>) con -> con.bitOp(RedisStringCommands.BitOperation.AND, bytes("dest"),
                bytes("key1"),
                bytes("key2")));

        assertEquals(v.longValue(), 3L);
        assertTrue(v().getBit("dest", 5));
        assertFalse(v().getBit("dest", 1));
        assertFalse(v().getBit("dest", 19));
        assertEquals(bitCount(bytes("dest")).longValue(), 1L);

    }

    @Test
    public void shouldBitopOrNormal() {
        v().setBit("key1", 5, true);
        v().setBit("key2", 5, true);
        v().setBit("key1", 1, true);
        v().setBit("key2", 19, true);

        Long v = t().execute((RedisCallback<Long>) con -> con.bitOp(RedisStringCommands.BitOperation.OR, bytes("dest"),
                bytes("key1"),
                bytes("key2")));

        assertEquals(v.longValue(), 3L);
        assertTrue(v().getBit("dest", 5));
        assertTrue(v().getBit("dest", 1));
        assertTrue(v().getBit("dest", 19));
        assertEquals(bitCount(bytes("dest")).longValue(), 3L);

    }

    @Test
    public void shouldBitopXorNormal() {
        v().setBit("key1", 5, true);
        v().setBit("key2", 5, true);
        v().setBit("key1", 1, true);
        v().setBit("key2", 19, true);

        Long v = t().execute((RedisCallback<Long>) con -> con.bitOp(RedisStringCommands.BitOperation.XOR, bytes("dest"),
                bytes("key1"),
                bytes("key2")));

        assertEquals(v.longValue(), 3L);
        assertFalse(v().getBit("dest", 5));
        assertTrue(v().getBit("dest", 1));
        assertTrue(v().getBit("dest", 19));
        assertEquals(bitCount(bytes("dest")).longValue(), 2L);

    }

    @Test
    public void shouldBitopNotNormal() {
        v().setBit("key1", 1, true);
        v().setBit("key1", 5, true);
        v().setBit("key1", 19, true);

        Long v = t().execute((RedisCallback<Long>) con -> con.bitOp(RedisStringCommands.BitOperation.NOT, bytes("dest"),
                bytes("key1")));

        assertEquals(v.longValue(), 3L);
        assertFalse(v().getBit("dest", 5));
        assertFalse(v().getBit("dest", 1));
        assertFalse(v().getBit("dest", 19));

        assertTrue(v().getBit("dest", 0));
        assertTrue(v().getBit("dest", 2));

        assertEquals(bitCount(bytes("dest")).intValue(), 24 - 3);

    }

    @Test
    public void shouldBitopAndNotExist2() {
        v().set("any", "any");

        Long v = t().execute((RedisCallback<Long>) con -> con.bitOp(RedisStringCommands.BitOperation.AND, bytes("dest"), bytes("not-exist")));

        assertEquals(v.longValue(), 0L);
        assertNull(v().get("dest"));
    }

    @Test
    public void shouldBitopOrNotExist2() {
        v().set("any", "any");

        Long v = t().execute((RedisCallback<Long>) con -> con.bitOp(RedisStringCommands.BitOperation.OR, bytes("dest"), bytes("not-exist")));

        assertEquals(v.longValue(), 0L);
        assertNull(v().get("dest"));
    }

    @Test
    public void shouldBitopXorNotExist2() {
        v().set("any", "any");

        Long v = t().execute((RedisCallback<Long>) con -> con.bitOp(RedisStringCommands.BitOperation.XOR, bytes("dest"), bytes("not-exist")));

        assertEquals(v.longValue(), 0L);
        assertNull(v().get("dest"));
    }

    @Test
    public void shouldStrlen() {
        Boolean ok = set(bytes("key"), bytes("123你"));
        Long len = t().execute((RedisCallback<Long>) con -> con.strLen(bytes("key")));

        assertTrue(ok);
        assertEquals(len.intValue(), 6);

    }

    @Test
    public void shouldStrlenEmpty() {
        Long len = t().execute((RedisCallback<Long>) con -> con.strLen(bytes("key")));

        assertEquals(len.intValue(), 0);
    }

    @Test
    public void shouldGetRange() {
        byte[] ovalue = bytes("123你");
        t().execute((RedisCallback<Boolean>) con -> con.set(bytes("key"), ovalue));

        byte[] r = t().execute((RedisCallback<byte[]>) con -> con.getRange(bytes("key"), 0, -1));
        assertTrue(Arrays.equals(ovalue, r));
    }

    @Test
    public void shouldGetRange2() {
        byte[] ovalue = bytes("123你");
        set(bytes("key"), ovalue);

        //total len 6
        byte[] r = t().execute((RedisCallback<byte[]>) con -> con.getRange(bytes("key"), 1, -2));
        assertEquals(ovalue[1], r[0]);
        assertEquals(ovalue[2], r[1]);
        assertEquals(ovalue[3], r[2]);
        assertEquals(ovalue[4], r[3]);
    }

    @Test
    public void shouldGetRange3ExceedRange() {
        byte[] ovalue = bytes("123你");
        set(bytes("key"), ovalue);

        //total len 6
        byte[] r = t().execute((RedisCallback<byte[]>) con -> con.getRange(bytes("key"), 1, 10));
        assertEquals(ovalue[1], r[0]);
        assertEquals(ovalue[2], r[1]);
        assertEquals(ovalue[3], r[2]);
        assertEquals(ovalue[4], r[3]);
        assertEquals(ovalue[5], r[4]);
    }

    @Test
    public void shouldGetRange3ExceedRange2() {
        byte[] ovalue = bytes("123你");
        t().execute((RedisCallback<Boolean>) con -> con.set(bytes("key"), ovalue));

        //total len 6
        byte[] r = t().execute((RedisCallback<byte[]>) con -> con.getRange(bytes("key"), -1, -5));
        assertEquals(r.length, 0);
    }


    @Test
    public void shouldGetRangeNotExist() {
        byte[] r = t().execute((RedisCallback<byte[]>) con -> con.getRange(bytes("key"), 1, -2));
        assertEquals(r.length, 0);
    }

    @Test
    public void shouldSetRange1() {
        set(bytes("key"), bytes("123你"));

        t().execute((RedisCallback<? extends Object>)
                con -> {
                    con.setRange(bytes("key"), bytes("HAhahha"), 3);
                    return null;
                });
        assertArrayEquals(get(bytes("key")), bytes("123HAhahha"));
    }

    @Test
    public void shouldSetRangeEmpty() {
        byte[] b1 = bytes("123你");  //len 6
        byte[] b2 = bytes("HAhahha你");
        set(bytes("key"), b1);
        t().execute((RedisCallback<? extends Object>)
                con -> {
                    con.setRange(bytes("key"), b2, 7);
                    return null;
                });
        byte[] r = get(bytes("key"));
        assertEquals(r[0], b1[0]);
        assertEquals(r[1], b1[1]);
        assertEquals(r[2], b1[2]);
        assertEquals(r[3], b1[3]);
        assertEquals(r[4], b1[4]);
        assertEquals(r[5], b1[5]);
        assertEquals(r[6], 0);
        assertEquals(r[7], b2[0]);
        assertEquals(r[8], b2[1]);
        assertEquals(r[9], b2[2]);
    }

    @Test
    public void shouldSetRangeExceed() {
        t().execute((RedisCallback<? extends Object>)
                con -> {
                    con.setRange(bytes("key"), bytes("HAhahha你"), 3);
                    return null;
                });

        byte[] r = get(bytes("key"));
        assertArrayEquals(new byte[]{0, 0, 0}, Arrays.copyOfRange(r, 0, 3));
        assertArrayEquals(bytes("HAhahha你"), Arrays.copyOfRange(r, 3, 13));
    }

    @Test
    public void shouldGetSet() {
        Object old = v().getAndSet("key", "newOne号");

        assertNull(old);
        assertEquals(v().get("key"), "newOne号");
    }

    @Test
    public void shouldGetSet2() {
        v().set("key", "old笑");
        Object old = v().getAndSet("key", "newOne号");

        assertEquals(old, "old笑");
        assertEquals(v().get("key"), "newOne号");
    }

    @Test
    public void shouldMGet() {
        v().set("1", "a");
        v().set("3", "b");

        List<Object> obj = v().multiGet(Arrays.asList("1", "2", "3"));
        assertEquals(obj.get(0), "a");
        assertEquals(obj.get(1), null);
        assertEquals(obj.get(2), "b");
    }

    @Test
    public void shouldMSet() {
        Map<String, String> m = new HashMap<>();
        m.put("a", "1");
        m.put("b", "2");
        m.put("c", "3");
        v().multiSet(m);

        List<Object> obj = v().multiGet(Arrays.asList("a", "b", "c"));
        assertEquals(obj.get(0), "1");
        assertEquals(obj.get(1), "2");
        assertEquals(obj.get(2), "3");
    }

    @Test
    public void shouldMSetOK() {
        Map<String, String> m = new HashMap<>();
        m.put("a", "1");
        m.put("b", "2");
        m.put("c", "3");
        v().multiSetIfAbsent(m);

        List<Object> obj = v().multiGet(Arrays.asList("a", "b", "c"));
        assertEquals(obj.get(0), "1");
        assertEquals(obj.get(1), "2");
        assertEquals(obj.get(2), "3");
    }

    @Test
    public void shouldMSetFail() {
        v().set("a", "oldA");
        Map<String, String> m = new HashMap<>();
        m.put("a", "1");
        m.put("b", "2");
        m.put("c", "3");
        v().multiSetIfAbsent(m);

        List<Object> obj = v().multiGet(Arrays.asList("a", "b", "c"));
        assertEquals(obj.get(0), "oldA");
        assertEquals(obj.get(1), null);
        assertEquals(obj.get(2), null);
    }

    @Test
    public void shouldIncrByFloatEmpty() {
        Double result = v().increment("key", -12.345e2);

        assertEquals(NumberUtils.formatDouble(result), "-1234.5");
    }

    @Test
    public void shouldIncrByFloat1() {
        set(bytes("key"), bytes("10.345"));
        Double result = v().increment("key", -12.345e2);

        assertEquals(NumberUtils.formatDouble(result), "-1224.155");
    }

    @Test
    public void shouldIncrByFloat2() {
        set(bytes("key"), bytes("0.10345e3"));
        Double result = v().increment("key", 0.05);

        assertEquals(NumberUtils.formatDouble(result), "103.5");
    }

    @Test
    public void shouldIncrByFloat3() {
        set(bytes("key"), bytes("1.5"));
        Double result = v().increment("key", 5e-1);

        assertEquals(NumberUtils.formatDouble(result), "2");

        Long result2 = v().increment("key", 1);
        assertEquals(result2.longValue(), 3L);
    }

    @Test
    public void shouldBitPos() {
        set(bytes("key"), new byte[]{(byte) 0B00110000, (byte) 0xff});
        Long pos = t().execute((RedisCallback<Long>) con -> con.bitPos(bytes("key"), true));

        assertEquals(pos.longValue(), 2L);
    }

    @Test
    public void shouldBitPos1() {
        set(bytes("key"), new byte[]{(byte) 0B11111000, (byte) 0xff});
        Long pos = t().execute((RedisCallback<Long>) con -> con.bitPos(bytes("key"), false));

        assertEquals(pos.longValue(), 5L);
    }

    @Test
    public void shouldBitPos3() {
        set(bytes("key"), new byte[]{(byte) 0B11111000, (byte) 0B11100000, (byte) 0xff});
        Long pos = t().execute((RedisCallback<Long>) con -> con.bitPos(bytes("key"), false, Range.closed(1L, 2L)));

        assertEquals(pos.longValue(), 8 + 3);
    }

    @Test
    public void shouldBitPos4() {
        set(bytes("key"), new byte[]{(byte) 0B11111000, (byte) 0B00011111, (byte) 0xff});
        Long pos = t().execute((RedisCallback<Long>) con -> con.bitPos(bytes("key"), true, Range.closed(1L, -1L)));

        assertEquals(pos.longValue(), 8 + 3);
    }

    @Test
    public void shouldBitPos5() {
        set(bytes("key"), new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff});
        Long pos = t().execute((RedisCallback<Long>) con -> con.bitPos(bytes("key"), false, Range.closed(1L, -1L)));

        assertEquals(pos.longValue(), -1L);
    }

    @Test
    public void shouldBitPos5_2() {
        set(bytes("key"), new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff});
        Long pos = t().execute((RedisCallback<Long>) con -> con.bitPos(bytes("key"), false));

        assertEquals(pos.longValue(), 24L);
    }

    @Test
    public void shouldBitPos6() {
        set(bytes("key"), new byte[]{(byte) 0, (byte) 0, (byte) 0});
        Long pos = t().execute((RedisCallback<Long>) con -> con.bitPos(bytes("key"), true, Range.closed(1L, -1L)));

        assertEquals(pos.longValue(), -1L);
    }
}

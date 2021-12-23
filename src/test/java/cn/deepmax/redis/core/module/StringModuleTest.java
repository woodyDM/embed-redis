package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.RedisServerException;
import cn.deepmax.redis.base.BaseTemplateTest;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.utils.NumberUtils;
import io.netty.handler.codec.redis.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.*;

/**
 * @author wudi
 * @date 2021/12/22
 */
public class StringModuleTest extends BaseTemplateTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public StringModuleTest(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    @Test
    public void shouldSetParse1() {
        StringModule.Set s = new StringModule.Set();

        ListRedisMessage msg = ListRedisMessage.ofString("set key value");

        assertFalse(s.parseExpire(msg, "EX").isPresent());
        assertFalse(s.parseExpire(msg, "PX").isPresent());
        assertFalse(s.parseFlag(msg, "NX"));
        assertFalse(s.parseFlag(msg, "XX"));
    }

    @Test
    public void shouldSetParse2() {
        StringModule.Set s = new StringModule.Set();

        ListRedisMessage msg = ListRedisMessage.ofString("set key value ex 600 nx");

        Optional<Long> ex = s.parseExpire(msg, "EX");
        assertTrue(ex.isPresent());
        assertThat(ex.get(), is(600L));
        assertFalse(s.parseExpire(msg, "PX").isPresent());
        assertTrue(s.parseFlag(msg, "NX"));
        assertFalse(s.parseFlag(msg, "XX"));
    }

    @Test
    public void shouldSetParse3() {
        StringModule.Set s = new StringModule.Set();

        ListRedisMessage msg = ListRedisMessage.ofString("set key value xx px 600");

        Optional<Long> ex = s.parseExpire(msg, "PX");
        assertTrue(ex.isPresent());
        assertThat(ex.get(), is(600L));
        assertFalse(s.parseExpire(msg, "EX").isPresent());
        assertTrue(s.parseFlag(msg, "XX"));
        assertFalse(s.parseFlag(msg, "NX"));
    }

    @Test
    public void shouldError1() {
        StringModule.Set s = new StringModule.Set();

        ListRedisMessage msg = ListRedisMessage.ofString("set key value px");
        expectedException.expect(RedisServerException.class);
        expectedException.expectMessage("ERR syntax error");

        s.parseExpire(msg, "PX");

    }

    @Test
    public void shouldError2() {
        StringModule.Set s = new StringModule.Set();

        ListRedisMessage msg = ListRedisMessage.ofString("set key value px abc");
        expectedException.expect(RedisServerException.class);
        expectedException.expectMessage("ERR value is not an integer or out of range");

        s.parseExpire(msg, "PX");

    }

    @Test
    public void shouldError3() {
        StringModule.Set s = new StringModule.Set();

        ListRedisMessage msg = ListRedisMessage.ofString("set key value px 100 ex 5");
        expectedException.expect(RedisServerException.class);
        expectedException.expectMessage("ERR syntax error");

        s.parseExpire(msg);

    }

    @Test
    public void shouldParseE_1() {
        StringModule.Set s = new StringModule.Set();

        ListRedisMessage msg = ListRedisMessage.ofString("set key value px 1000");

        assertThat(s.parseExpire(msg).get(), is(1000L));
    }

    @Test
    public void shouldParseE_2() {
        StringModule.Set s = new StringModule.Set();

        ListRedisMessage msg = ListRedisMessage.ofString("set key value ex 5");

        assertThat(s.parseExpire(msg).get(), is(5000L));
    }

    @Test
    public void shouldSetEx() {
        ListRedisMessage msg = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好"))
                .append("ex")
                .append("3600").build();
        RedisMessage resp = engine().execute(msg, embeddedClient());

        SimpleStringRedisMessage m = (SimpleStringRedisMessage) resp;
        assertThat(m.content(), is("OK"));
        assertThat(v().get("key"), is("好"));
        assertThat(t().getExpire("key"), is(3600L));

    }

    @Test
    public void shouldSetNxEx() {
        ListRedisMessage msg = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好"))
                .append("nx")
                .append("ex")
                .append("3600").build();
        RedisMessage resp = engine().execute(msg, embeddedClient());

        SimpleStringRedisMessage m = (SimpleStringRedisMessage) resp;
        assertThat(m.content(), is("OK"));
        assertThat(v().get("key"), is("好"));
        assertThat(t().getExpire("key"), is(3600L));

    }


    @Test
    public void shouldSetPx() {
        ListRedisMessage msg = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好"))
                .append("px")
                .append("6000").build();
        RedisMessage resp = engine().execute(msg, embeddedClient());

        SimpleStringRedisMessage m = (SimpleStringRedisMessage) resp;
        assertThat(m.content(), is("OK"));
        assertThat(v().get("key"), is("好"));
        assertThat(t().getExpire("key"), is(6L));

    }

    @Test
    public void shouldSetNxPx() {
        ListRedisMessage msg = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好"))
                .append("nx")
                .append("px")
                .append("6000").build();
        RedisMessage resp = engine().execute(msg, embeddedClient());

        SimpleStringRedisMessage m = (SimpleStringRedisMessage) resp;
        assertThat(m.content(), is("OK"));
        assertThat(v().get("key"), is("好"));
        assertThat(t().getExpire("key"), is(6L));

    }

    @Test
    public void shouldSetNxFail() {
        v().set("key", "hahahah哈哈");

        ListRedisMessage msg = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好"))
                .append("nx")
                .append("3600").build();
        RedisMessage resp = engine().execute(msg, embeddedClient());

        assertThat(resp, sameInstance(FullBulkStringRedisMessage.NULL_INSTANCE));

        assertThat(v().get("key"), is("hahahah哈哈"));
    }

    @Test
    public void shouldSetXXWithoutKey() {
        ListRedisMessage msg = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好"))
                .append("xx")
                .append("ex")
                .append("3600").build();
        RedisMessage resp = engine().execute(msg, embeddedClient());

        assertThat(resp, sameInstance(FullBulkStringRedisMessage.NULL_INSTANCE));
        assertNull(v().get("key"));
    }


    @Test
    public void shouldSetXXWithEx() {
        ListRedisMessage msg = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好"))
                .append("px")
                .append("6000").build();
        engine().execute(msg, embeddedClient());

        ListRedisMessage msg2 = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好2"))
                .append("xx")
                .append("px")
                .append("8000").build();
        RedisMessage resp2 = engine().execute(msg2, embeddedClient());

        SimpleStringRedisMessage m = (SimpleStringRedisMessage) resp2;
        assertThat(m.content(), is("OK"));
        assertThat(v().get("key"), is("好2"));
        assertThat(t().getExpire("key"), is(8L));

    }

    @Test
    public void shouldSetEx8KeepTtl() {
        ListRedisMessage msg = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好"))
                .append("px")
                .append("6000").build();
        engine().execute(msg, embeddedClient());

        ListRedisMessage msg2 = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好2"))
                .append("xx")
                .append("keepttl")
                .build();
        RedisMessage resp2 = engine().execute(msg2, embeddedClient());

        SimpleStringRedisMessage m = (SimpleStringRedisMessage) resp2;
        assertThat(m.content(), is("OK"));
        assertThat(v().get("key"), is("好2"));
        assertThat(t().getExpire("key"), is(6L));

    }

    @Test
    public void shouldSetErrorWithPxAndEx() {
        ListRedisMessage msg = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好"))
                .append("px")
                .append("6000")
                .append("ex")
                .append("5").build();
        RedisMessage msg2 = engine().execute(msg, embeddedClient());


        ErrorRedisMessage m = (ErrorRedisMessage) msg2;
        assertThat(m.content(), is("ERR syntax error"));
    }

    @Test
    public void shouldSetErrorWithPxAndKeepttl() {
        ListRedisMessage msg = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好"))
                .append("px")
                .append("6000")
                .append("keepttl")
                .build();
        RedisMessage msg2 = engine().execute(msg, embeddedClient());


        ErrorRedisMessage m = (ErrorRedisMessage) msg2;
        assertThat(m.content(), is("ERR syntax error"));
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

    /*  append */
    @Test
    public void shouldAppendNotExist() {
        Integer len = v().append("key", "123");
        t().expire("key", 25, TimeUnit.SECONDS);
        RedisMessage resp = engine().execute(ListRedisMessage.ofString("get key"), embeddedClient());

        assertThat(len, is(3));
        assertThat(((FullBulkStringRedisMessage) resp).content().toString(StandardCharsets.UTF_8), is("123"));

        Integer len2 = v().append("key", " 456你");
        RedisMessage resp2 = engine().execute(ListRedisMessage.ofString("get key"), embeddedClient());

        assertThat(len2, is(3 + 4 + 3));
        assertThat(((FullBulkStringRedisMessage) resp2).content().toString(StandardCharsets.UTF_8), is("123 456你"));
        assertThat(t().getExpire("key"), is(25L));
    }
    /*   incr incrby decr decrby */

    @Test
    public void shouldIncrAndDecr() {
        RedisMessage resp = engine().execute(ListRedisMessage.ofString("incr key"), embeddedClient());
        RedisMessage resp2 = engine().execute(ListRedisMessage.ofString("get key"), embeddedClient());
        assertThat(((IntegerRedisMessage) resp).value(), is(1L));
        assertThat(((FullBulkStringRedisMessage) resp2).content().toString(StandardCharsets.UTF_8), is("1"));

        resp = engine().execute(ListRedisMessage.ofString("incr key"), embeddedClient());
        resp2 = engine().execute(ListRedisMessage.ofString("get key"), embeddedClient());
        assertThat(((IntegerRedisMessage) resp).value(), is(2L));
        assertThat(((FullBulkStringRedisMessage) resp2).content().toString(StandardCharsets.UTF_8), is("2"));

        t().expire("key", 50, TimeUnit.SECONDS);
        resp = engine().execute(ListRedisMessage.ofString("incrby key 10"), embeddedClient());
        resp2 = engine().execute(ListRedisMessage.ofString("get key"), embeddedClient());
        assertThat(((IntegerRedisMessage) resp).value(), is(12L));
        assertThat(((FullBulkStringRedisMessage) resp2).content().toString(StandardCharsets.UTF_8), is("12"));

        resp = engine().execute(ListRedisMessage.ofString("decr key"), embeddedClient());
        resp2 = engine().execute(ListRedisMessage.ofString("get key"), embeddedClient());
        assertThat(((IntegerRedisMessage) resp).value(), is(11L));
        assertThat(((FullBulkStringRedisMessage) resp2).content().toString(StandardCharsets.UTF_8), is("11"));

        resp = engine().execute(ListRedisMessage.ofString("decrby key 5"), embeddedClient());
        resp2 = engine().execute(ListRedisMessage.ofString("get key"), embeddedClient());
        assertThat(((IntegerRedisMessage) resp).value(), is(6L));
        assertThat(((FullBulkStringRedisMessage) resp2).content().toString(StandardCharsets.UTF_8), is("6"));
        assertThat(t().getExpire("key"), is(50L));

        //errors
        resp = engine().execute(ListRedisMessage.ofString("incrby key 2332e2"), embeddedClient());
        assertTrue(((ErrorRedisMessage) resp).content().contains("ERR value is not an integer or out of range"));

    }

    @Test
    public void shouldIncrError() {
        RedisMessage resp = engine().execute(ListRedisMessage.ofString("incrby key 23aaa"), embeddedClient());
        assertTrue(((ErrorRedisMessage) resp).content().contains("ERR value is not an integer or out of range"));

        RedisMessage resp2 = engine().execute(ListRedisMessage.ofString("decrby key 23bee1"), embeddedClient());
        assertTrue(((ErrorRedisMessage) resp2).content().contains("ERR value is not an integer or out of range"));
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

    boolean set(byte[] key, byte[] value) {
        return t().execute((RedisCallback<Boolean>) con -> con.set(key, value));
    }

    byte[] get(byte[] key) {
        return t().execute((RedisCallback<byte[]>) con -> con.get(key));
    }

    protected Long bitCount(byte[] key, long start, long end) {
        return t().execute((RedisCallback<Long>) cn -> cn.bitCount(key, start, end));
    }

    protected Long bitCount(byte[] key) {
        return t().execute((RedisCallback<Long>) cn -> cn.bitCount(key, 0, -1L));
    }
}
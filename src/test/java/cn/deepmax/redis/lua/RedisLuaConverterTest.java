package cn.deepmax.redis.lua;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.base.ByteHelper;
import cn.deepmax.redis.resp3.*;
import cn.deepmax.redis.utils.NumberUtils;
import io.netty.handler.codec.redis.*;
import org.junit.Test;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import static cn.deepmax.redis.lua.RedisLuaConverter.*;
import static org.junit.Assert.*;

/**
 * @author wudi
 * @date 2022/1/14
 */
public class RedisLuaConverterTest implements ByteHelper {

    @Test
    public void shouldConvertToLua() {
        Client.Protocol resp = Client.Protocol.RESP2;
        assertEquals(LuaValue.NIL, toLua(NullRedisMessage.INSTANCE, resp));
        LuaValue v = toLua(new IntegerRedisMessage(-100), resp);
        assertEquals(v.type(), LuaValue.TNUMBER);
        assertEquals(v.tolong(), -100L);

        v = toLua(new IntegerRedisMessage(0), resp);
        assertEquals(v.type(), LuaValue.TNUMBER);
        assertEquals(v.tolong(), 0L);

        v = toLua(FullBulkStringRedisMessage.NULL_INSTANCE, resp);
        assertEquals(v.type(), LuaValue.TBOOLEAN);
        assertFalse(v.toboolean());

        v = toLua(FullBulkStringRedisMessage.EMPTY_INSTANCE, resp);
        assertEquals(v.type(), LuaValue.TSTRING);
        assertEquals(v.tostring().tojstring(), "");

        v = toLua(FullBulkValueRedisMessage.ofString("你好😊"), resp);
        assertEquals(v.type(), LuaValue.TSTRING);
        assertArrayEquals(v.strvalue().m_bytes, "你好😊".getBytes(StandardCharsets.UTF_8));

        v = toLua(new SimpleStringRedisMessage("hello world"), resp);
        assertEquals(v.type(), LuaValue.TTABLE);
        assertEquals(v.checktable().keyCount(), 1);
        assertArrayEquals(v.checktable().get("ok").strvalue().m_bytes, bytes("hello world"));

        v = toLua(new ErrorRedisMessage("some error"), resp);
        assertEquals(v.type(), LuaValue.TTABLE);
        assertEquals(v.checktable().keyCount(), 1);
        assertArrayEquals(v.checktable().get("err").strvalue().m_bytes, bytes("some error"));

        v = toLua(BooleanRedisMessage.TRUE, resp);
        assertEquals(v.type(), LuaValue.TBOOLEAN);
        assertTrue(v.toboolean());

        v = toLua(BooleanRedisMessage.FALSE, resp);
        assertEquals(v.type(), LuaValue.TBOOLEAN);
        assertFalse(v.toboolean());

        v = toLua(DoubleRedisMessage.ofDouble(1.4D), resp);
        assertEquals(v.type(), LuaValue.TTABLE);
        assertEquals(v.checktable().keyCount(), 1);
        assertEquals(NumberUtils.formatDouble(v.checktable().get("double").todouble()), "1.4");

        v = toLua(DoubleRedisMessage.ofDouble(Double.POSITIVE_INFINITY), resp);
        assertEquals(v.type(), LuaValue.TTABLE);
        assertEquals(v.checktable().keyCount(), 1);
        assertTrue(v.checktable().get("double").todouble() == Double.POSITIVE_INFINITY);

        v = toLua(DoubleRedisMessage.ofDouble(Double.NEGATIVE_INFINITY), resp);
        assertEquals(v.type(), LuaValue.TTABLE);
        assertEquals(v.checktable().keyCount(), 1);
        assertTrue(v.checktable().get("double").todouble() == Double.NEGATIVE_INFINITY);

        v = toLua(new BigNumberRedisMessage(BigDecimal.TEN), resp);
        assertEquals(v.type(), LuaValue.TTABLE);
        assertEquals(v.checktable().keyCount(), 1);
        assertTrue(v.checktable().get("big_number").toint() == 10);

    }

    @Test
    public void shouldConvertAggToLua() {
        Client.Protocol resp = Client.Protocol.RESP2;
        ListRedisMessage dm = ListRedisMessage.newBuilder()
                .append(FullBulkValueRedisMessage.ofString("key"))
                .append(new IntegerRedisMessage(100))
                .append(FullBulkValueRedisMessage.ofString("haha"))
                .append(new ErrorRedisMessage("err ha")).build();

        LuaValue v = toLua(dm, resp);
        assertResp2Agg(v);

        SetRedisMessage set = new SetRedisMessage(dm.children());
        v = toLua(set, resp);
        assertResp2Agg(v);

        MapRedisMessage map = new MapRedisMessage(dm.children());
        v = toLua(map, resp);
        assertResp2Agg(v);

        //test for resp3
        resp = Client.Protocol.RESP3;

        v = toLua(set, resp);
        assertEquals(v.type(), LuaValue.TTABLE);
        assertEquals(v.checktable().keyCount(), 1);
        LuaTable intable = v.get("set").checktable();
        assertTrue(intable.get("key").toboolean());
        assertTrue(intable.get(100).toboolean());
        assertTrue(intable.get("haha").toboolean());

        LuaValue[] k = intable.keys();
        assertEquals(k.length, 4);
        for (LuaValue value : k) {
            if (value.istable()) {
                assertTrue(intable.get(value).toboolean());
            }
        }

        v = toLua(map, resp);
        assertEquals(v.type(), LuaValue.TTABLE);
        assertEquals(v.checktable().keyCount(), 1);
        intable = v.get("map").checktable();
        assertEquals(intable.keyCount(), 2);
        assertEquals(intable.get("key").toint(), 100);
        assertEquals(intable.get("haha").checktable().get("err").tojstring(), "err ha");

    }

    private void assertResp2Agg(LuaValue v) {
        assertEquals(v.type(), LuaValue.TTABLE);
        assertEquals(v.checktable().keyCount(), 4);
        assertEquals(v.get(1).strvalue().tojstring(), "key");
        assertEquals(v.get(2).strvalue().toint(), 100);
        assertEquals(v.get(3).strvalue().tojstring(), "haha");
        assertEquals(v.get(4).get("err").strvalue().tojstring(), "err ha");
    }

    @Test
    public void shouldLuaToRedisCommon() {
        Client.Protocol resp = Client.Protocol.RESP2;

        LuaValue v = LuaValue.valueOf("你好");
        RedisMessage r = toRedis(v, resp);
        assertEquals(((FullBulkValueRedisMessage) r).str(), "你好");

        v = LuaValue.NIL;
        assertSame(toRedis(v, resp), NullRedisMessage.INSTANCE);

        v = LuaValue.valueOf(true);
        r = toRedis(v, resp);
        assertSame(r, Constants.INT_ONE);
        assertSame(toRedis(v, Client.Protocol.RESP3), BooleanRedisMessage.TRUE);

        v = LuaValue.valueOf(false);
        r = toRedis(v, resp);
        assertSame(r, FullBulkValueRedisMessage.NULL_INSTANCE);
        assertSame(toRedis(v, Client.Protocol.RESP3), BooleanRedisMessage.FALSE);

        v = LuaValue.valueOf(45.6);
        r = toRedis(v, resp);
        assertEquals(((IntegerRedisMessage) r).value(), 45);

        v = LuaValue.valueOf(-100);
        r = toRedis(v, resp);
        assertEquals(((IntegerRedisMessage) r).value(), -100);

        v = table("ok", "OK simple");
        r = toRedis(v, resp);
        assertEquals(((SimpleStringRedisMessage) r).content(), "OK simple");

        v = table("err", "some err");
        r = toRedis(v, resp);
        assertEquals(((ErrorRedisMessage) r).content(), "some err");

        v = table("double", 45.6);
        r = toRedis(v, resp);
        assertEquals(((DoubleRedisMessage) r).content(), "45.6");

        //map
        v = LuaValue.tableOf();
        LuaTable in = LuaValue.tableOf();
        v.set("map", in);
        in.set(LuaValue.valueOf("someKey"), LuaValue.valueOf(100));
        in.set(LuaValue.valueOf("errOf"), table("err", "param error"));
        r = toRedis(v, resp);

        MapRedisMessage map = (MapRedisMessage) r;
        assertEquals(map.size(), 2);
        map.content().forEach((k, val) -> {
            if (((FullBulkValueRedisMessage) k).str().equals("someKey")) {
                assertEquals(((IntegerRedisMessage) val).value(), 100L);
            } else if (((FullBulkValueRedisMessage) k).str().equals("errOf")) {
                assertEquals(((ErrorRedisMessage) val).content(), "param error");
            }
        });

    }

    @Test
    public void shouldLuaToRedisSet() {
        LuaValue v = LuaValue.tableOf();
        LuaTable in = LuaValue.tableOf();
        v.set("set", in);
        in.set(LuaValue.valueOf("someKey"), LuaValue.valueOf(true));
        in.set(LuaValue.valueOf(100), LuaValue.valueOf(true));
        in.set(table("err", "param error"), LuaValue.valueOf(true));
        RedisMessage r = toRedis(v, Client.Protocol.RESP2);

        SetRedisMessage set = (SetRedisMessage) r;
        assertEquals(set.children().size(), 3);
        assertEquals(((FullBulkValueRedisMessage) set.children().get(0)).str(), "someKey");
        assertEquals(((IntegerRedisMessage) set.children().get(1)).value(), 100L);
        assertEquals(((ErrorRedisMessage) set.children().get(2)).content(), "param error");
    }

    @Test
    public void shouldLuaToRedisList() {
        LuaTable in = LuaValue.tableOf();

        in.set(2, LuaValue.valueOf("someKey"));
        in.set(3, LuaValue.valueOf(100));
        in.set(4, table("err", "param error"));
        RedisMessage r = toRedis(in, Client.Protocol.RESP2);

        ListRedisMessage list = (ListRedisMessage) r;
        assertEquals(list.children().size(), 3);
        assertEquals(((FullBulkValueRedisMessage) list.children().get(0)).str(), "someKey");
        assertEquals(((IntegerRedisMessage) list.children().get(1)).value(), 100L);
        assertEquals(((ErrorRedisMessage) list.children().get(2)).content(), "param error");
    }

    @Test
    public void shouldVarLuaToRedisList() {
        Varargs in = LuaValue.varargsOf(new LuaValue[]{
                LuaValue.valueOf("someKey"), LuaValue.valueOf(100), table("err", "param error")
                , LuaValue.NIL, LuaValue.valueOf(true)
        });

        RedisMessage r = toRedis(in, Client.Protocol.RESP2);

        ListRedisMessage list = (ListRedisMessage) r;
        assertEquals(list.children().size(), 3);
        assertEquals(((FullBulkValueRedisMessage) list.children().get(0)).str(), "someKey");
        assertEquals(((IntegerRedisMessage) list.children().get(1)).value(), 100L);
        assertEquals(((ErrorRedisMessage) list.children().get(2)).content(), "param error");
    }
}
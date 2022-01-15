package cn.deepmax.redis.lua;

import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.base.ByteHelper;
import cn.deepmax.redis.resp3.*;
import cn.deepmax.redis.utils.NumberUtils;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import org.junit.Test;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;

import java.nio.charset.StandardCharsets;

import static cn.deepmax.redis.lua.RedisLuaConverter.toLua;
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

        resp = Client.Protocol.RESP3;

        v = toLua(set, resp);
        assertEquals(v.type(), LuaValue.TTABLE);
        assertEquals(v.checktable().keyCount(), 1);
        LuaTable intable = v.get("set").checktable();
        assertTrue(intable.get("key").toboolean() );
        assertTrue(intable.get(100).toboolean() );
        assertTrue(intable.get("haha").toboolean() );
        
        LuaValue[] k = intable.keys();
        assertEquals(k.length,4);
        for (LuaValue value : k) {
            if (value.istable()) {
                assertTrue(intable.get(value).toboolean());
            }
        }

    }

    private void assertResp2Agg(LuaValue v) {
        assertEquals(v.type(), LuaValue.TTABLE);
        assertEquals(v.checktable().keyCount(), 4);
        assertEquals(v.get(1).strvalue().tojstring(),"key");
        assertEquals(v.get(2).strvalue().toint(),100);
        assertEquals(v.get(3).strvalue().tojstring(),"haha");
        assertEquals(v.get(4).get("err").strvalue().tojstring() ,"err ha");
    }
}
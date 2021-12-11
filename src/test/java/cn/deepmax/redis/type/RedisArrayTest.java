package cn.deepmax.redis.type;

import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

/**
 * @author wudi
 * @date 2021/5/1
 */
public class RedisArrayTest {

    @Test
    public void emptyArray() {
        RedisArray array = new RedisArray();
        
        assertTrue(array.isArray());
        assertFalse(array.isNull());
        assertArrayEquals(array.respContent(),"*0\r\n".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void nilArray() {
        RedisArray array = RedisArray.NIL;

        assertTrue(array.isArray());
        assertTrue(array.isNull());
        assertArrayEquals(array.respContent(),"*-1\r\n".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void normaArray() {
        RedisArray array = new RedisArray();
        array.add(new RedisString("ab你"));
        array.add(new RedisInteger(1000));
        array.add(new RedisError("Err 1"));
        array.add(  RedisBulkString.of("abcd"));
        
        assertTrue(array.isArray());
        assertFalse(array.isNull());
        assertEquals(array.children().size(),4);
        assertArrayEquals(array.respContent(),"*4\r\n+ab你\r\n:1000\r\n-Err 1\r\n$4\r\nabcd\r\n".getBytes(StandardCharsets.UTF_8));
    }
}
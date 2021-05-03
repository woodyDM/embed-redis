package cn.deepmax.redis.type;

import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

/**
 * @author wudi
 * @date 2021/5/1
 */
public class RedisBulkStringTest {


    @Test
    public void shouldOK() {
        RedisBulkString string = RedisBulkString.of((String)null);
     
        assertTrue(string.isNil());
        assertTrue(string.isString());
        assertArrayEquals(string.respContent(),"$-1\r\n".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void shouldOK2() {
        RedisBulkString string = RedisBulkString.of("");

        assertFalse(string.isNil());
        assertTrue(string.isString());
        assertArrayEquals(string.respContent(),"$0\r\n\r\n".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void shouldOK3() {
        RedisBulkString string = RedisBulkString.of("ab你好");

        assertFalse(string.isNil());
        assertTrue(string.isString());
        assertArrayEquals(string.respContent(),"$8\r\nab你好\r\n".getBytes(StandardCharsets.UTF_8));
    }
}
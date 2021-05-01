package cn.deepmax.redis.type;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author wudi
 * @date 2021/5/1
 */
public class RedisBulkStringTest {


    @Test
    public void shouldOK() {
        RedisBulkString string = RedisBulkString.valueOf(null);
     
        assertTrue(string.isNil());
        assertTrue(string.isString());
        assertEquals(string.respContent(),"$-1\r\n");
    }

    @Test
    public void shouldOK2() {
        RedisBulkString string = RedisBulkString.valueOf("");

        assertFalse(string.isNil());
        assertTrue(string.isString());
        assertEquals(string.respContent(),"$0\r\n\r\n");
    }

    @Test
    public void shouldOK3() {
        RedisBulkString string = RedisBulkString.valueOf("ab你好");

        assertFalse(string.isNil());
        assertTrue(string.isString());
        assertEquals(string.respContent(),"$8\r\nab你好\r\n");
    }
}
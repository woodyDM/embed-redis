package cn.deepmax.redis.type;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author wudi
 * @date 2021/5/1
 */
public class RedisStringTest {
    
    RedisString string;

    @Before
    public void setUp() throws Exception {
        string = new RedisString("OK");
    }

    @Test
    public void shouldOK() {
        assertTrue(string.isString());
        assertFalse(string.isArray());
        assertFalse(string.isInteger());
        assertFalse(string.isError());
        assertFalse(string.isNil());
        assertEquals(string.respContent(),"+OK\r\n");
        
    }
}
package cn.deepmax.redis.type;

import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

/**
 * @author wudi
 * @date 2021/5/1
 */
public class RStringTest {
    
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
        assertArrayEquals(string.respContent(),"+OK\r\n".getBytes(StandardCharsets.UTF_8));
        
    }
}
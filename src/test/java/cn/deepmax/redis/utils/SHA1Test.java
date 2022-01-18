package cn.deepmax.redis.utils;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author wudi
 */
public class SHA1Test {

    public static final String SCRIPT = "return ARGV[1];  ";
    public static final String SCRIPT_SHA = "1c1cc42f444fdebd9f88e92656f2eb40d7c27c4c";

    @Test
    public void shouldSha() {
        String s = SHA1.encode(SCRIPT);
        
        assertEquals(SCRIPT_SHA, s);
    }
}
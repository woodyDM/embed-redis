package cn.deepmax.redis.utils;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author wudi
 * @date 2021/12/16
 */
public class SHA1Test {

    public static final String SCRIPT = "return ARGV[1];  ";
    public static final String SCRIPT_SHA = "1C1CC42F444FDEBD9F88E92656F2EB40D7C27C4C";

    @Test
    public void shouldSha() {
        String s = SHA1.encode(SCRIPT);
        
        assertEquals(SCRIPT_SHA, s);
    }
}
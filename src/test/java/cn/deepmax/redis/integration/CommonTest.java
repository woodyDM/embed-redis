package cn.deepmax.redis.integration;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CommonTest extends BaseTest {

    @Test
    public void shouldResponsePing() {
        String pong = redis.ping();
        assertEquals(pong, "PONG");
    }

    @Test
    public void shouldDel() {

        String v = redis.set("1", "1");
        String v2 = redis.set("2", "2");
        String v3 = redis.set("3", "3");

        Long num = redis.del("1", "3");
        String vg1 = redis.get("1");
        String vg2 = redis.get("2");
        String vg3 = redis.get("3");

        assertEquals(num.longValue(), 2L);
        assertNull(vg1);
        assertNull(vg3);
        assertEquals(vg2, "2");

        redis.set("2", "2M");
        assertEquals("2M", redis.get("2"));

    }
}

package cn.deepmax.redis.integration;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StringTest extends BaseTest{


    @Test
    public void shouldSetAndGet() {
        String va = "整合Lettuce Redis SpringBoot\uD83D\uDE0A";
        String v = redis.set("1", va);
        String v2 = redis.get("1");

        assertEquals(va, v2);
    }

}

package cn.deepmax.redis.integration;

import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * @author wudi
 * @date 2021/12/15
 */
public class LuaTest extends BaseTemplateTest {
    public LuaTest(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }
    
    @Test
    public void shouldExec() {
        String sc = " redis.call('SET', KEYS[1], ARGV[1]) ; return 1; ";
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>(sc, Long.class);
        Long execute = t().execute(redisScript, Collections.singletonList("name"), "myName你👌");

        assertEquals(execute.longValue(), 1L);
        assertEquals(v().get("name"), "myName你👌");
    }

    @Ignore
    @Test
    public void shouldExec2() {

        byte[] bytes = {5, 11, 2, 3};
        String sc = "return ARGV[1];";


        DefaultRedisScript<byte[]> redisScript = new DefaultRedisScript<>(sc, byte[].class);
        byte[] results = t().execute(redisScript, Collections.singletonList("name"), bytes);


        assertEquals(results[0], 5);
        assertEquals(results[1], 11);
        assertEquals(results[2], 2);
        assertEquals(results[3], 3);
    }
}

package cn.deepmax.redis.core.template;

import cn.deepmax.redis.base.BasePureTemplateTest;
import cn.deepmax.redis.core.mixed.LuaMixedTest;
import org.junit.Test;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class LuaTemplateTest extends BasePureTemplateTest {
    public LuaTemplateTest(RedisTemplate<String, Object> redisTemplate) {
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

    @Test
    public void shouldEvalsha() {
        String script = "return ARGV[1];";
        RedisTemplate<String, Object> redisTemplate = t();
        DefaultRedisScript<byte[]> redisScript = new DefaultRedisScript<>(script, byte[].class);
        byte[] results = redisTemplate.execute(redisScript, Collections.singletonList("any"), new byte[]{5, 11, 2, 3});

        assertEquals(results[0], 5);
        assertEquals(results[1], 11);
        assertEquals(results[2], 2);
        assertEquals(results[3], 3);
    }

    /**
     * @see LuaMixedTest#shouldExec2()
     */
    @Test
    public void shouldExec2() {
        String sc = " redis.call('set', KEYS[1], ARGV[1]) ;" +
                " redis.call('lpush', KEYS[2], ARGV[2]) ; " +
                " local v = redis.call('get',KEYS[1]);" +
                " return v; ";

        DefaultRedisScript<String> redisScript = new DefaultRedisScript<>(sc, String.class);
        String execute = t().execute(redisScript, Arrays.asList("key1", "key2"), "myName你👌", "list_value");

        assertEquals(execute, "myName你👌");
        assertEquals(v().get("key1"), "myName你👌");
        assertEquals(l().rightPop("key2"), "list_value");
    }
}

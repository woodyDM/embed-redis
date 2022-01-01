package cn.deepmax.redis.core.mixed;

import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.base.BaseMixedTemplateTest;
import org.junit.Test;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * @author wudi
 * @date 2021/12/15
 */
public class LuaMixedTest extends BaseMixedTemplateTest {
    public LuaMixedTest(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    @Test
    public void shouldExec2() {
        String sc = " redis.call('set', KEYS[1], ARGV[1]) ;" +
                " redis.call('lpush', KEYS[2], ARGV[2]) ; " +
                " local v = redis.call('get',KEYS[1]);" +
                " return v; ";

        RedisEngine e = engine();

        ExpectedEvents events = listen("key1");
        DefaultRedisScript<String> redisScript = new DefaultRedisScript<>(sc, String.class);
        String execute = t().execute(redisScript, Arrays.asList("key1", "key2"), "myName你👌", "list_value");

        RedisEngine e2 = engine();
        assertEquals(execute, "myName你👌");
        assertEquals(events.triggerTimes, 1);   //set two key in ,but only trigger one time.
        assertEquals(events.events.size(), 2);
        assertEquals(v().get("key1"), "myName你👌");
        assertEquals(l().rightPop("key2"), "list_value");
    }


}

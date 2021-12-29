package cn.deepmax.redis.core.module;

import cn.deepmax.redis.base.BaseTemplateTest;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import org.junit.Test;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Arrays;
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

    @Test
    public void shouldExec2() {
        //first load script to avoid redisson eval bug. 
        //@see https://github.com/redisson/redisson/pull/4032
        ExpectedEvents events = listen("key1");
        String sc = " redis.call('set', KEYS[1], ARGV[1]) ;" +
                " redis.call('lpush', KEYS[2], ARGV[2]) ; " +
                " local v = redis.call('get',KEYS[1]);" +
                " return v; ";
        engine().execute(ListRedisMessage.newBuilder()
                .append("script")
                .append("load")
                .append(FullBulkValueRedisMessage.ofString(sc)).build(), embeddedClient());

        DefaultRedisScript<String> redisScript = new DefaultRedisScript<>(sc, String.class);
        String execute = t().execute(redisScript, Arrays.asList("key1", "key2"), "myName你👌", "list_value");

        assertEquals(execute, "myName你👌");
        assertEquals(events.triggerTimes, 1);   //set two key in ,but only trigger one time.
        assertEquals(events.events.size(), 2);
        assertEquals(v().get("key1"), "myName你👌");
        assertEquals(l().rightPop("key2"), "list_value");
    }

    @Test
    public void shouldEvalsha() {
        //first load script to avoid redisson codec error.
        String script = "return ARGV[1];";
        engine().execute(ListRedisMessage.newBuilder()
                .append(FullBulkValueRedisMessage.ofString("script"))
                .append(FullBulkValueRedisMessage.ofString("load"))
                .append(FullBulkValueRedisMessage.ofString(script)).build(), embeddedClient());


        RedisTemplate<String, Object> redisTemplate = t();
        DefaultRedisScript<byte[]> redisScript = new DefaultRedisScript<>(script, byte[].class);
        byte[] results = redisTemplate.execute(redisScript, Collections.singletonList("any"), new byte[]{5, 11, 2, 3});
        
        assertEquals(results[0], 5);
        assertEquals(results[1], 11);
        assertEquals(results[2], 2);
        assertEquals(results[3], 3);
    }
}

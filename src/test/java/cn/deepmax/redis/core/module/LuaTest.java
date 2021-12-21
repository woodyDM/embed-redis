package cn.deepmax.redis.core.module;

import cn.deepmax.redis.base.BaseTemplateTest;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.utils.SHA1Test;
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

package cn.deepmax.redis.core.mixed;

import cn.deepmax.redis.base.BaseMixedTemplateTest;
import org.junit.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author wudi
 */
public class TransactionModuleTest extends BaseMixedTemplateTest {
    public TransactionModuleTest(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    @Test
    public void shouldRunTx() {
        if (isRedisson()) {
            return; //redisson 3.16.7 will not run exec???
        }
        ExpectedEvents events = listen("k1");
        SessionCallback<Object> sessionCallback = new SessionCallback<Object>() {
            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                operations.multi();
                operations.opsForValue().set("k1", "old");
                operations.opsForValue().getAndSet("k1", "new");
                operations.opsForValue().set("k2", "v2", 15, TimeUnit.SECONDS);
                return operations.exec();
            }
        };
        //redisson v:  size = 1 : only old
        //jedis / lettuce: v: size = 3: true old true
        List<Object> v = (List<Object>) t().execute(sessionCallback);

        assertTrue(v.stream().anyMatch(vl -> "old".equals(vl)));
        assertEquals(v().get("k1"), "new");
        assertEquals(v().get("k2"), "v2");
        assertEquals(t().getExpire("k2").longValue(), 15L);
        assertEquals(events.events.size(), 2);
        assertEquals(events.triggerTimes, 1);
    }


}
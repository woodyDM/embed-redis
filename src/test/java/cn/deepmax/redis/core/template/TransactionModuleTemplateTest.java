package cn.deepmax.redis.core.template;

import cn.deepmax.redis.base.BasePureTemplateTest;
import org.junit.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TransactionModuleTemplateTest extends BasePureTemplateTest {
    public TransactionModuleTemplateTest(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    @Test
    public void shouldRunEmpty() {
        SessionCallback<Object> sessionCallback = new SessionCallback<Object>() {
            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                operations.multi();
                return operations.exec();
            }
        };
        List<Object> v = (List<Object>) t().execute(sessionCallback);

        assertEquals(v.size(), 0);
    }
}

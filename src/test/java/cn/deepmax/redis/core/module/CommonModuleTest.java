package cn.deepmax.redis.core.module;

import cn.deepmax.redis.base.BaseTemplateTest;
import org.junit.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * @author wudi
 * @date 2021/12/16
 */
public class CommonModuleTest extends BaseTemplateTest {
    public CommonModuleTest(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    @Test
    public void shouldResponsePing() {
        String value = redisTemplate.execute(new RedisCallback<String>() {
            @Override
            public String doInRedis(RedisConnection redisConnection) throws DataAccessException {
                return redisConnection.ping();
            }
        });
        assertEquals(value, "PONG");
    }

    @Test
    public void shouldDel() {

        v().set("1", "1");
        v().set("2", "2");
        v().set("3", "3");

        Long num = t().delete(Arrays.asList("1", "3"));
        Object vg1 = v().get("1");
        Object vg2 = v().get("2");
        Object vg3 = v().get("3");

        assertEquals(num.longValue(), 2L);
        assertNull(vg1);
        assertNull(vg3);
        assertEquals(vg2, "2");

        v().set("2", "2M");
        assertEquals("2M", v().get("2"));

    }
}
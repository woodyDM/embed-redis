package cn.deepmax.redis.core.cluster;

import cn.deepmax.redis.base.BaseClusterTemplateTest;
import org.junit.Test;
import org.springframework.data.redis.core.RedisTemplate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author wudi
 * @date 2022/1/11
 */
public class HashModuleClusterTest extends BaseClusterTemplateTest {
    public HashModuleClusterTest(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    @Test
    public void shouldHSetAndGet() {
        h().put("key", "a1", "value1");
        Object v = h().get("key", "a1");
        assertEquals(v, "value1");

        assertNull(h().get("not-exist", "any"));
        assertNull(h().get("key", "hashKey-not-exist"));
    }
}

package cn.deepmax.redis.core.module;

import cn.deepmax.redis.base.BaseTemplateTest;
import org.junit.Test;
import org.springframework.data.redis.core.RedisTemplate;

import static org.junit.Assert.assertEquals;

public class StringTest extends BaseTemplateTest {
    
    public StringTest(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    @Test
    public void shouldSetAndGet() {
        String va = "整合Lettuce Redis SpringBoot\uD83D\uDE0A";
        redisTemplate.opsForValue().set("1", va);
        
        String v2 = redisTemplate.opsForValue().get("1").toString();
        
        assertEquals(va, v2);
    }

}

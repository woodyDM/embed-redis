package cn.deepmax.redis.base;

import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.support.EmbedRedisRunner;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * test use engine and template
 */
@RunWith(Parameterized.class)
public abstract class BaseMixedTemplateTest extends BaseTemplateTest implements EngineTest, TimedTest {

    public static Client[] ts;

    public BaseMixedTemplateTest(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    @Parameterized.Parameters
    public static Collection<RedisTemplate<String, Object>> prepareTemplate() {
        return Arrays.stream(ts).filter(Objects::nonNull).map(c -> c.t).collect(Collectors.toList());
    }

    static {
        try {
            initTs();
        } catch (Exception e) {
            log.error("启动失败", e);
            throw new IllegalStateException(e);
        }
    }

    private static void initTs() {
        ts = initStandalone();
    }

    @Before
    public void setUp() {
        engine().flush();
    }

    @Override
    public String auth() {
        return EmbedRedisRunner.AUTH;
    }

    @Override
    public RedisEngine engine() {
        return engine;
    }

}

package cn.deepmax.redis.base;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * test only use template
 */
@RunWith(Parameterized.class)
public abstract class BaseClusterTemplateTest extends BaseTemplateTest {

    public static Client[] ts;

    public BaseClusterTemplateTest(RedisTemplate<String, Object> redisTemplate) {
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
        ts = initCluster();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Before
    public void setUp() {
        t().execute((RedisCallback) con -> {
            con.flushAll();
            con.scriptingCommands().scriptFlush();
            return null;
        });
    }

}

package cn.deepmax.redis.core.module;

import cn.deepmax.redis.base.BaseTemplateTest;
import cn.deepmax.redis.utils.Tuple;
import org.junit.Test;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.Assert.*;

/**
 * @author wudi
 * @date 2021/12/28
 */
public class ListModuleTest extends BaseTemplateTest {
    public ListModuleTest(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    @Test
    public void shouldLPushNil() {
        Long v = l().leftPush("not", "1");

        assertEquals(v.longValue(), 1L);
    }

    @Test
    public void shouldLPush2() {
        Long v1 = l().leftPush("n", "1");
        Long v2 = l().leftPushAll("n", "2", "3", "4");

        assertEquals(v1.longValue(), 1L);
        assertEquals(v2.longValue(), 4L);
    }

    @Test
    public void shouldBLPopWithValue() {
        Long v1 = l().leftPush("n", "1");
        Tuple<Long, Object> b = block(() -> l().leftPop("n", 1, TimeUnit.SECONDS));

        assertEquals(b.b, "1");
        assertTrue(b.a < 100);  //cost no time
    }
    
    @Test
    public void shouldBLPopWithValueAdd() {
        log.info("To schedule");
        ScheduledFuture<?> future = scheduler.schedule(() -> l().leftPush("n", "the_😊"), 300, TimeUnit.MILLISECONDS);

        Tuple<Long, Object> b = block(() -> l().leftPop("n", 1, TimeUnit.SECONDS));

        assertEquals(b.b, "the_😊");
        assertTrue(b.a > 300);  //cost at least 500mills
        assertTrue(b.a < 500);  //cost no more than 1S
        future.cancel(true);
    }

    @Test
    public void shouldBLPopWithTimeout() {
        Tuple<Long, Object> b = block(() -> l().leftPop("n", 300, TimeUnit.MILLISECONDS));

        assertNull(b.b);
        assertTrue(b.a > 300);
    }


    private <T> Tuple<Long, T> block(Supplier<T> action) {
        long start = System.nanoTime();
        T v = action.get();
        long cost = System.nanoTime() - start;
        long mills = cost / 1000_000;
        return new Tuple<>(mills, v);
    }
}
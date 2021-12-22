package cn.deepmax.redis.core.module;

import cn.deepmax.redis.base.BaseTemplateTest;
import org.junit.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
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
    public void shouldResponseWithCommonCommands() {
        assertThat(t().countExistingKeys(Collections.singletonList("key")), is(0L));
        assertThat(t().getExpire("key"), is(-2L));
        assertThat(t().getExpire("key", TimeUnit.MILLISECONDS), is(-2L));
        assertThat(t().delete("key"), is(false));

        v().set("key", "你好");

        assertThat(t().countExistingKeys(Collections.singletonList("key")), is(1L));
        assertThat(t().getExpire("key"), is(-1L));
        assertThat(t().getExpire("key", TimeUnit.MILLISECONDS), is(-1L));
        assertThat(v().get("key"), is("你好"));

        t().expire("key", 5, TimeUnit.SECONDS);
        assertThat(t().getExpire("key"), is(5L));
        assertThat(t().getExpire("key", TimeUnit.MILLISECONDS), is(5000L));
        assertThat(v().get("key"), is("你好"));

        t().expireAt("key", BASE.plusSeconds(15).toInstant(OffsetDateTime.now().getOffset()));
        assertThat(t().getExpire("key"), is(15L));
        assertThat(t().getExpire("key", TimeUnit.MILLISECONDS), is(15_000L));
        assertThat(v().get("key"), is("你好"));

        Boolean pok = t().persist("key");
        assertThat(pok, is(true));
        assertThat(t().countExistingKeys(Collections.singletonList("key")), is(1L));
        assertThat(t().getExpire("key"), is(-1L));
        assertThat(t().getExpire("key", TimeUnit.MILLISECONDS), is(-1L));

        Boolean dok = t().delete("key");
        assertThat(dok, is(true));
        assertThat(t().countExistingKeys(Collections.singletonList("key")), is(0L));
        assertThat(t().getExpire("key"), is(-2L));
        assertThat(t().getExpire("key", TimeUnit.MILLISECONDS), is(-2L));
        assertNull(v().get("key"));

        assertThat(t().delete("key-not-exist"),is(false));
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
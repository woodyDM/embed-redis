package cn.deepmax.redis.core.support;

import cn.deepmax.redis.api.RedisObject;
import cn.deepmax.redis.api.TimeProvider;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.RedisDataType;
import cn.deepmax.redis.support.MockTimeProvider;
import org.junit.Test;

import java.time.LocalDateTime;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

/**
 * @author wudi
 * @date 2021/12/21
 */
public class AbstractRedisObjectTest {

    static LocalDateTime t1 = LocalDateTime.of(2021, 12, 21, 1, 1, 5);
    static LocalDateTime t2 = LocalDateTime.of(2021, 12, 21, 1, 1, 6);
    static LocalDateTime t3 = LocalDateTime.of(2021, 12, 21, 1, 1, 16);

    @Test
    public void shouldOK() {
        MockTimeProvider p = new MockTimeProvider();
        MockObj o = new MockObj(p);
        assertNull(o.expireTime());
        assertThat(o.ttl(), is(-1L));
        p.time = t1;

        o.expire(10);
        assertThat(o.ttl(), is(10L));
        assertThat(o.pttl(), is(10L * 1000));

        o.pexpire(5000);
        assertThat(o.ttl(), is(5L));
        assertThat(o.pttl(), is(5L * 1000));
        assertFalse(o.isExpire());

        //tick 1s
        p.time = t2;
        assertThat(o.ttl(), is(4L));
        assertThat(o.pttl(), is(4L * 1000));

        p.time = t3;
        assertThat(o.ttl(), is(-2L));
        assertThat(o.pttl(), is(-2L));
        assertTrue(o.isExpire());
    }

    static class MockObj extends AbstractRedisObject {
        public MockObj(TimeProvider timeProvider) {
            super(timeProvider);
        }

        @Override
        public RedisObject copyTo(Key key) {
            return this;
        }
        
        @Override
        public Type type() {
            return new RedisDataType("mock","mock");
        }
    }

}
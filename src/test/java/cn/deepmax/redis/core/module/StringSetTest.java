package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.RedisServerException;
import cn.deepmax.redis.resp3.ListRedisMessage;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class StringSetTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldSetParse1() {
        StringModule.Set s = new StringModule.Set();

        ListRedisMessage msg = ListRedisMessage.ofString("set key value");

        assertFalse(s.parseExpire(msg, "EX").isPresent());
        assertFalse(s.parseExpire(msg, "PX").isPresent());
        assertFalse(s.parseFlag(msg, "NX"));
        assertFalse(s.parseFlag(msg, "XX"));
    }

    @Test
    public void shouldSetParse2() {
        StringModule.Set s = new StringModule.Set();

        ListRedisMessage msg = ListRedisMessage.ofString("set key value ex 600 nx");

        Optional<Long> ex = s.parseExpire(msg, "EX");
        assertTrue(ex.isPresent());
        assertThat(ex.get(), is(600L));
        assertFalse(s.parseExpire(msg, "PX").isPresent());
        assertTrue(s.parseFlag(msg, "NX"));
        assertFalse(s.parseFlag(msg, "XX"));
    }

    @Test
    public void shouldSetParse3() {
        StringModule.Set s = new StringModule.Set();

        ListRedisMessage msg = ListRedisMessage.ofString("set key value xx px 600");

        Optional<Long> ex = s.parseExpire(msg, "PX");
        assertTrue(ex.isPresent());
        assertThat(ex.get(), is(600L));
        assertFalse(s.parseExpire(msg, "EX").isPresent());
        assertTrue(s.parseFlag(msg, "XX"));
        assertFalse(s.parseFlag(msg, "NX"));
    }

    @Test
    public void shouldError1() {
        StringModule.Set s = new StringModule.Set();

        ListRedisMessage msg = ListRedisMessage.ofString("set key value px");
        expectedException.expect(RedisServerException.class);
        expectedException.expectMessage("ERR syntax error");

        s.parseExpire(msg, "PX");

    }

    @Test
    public void shouldError2() {
        StringModule.Set s = new StringModule.Set();

        ListRedisMessage msg = ListRedisMessage.ofString("set key value px abc");
        expectedException.expect(RedisServerException.class);
        expectedException.expectMessage("ERR value is not an integer or out of range");

        s.parseExpire(msg, "PX");

    }

    @Test
    public void shouldError3() {
        StringModule.Set s = new StringModule.Set();

        ListRedisMessage msg = ListRedisMessage.ofString("set key value px 100 ex 5");
        expectedException.expect(RedisServerException.class);
        expectedException.expectMessage("ERR syntax error");

        s.parseExpire(msg);

    }

    @Test
    public void shouldParseE_1() {
        StringModule.Set s = new StringModule.Set();

        ListRedisMessage msg = ListRedisMessage.ofString("set key value px 1000");

        assertThat(s.parseExpire(msg).get(), is(1000L));
    }

    @Test
    public void shouldParseE_2() {
        StringModule.Set s = new StringModule.Set();

        ListRedisMessage msg = ListRedisMessage.ofString("set key value ex 5");

        assertThat(s.parseExpire(msg).get(), is(5000L));
    }

}

package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.RedisServerException;
import cn.deepmax.redis.base.BaseTemplateTest;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.data.redis.core.RedisTemplate;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.*;

/**
 * @author wudi
 * @date 2021/12/22
 */
public class StringModuleTest extends BaseTemplateTest {
    public StringModuleTest(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

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

    @Test
    public void shouldSetEx() {
        ListRedisMessage msg = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好"))
                .append("ex")
                .append("3600").build();
        RedisMessage resp = engine().execute(msg, embeddedClient());

        SimpleStringRedisMessage m = (SimpleStringRedisMessage) resp;
        assertThat(m.content(), is("OK"));
        assertThat(v().get("key"), is("好"));
        assertThat(t().getExpire("key"), is(3600L));

    }

    @Test
    public void shouldSetNxEx() {
        ListRedisMessage msg = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好"))
                .append("nx")
                .append("ex")
                .append("3600").build();
        RedisMessage resp = engine().execute(msg, embeddedClient());

        SimpleStringRedisMessage m = (SimpleStringRedisMessage) resp;
        assertThat(m.content(), is("OK"));
        assertThat(v().get("key"), is("好"));
        assertThat(t().getExpire("key"), is(3600L));

    }


    @Test
    public void shouldSetPx() {
        ListRedisMessage msg = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好"))
                .append("px")
                .append("6000").build();
        RedisMessage resp = engine().execute(msg, embeddedClient());

        SimpleStringRedisMessage m = (SimpleStringRedisMessage) resp;
        assertThat(m.content(), is("OK"));
        assertThat(v().get("key"), is("好"));
        assertThat(t().getExpire("key"), is(6L));

    }

    @Test
    public void shouldSetNxPx() {
        ListRedisMessage msg = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好"))
                .append("nx")
                .append("px")
                .append("6000").build();
        RedisMessage resp = engine().execute(msg, embeddedClient());

        SimpleStringRedisMessage m = (SimpleStringRedisMessage) resp;
        assertThat(m.content(), is("OK"));
        assertThat(v().get("key"), is("好"));
        assertThat(t().getExpire("key"), is(6L));

    }

    @Test
    public void shouldSetNxFail() {
        v().set("key", "hahahah哈哈");

        ListRedisMessage msg = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好"))
                .append("nx")
                .append("3600").build();
        RedisMessage resp = engine().execute(msg, embeddedClient());

        assertThat(resp, sameInstance(FullBulkStringRedisMessage.NULL_INSTANCE));

        assertThat(v().get("key"), is("hahahah哈哈"));
    }

    @Test
    public void shouldSetXXWithoutKey() {
        ListRedisMessage msg = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好"))
                .append("xx")
                .append("ex")
                .append("3600").build();
        RedisMessage resp = engine().execute(msg, embeddedClient());

        assertThat(resp, sameInstance(FullBulkStringRedisMessage.NULL_INSTANCE));
        assertNull(v().get("key"));
    }


    @Test
    public void shouldSetXXWithEx() {
        ListRedisMessage msg = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好"))
                .append("px")
                .append("6000").build();
        engine().execute(msg, embeddedClient());

        ListRedisMessage msg2 = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好2"))
                .append("xx")
                .append("px")
                .append("8000").build();
        RedisMessage resp2 = engine().execute(msg2, embeddedClient());

        SimpleStringRedisMessage m = (SimpleStringRedisMessage) resp2;
        assertThat(m.content(), is("OK"));
        assertThat(v().get("key"), is("好2"));
        assertThat(t().getExpire("key"), is(8L));

    }

    @Test
    public void shouldSetEx8KeepTtl() {
        ListRedisMessage msg = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好"))
                .append("px")
                .append("6000").build();
        engine().execute(msg, embeddedClient());

        ListRedisMessage msg2 = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好2"))
                .append("xx")
                .append("keepttl")
                .build();
        RedisMessage resp2 = engine().execute(msg2, embeddedClient());

        SimpleStringRedisMessage m = (SimpleStringRedisMessage) resp2;
        assertThat(m.content(), is("OK"));
        assertThat(v().get("key"), is("好2"));
        assertThat(t().getExpire("key"), is(6L));

    }

    @Test
    public void shouldSetErrorWithPxAndEx() {
        ListRedisMessage msg = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好"))
                .append("px")
                .append("6000")
                .append("ex")
                .append("5").build();
        RedisMessage msg2 = engine().execute(msg, embeddedClient());
        
        
        ErrorRedisMessage m = (ErrorRedisMessage) msg2;
        assertThat(m.content(), is("ERR syntax error"));
    }

    @Test
    public void shouldSetErrorWithPxAndKeepttl() {
        ListRedisMessage msg = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好"))
                .append("px")
                .append("6000")
                .append("keepttl")
                .build();
        RedisMessage msg2 = engine().execute(msg, embeddedClient());


        ErrorRedisMessage m = (ErrorRedisMessage) msg2;
        assertThat(m.content(), is("ERR syntax error"));
    }
    // ------ setnx ------
    @Test
    public void shouldSetNx() {
        Boolean ok = v().setIfAbsent("key", "hello你好");
        
        assertTrue(ok);
        assertThat(v().get("key"),is("hello你好"));
    }

    @Test
    public void shouldSetNxWithExist() {
        v().set("key","OK");
        Boolean ok = v().setIfAbsent("key", "hello你好");

        assertFalse(ok);
        assertThat(v().get("key"),is("OK"));
    }
    
    /*  setex */
    @Test
    public void shouldSetExT() {
        v().set("key", "hello你好",15, TimeUnit.SECONDS);
        
        assertThat(v().get("key"),is("hello你好"));
        assertThat(t().getExpire("key"),is(15L));
    }
    /*  append */
    @Test
    public void shouldAppendNotExist() {
        Integer len = v().append("key", "123");
        t().expire("key", 25, TimeUnit.SECONDS);
        RedisMessage resp = engine().execute(ListRedisMessage.ofString("get key"), embeddedClient());
        
        assertThat(len, is(3));
        assertThat(((FullBulkStringRedisMessage)resp).content().toString(StandardCharsets.UTF_8),is("123"));

        Integer len2 = v().append("key", " 456你");
        RedisMessage resp2 = engine().execute(ListRedisMessage.ofString("get key"), embeddedClient());

        assertThat(len2, is(3+4+3));
        assertThat(((FullBulkStringRedisMessage)resp2).content().toString(StandardCharsets.UTF_8),is("123 456你"));
        assertThat(t().getExpire("key"),is(25L));
    }

 
}
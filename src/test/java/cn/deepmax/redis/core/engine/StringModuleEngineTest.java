package cn.deepmax.redis.core.engine;

import cn.deepmax.redis.base.BaseMemEngineTest;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.handler.codec.redis.*;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.*;

/**
 * @author wudi
 * @date 2022/1/4
 */
public class StringModuleEngineTest extends BaseMemEngineTest {

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
        assertThat(get("key"), is(serialize("好")));
        assertThat(getExpire("key"), is(3600L));

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
        assertThat(get("key"), is(serialize("好")));
        assertThat(getExpire("key"), is(3600L));

    }


    @Test
    public void shouldSetPx() {
        ExpectedEvents events = listen("key");
        ListRedisMessage msg = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好"))
                .append("px")
                .append("6000").build();
        RedisMessage resp = engine().execute(msg, embeddedClient());

        SimpleStringRedisMessage m = (SimpleStringRedisMessage) resp;
        assertThat(m.content(), is("OK"));
        assertThat(get("key"), is(serialize("好")));
        assertThat(getExpire("key"), is(6L));
        assertEquals(events.triggerTimes, 1);
        assertEquals(events.events.size(), 1);

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
        assertThat(get("key"), is(serialize("好")));
        assertThat(getExpire("key"), is(6L));

    }

    @Test
    public void shouldSetNxFail() {
        set("key", "hahahah哈哈");

        ExpectedEvents events = listen("key");
        ListRedisMessage msg = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好"))
                .append("nx")
                .append("3600").build();
        RedisMessage resp = engine().execute(msg, embeddedClient());

        assertThat(resp, sameInstance(FullBulkStringRedisMessage.NULL_INSTANCE));

        assertThat(get("key"), is(bytes("hahahah哈哈")));
        assertEquals(events.triggerTimes, 0);
    }

    @Test
    public void shouldSetXXWithoutKey() {
        ExpectedEvents events = listen("key");
        ListRedisMessage msg = ListRedisMessage.newBuilder()
                .append("set")
                .append("key")
                .append(serialize("好"))
                .append("xx")
                .append("ex")
                .append("3600").build();
        RedisMessage resp = engine().execute(msg, embeddedClient());

        assertThat(resp, sameInstance(FullBulkStringRedisMessage.NULL_INSTANCE));
        assertNull(get("key"));
        assertEquals(events.triggerTimes, 0);

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
        assertThat(get("key"), is(serialize("好2")));
        assertThat(getExpire("key"), is(8L));

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
        assertThat(get("key"), is(serialize("好2")));
        assertThat(getExpire("key"), is(6L));

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

    /*  append */
    @Test
    public void shouldAppendNotExist() {
        IntegerRedisMessage len = exec("append key 123");
        exec("expire key 25");

        RedisMessage resp = engine().execute(ListRedisMessage.ofString("get key"), embeddedClient());

        assertThat(len.value(), is(3L));
        assertThat(((FullBulkStringRedisMessage) resp).content().toString(StandardCharsets.UTF_8), is("123"));

        len = exec("append key 456你");

        RedisMessage resp2 = engine().execute(ListRedisMessage.ofString("get key"), embeddedClient());
        //old + 456 + 你
        assertThat((int) len.value(), is(3 + 3 + 3));
        assertThat(((FullBulkStringRedisMessage) resp2).content().toString(StandardCharsets.UTF_8), is("123456你"));
        assertThat(getExpire("key"), is(25L));
    }

    /*   incr incrby decr decrby */
    @Test
    public void shouldIncrAndDecr() {
        RedisMessage resp = engine().execute(ListRedisMessage.ofString("incr key"), embeddedClient());
        RedisMessage resp2 = engine().execute(ListRedisMessage.ofString("get key"), embeddedClient());
        assertThat(((IntegerRedisMessage) resp).value(), is(1L));
        assertThat(((FullBulkStringRedisMessage) resp2).content().toString(StandardCharsets.UTF_8), is("1"));

        resp = engine().execute(ListRedisMessage.ofString("incr key"), embeddedClient());
        resp2 = engine().execute(ListRedisMessage.ofString("get key"), embeddedClient());
        assertThat(((IntegerRedisMessage) resp).value(), is(2L));
        assertThat(((FullBulkStringRedisMessage) resp2).content().toString(StandardCharsets.UTF_8), is("2"));

        exec("expire key 50");
        resp = engine().execute(ListRedisMessage.ofString("incrby key 10"), embeddedClient());
        resp2 = engine().execute(ListRedisMessage.ofString("get key"), embeddedClient());
        assertThat(((IntegerRedisMessage) resp).value(), is(12L));
        assertThat(((FullBulkStringRedisMessage) resp2).content().toString(StandardCharsets.UTF_8), is("12"));

        resp = engine().execute(ListRedisMessage.ofString("decr key"), embeddedClient());
        resp2 = engine().execute(ListRedisMessage.ofString("get key"), embeddedClient());
        assertThat(((IntegerRedisMessage) resp).value(), is(11L));
        assertThat(((FullBulkStringRedisMessage) resp2).content().toString(StandardCharsets.UTF_8), is("11"));

        resp = engine().execute(ListRedisMessage.ofString("decrby key 5"), embeddedClient());
        resp2 = engine().execute(ListRedisMessage.ofString("get key"), embeddedClient());
        assertThat(((IntegerRedisMessage) resp).value(), is(6L));
        assertThat(((FullBulkStringRedisMessage) resp2).content().toString(StandardCharsets.UTF_8), is("6"));

        //errors
        resp = engine().execute(ListRedisMessage.ofString("incrby key 2332e2"), embeddedClient());
        assertTrue(((ErrorRedisMessage) resp).content().contains("ERR value is not an integer or out of range"));

    }

    @Test
    public void shouldIncrError() {
        RedisMessage resp = engine().execute(ListRedisMessage.ofString("incrby key 23aaa"), embeddedClient());
        assertTrue(((ErrorRedisMessage) resp).content().contains("ERR value is not an integer or out of range"));

        RedisMessage resp2 = engine().execute(ListRedisMessage.ofString("decrby key 23bee1"), embeddedClient());
        assertTrue(((ErrorRedisMessage) resp2).content().contains("ERR value is not an integer or out of range"));
    }

}

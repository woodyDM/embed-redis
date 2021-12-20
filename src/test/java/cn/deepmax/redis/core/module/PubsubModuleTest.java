package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.base.BaseEngineTest;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.type.CompositeRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static cn.deepmax.redis.resp3.RedisCodecTestUtil.readAllMessage;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;


/**
 * @author wudi
 * @date 2021/12/17
 */
public class PubsubModuleTest extends BaseEngineTest {

    @Test
    public void shouldOK() {
        Redis.Client c1 = embeddedClient();
        Redis.Client c2 = embeddedClient();
        Redis.Client c3 = embeddedClient();

        RedisMessage m1 = engine().execute(ListRedisMessage.ofString("subscribe 123 abc"), c1);
        RedisMessage m2 = engine().execute(ListRedisMessage.ofString("psubscribe 12? 1*23 1*23 1[24]3"), c2);
        RedisMessage m2_2 = engine().execute(ListRedisMessage.ofString("psubscribe abc"), c2);
        RedisMessage m3 = engine().execute(ListRedisMessage.ofString("publish 123 hahaha😯"), c3);

        List<RedisMessage> msg1 = readAllMessage(c1.channel());
        List<RedisMessage> msg2 = readAllMessage(c2.channel());

        //assert sub 1
        assertTrue(m1 instanceof CompositeRedisMessage);
        List<RedisMessage> ml1 = ((CompositeRedisMessage) m1).children();
        assertEquals(ml1.size(), 2);
        assertTrue(ml1.get(0) instanceof ListRedisMessage);
        assertEquals(((ListRedisMessage) ml1.get(0)).getAt(0).str(), "subscribe");
        assertEquals(((ListRedisMessage) ml1.get(0)).getAt(1).str(), "123");
        assertEquals(((IntegerRedisMessage) ((ListRedisMessage) ml1.get(0)).children().get(2)).value(), 1L);
        assertTrue(ml1.get(1) instanceof ListRedisMessage);
        assertEquals(((ListRedisMessage) ml1.get(1)).getAt(0).str(), "subscribe");
        assertEquals(((ListRedisMessage) ml1.get(1)).getAt(1).str(), "abc");
        assertEquals(((IntegerRedisMessage) ((ListRedisMessage) ml1.get(1)).children().get(2)).value(), 2L);
        //assert psub 
        assertTrue(m2 instanceof CompositeRedisMessage);
        List<RedisMessage> ml2 = ((CompositeRedisMessage) m2).children();
        assertEquals(ml2.size(), 4);
        assertTrue(ml2.get(0) instanceof ListRedisMessage);
        assertEquals(((ListRedisMessage) ml2.get(0)).getAt(0).str(), "psubscribe");
        assertEquals(((ListRedisMessage) ml2.get(0)).getAt(1).str(), "12?");
        assertEquals(((IntegerRedisMessage) ((ListRedisMessage) ml2.get(0)).children().get(2)).value(), 1L);
        assertTrue(ml2.get(1) instanceof ListRedisMessage);
        assertEquals(((ListRedisMessage) ml2.get(1)).getAt(0).str(), "psubscribe");
        assertEquals(((ListRedisMessage) ml2.get(1)).getAt(1).str(), "1*23");
        assertEquals(((IntegerRedisMessage) ((ListRedisMessage) ml2.get(1)).children().get(2)).value(), 2L);
        assertTrue(ml2.get(2) instanceof ListRedisMessage);
        assertEquals(((ListRedisMessage) ml2.get(2)).getAt(0).str(), "psubscribe");
        assertEquals(((ListRedisMessage) ml2.get(2)).getAt(1).str(), "1*23");
        assertEquals(((IntegerRedisMessage) ((ListRedisMessage) ml2.get(2)).children().get(2)).value(), 2L);
        assertTrue(ml2.get(3) instanceof ListRedisMessage);
        assertEquals(((ListRedisMessage) ml2.get(3)).getAt(0).str(), "psubscribe");
        assertEquals(((ListRedisMessage) ml2.get(3)).getAt(1).str(), "1[24]3");
        assertEquals(((IntegerRedisMessage) ((ListRedisMessage) ml2.get(3)).children().get(2)).value(), 3L);
        //assert psub 2
        assertTrue(m2_2 instanceof CompositeRedisMessage);
        List<RedisMessage> ml2_2 = ((CompositeRedisMessage) m2_2).children();
        assertThat(ml2_2.size(),is(1));
        assertTrue(ml2_2.get(0) instanceof ListRedisMessage);
        assertEquals(((ListRedisMessage) ml2_2.get(0)).getAt(0).str(), "psubscribe");
        assertEquals(((ListRedisMessage) ml2_2.get(0)).getAt(1).str(), "abc");
        assertEquals(((IntegerRedisMessage) ((ListRedisMessage) ml2_2.get(0)).children().get(2)).value(), 4L);

        //assert pub
        assertTrue(m3 instanceof IntegerRedisMessage);
        assertEquals(((IntegerRedisMessage) m3).value(), 4);
        //assert sub msg 1
        assertThat(msg1.size(),is(1));
        assertTrue(msg1.get(0) instanceof ListRedisMessage);
        assertEquals(((ListRedisMessage) msg1.get(0)).getAt(0).str(), "message");
        assertEquals(((ListRedisMessage) msg1.get(0)).getAt(1).str(), "123");
        assertThat(((ListRedisMessage) msg1.get(0)).getAt(2).bytes(), is("hahaha😯".getBytes(StandardCharsets.UTF_8)));
        //assert sub msg 2
        assertThat(msg2.size(),is(3));
        assertTrue(msg2.get(0) instanceof ListRedisMessage);
        assertEquals(((ListRedisMessage) msg2.get(0)).getAt(0).str(), "pmessage");
        assertEquals(((ListRedisMessage) msg2.get(0)).getAt(1).str(), "12?");
        assertEquals(((ListRedisMessage) msg2.get(0)).getAt(2).str(), "123");
        assertThat(((ListRedisMessage) msg2.get(0)).getAt(3).bytes(), is("hahaha😯".getBytes(StandardCharsets.UTF_8)));
        assertTrue(msg2.get(1) instanceof ListRedisMessage);
        assertEquals(((ListRedisMessage) msg2.get(1)).getAt(0).str(), "pmessage");
        assertEquals(((ListRedisMessage) msg2.get(1)).getAt(1).str(), "1*23");
        assertEquals(((ListRedisMessage) msg2.get(1)).getAt(2).str(), "123");
        assertThat(((ListRedisMessage) msg2.get(1)).getAt(3).bytes(), is("hahaha😯".getBytes(StandardCharsets.UTF_8)));
        assertTrue(msg2.get(2) instanceof ListRedisMessage);
        assertEquals(((ListRedisMessage) msg2.get(2)).getAt(0).str(), "pmessage");
        assertEquals(((ListRedisMessage) msg2.get(2)).getAt(1).str(), "1[24]3");
        assertEquals(((ListRedisMessage) msg2.get(2)).getAt(2).str(), "123");
        assertThat(((ListRedisMessage) msg2.get(2)).getAt(3).bytes(), is("hahaha😯".getBytes(StandardCharsets.UTF_8)));
        //then unsub
        //todo
        RedisMessage m11 = engine().execute(ListRedisMessage.ofString("unsubscribe 123"), c1);
        RedisMessage m22 = engine().execute(ListRedisMessage.ofString("punsubscribe 12? 1[24]3"), c2);
        RedisMessage m33 = engine().execute(ListRedisMessage.ofString("publish 123 hahaha😯"), c3);
        //assert unsub m11
        //assertThat(m11 ,is(instanceOf(SimpleStringRedisMessage.class)));


    }
}
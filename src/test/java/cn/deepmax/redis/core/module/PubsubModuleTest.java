package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.base.BaseEngineTest;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.type.CompositeRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static cn.deepmax.redis.resp3.RedisCodecTestUtil.readAllMessage;
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
        RedisMessage m2 = engine().execute(ListRedisMessage.ofString("psubscribe 12? 1*23 1*23 1[24]3 abc"), c2);
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
        assertEquals(ml2.size(), 5);
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
        //assert pub
        assertTrue(m3 instanceof IntegerRedisMessage);
        assertEquals(((IntegerRedisMessage) m3).value(), 4);
        //assert sub msg 1
        assertThat(msg1.size(),is(1));
        assertTrue(msg1.get(0) instanceof ListRedisMessage);
        assertEquals(((ListRedisMessage) msg1.get(0)).getAt(0).str(), "message");
        assertEquals(((ListRedisMessage) msg1.get(0)).getAt(1).str(), "123");
        assertThat(((ListRedisMessage) msg1.get(0)).getAt(2).bytes(), is("hahaha😯".getBytes(StandardCharsets.UTF_8)));


    }
}
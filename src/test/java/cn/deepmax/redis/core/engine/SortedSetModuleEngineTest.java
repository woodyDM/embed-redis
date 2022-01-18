package cn.deepmax.redis.core.engine;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.DbManager;
import cn.deepmax.redis.base.BaseMemEngineTest;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/**
 * @author wudi
 */
public class SortedSetModuleEngineTest extends BaseMemEngineTest {
    @Test
    public void shouldErrorWhenZDiffArgNumberKeysMismatch() {
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("zdiff 4 key1"), embeddedClient());

        assertSame(msg, Constants.ERR_SYNTAX);
    }

    @Test
    public void shouldErrorWhenZDiffArgNumberKeysMismatch2() {
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("zdiff 2 key key2 key3"), embeddedClient());

        assertSame(msg, Constants.ERR_SYNTAX);
    }

    @Test
    public void shouldErrorWhenZDiffStoreArgNumberKeysMismatch() {
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("zdiffstore dest 4 key1"), embeddedClient());

        assertSame(msg, Constants.ERR_SYNTAX);
    }

    @Test
    public void shouldErrorWhenZDiffStoreArgNumberKeysMismatch2() {
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("zdiffstore dest 2 key key2 key3"), embeddedClient());

        assertSame(msg, Constants.ERR_SYNTAX);
    }

    @Test
    public void shouldZDiffEmpty() {
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("zdiff 3 key key2 key3"), embeddedClient());

        assertEquals(((ListRedisMessage) msg).children().size(), 0);
    }

    @Test
    public void shouldZDiffStoreEmpty() {
        RedisMessage msg = engine().execute(ListRedisMessage.ofString("zdiffstore dest 3 key key2 key3"), embeddedClient());

        assertEquals(((IntegerRedisMessage) msg).value(), 0L);
    }

    @Test
    public void shouldZIncreBy() {
        ExpectedEvents events = listen("key");

        RedisMessage msg = engine().execute(ListRedisMessage.ofString("zincrby key 0.25 m"), embeddedClient());
        assertEquals(((FullBulkValueRedisMessage) msg).str(), "0.25");
        assertEquals(events.triggerTimes, 1);
        assertEquals(events.events.get(0).type, DbManager.EventType.NEW_OR_REPLACE);
    }


    @Test
    public void shouldZIncreBy2() {
        engine().execute(ListRedisMessage.ofString("zadd key 0.25 m"), embeddedClient());

        ExpectedEvents events = listen("key");

        RedisMessage msg = engine().execute(ListRedisMessage.ofString("zincrby key -0.45 m"), embeddedClient());
        assertEquals(((FullBulkValueRedisMessage) msg).str(), "-0.2");
        assertEquals(events.triggerTimes, 1);
        assertEquals(events.events.get(0).type, DbManager.EventType.UPDATE);
    }

    @Test
    public void shouldZIncreBy3() {
        engine().execute(ListRedisMessage.ofString("zadd key 0.25 m1"), embeddedClient());

        ExpectedEvents events = listen("key");

        RedisMessage msg = engine().execute(ListRedisMessage.ofString("zincrby key -0.45 m2"), embeddedClient());
        RedisMessage zcard = engine().execute(ListRedisMessage.ofString("zcard key"), embeddedClient());

        assertEquals(((IntegerRedisMessage) zcard).value(), 2);
        assertEquals(((FullBulkValueRedisMessage) msg).str(), "-0.45");
        assertEquals(events.triggerTimes, 1);
        assertEquals(events.events.get(0).type, DbManager.EventType.UPDATE);
    }

    @Test
    public void shouldZAddIncr() {
        engine().execute(ListRedisMessage.ofString("zadd key 0.25 m1"), embeddedClient());

        ExpectedEvents events = listen("key");

        RedisMessage msg = engine().execute(ListRedisMessage.ofString("zadd key incr -0.45 m1"), embeddedClient());
        assertEquals(((FullBulkValueRedisMessage)msg).str(),"-0.2");
        assertEquals(events.triggerTimes, 1);
        assertEquals(events.events.get(0).type, DbManager.EventType.UPDATE);
    }

    @Test
    public void shouldZAddIncr2() {
        engine().execute(ListRedisMessage.ofString("zadd key 0.25 m1"), embeddedClient());

        ExpectedEvents events = listen("key");

        RedisMessage msg = engine().execute(ListRedisMessage.ofString("zadd key nx incr -0.45 m2"), embeddedClient());
        assertEquals(((FullBulkValueRedisMessage)msg).str(),"-0.45");
        assertEquals(events.triggerTimes, 1);
        assertEquals(events.events.get(0).type, DbManager.EventType.UPDATE);
    }

    @Test
    public void shouldZAddIncr3() {
        engine().execute(ListRedisMessage.ofString("zadd key 0.25 m1"), embeddedClient());

        ExpectedEvents events = listen("key");

        RedisMessage msg = engine().execute(ListRedisMessage.ofString("zadd key nx incr -0.45 m1"), embeddedClient());
        assertEquals(msg,FullBulkValueRedisMessage.NULL_INSTANCE);
        assertEquals(events.triggerTimes, 0);
    }

    @Test
    public void shouldZAddIncr4() {
        engine().execute(ListRedisMessage.ofString("zadd key 0.25 m2"), embeddedClient());

        ExpectedEvents events = listen("key");

        RedisMessage msg = engine().execute(ListRedisMessage.ofString("zadd key xx incr -0.45 m1"), embeddedClient());
        assertEquals(msg,FullBulkValueRedisMessage.NULL_INSTANCE);
        assertEquals(events.triggerTimes, 0);
    }

}

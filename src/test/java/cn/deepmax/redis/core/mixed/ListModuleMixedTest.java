package cn.deepmax.redis.core.mixed;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.DbManager;
import cn.deepmax.redis.base.BaseMixedTemplateTest;
import cn.deepmax.redis.base.BlockTest;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.utils.Tuple;
import io.netty.handler.codec.redis.RedisMessage;
import org.junit.Test;
import org.springframework.data.redis.connection.RedisListCommands;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

/**
 * @author wudi
 */
public class ListModuleMixedTest extends BaseMixedTemplateTest implements BlockTest {
    public ListModuleMixedTest(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    @Test
    public void shouldBLPopWithValue() {
        Long v1 = l().leftPush("n", "1");
        Tuple<Long, Object> b = block(() -> l().leftPop("n", 1, TimeUnit.SECONDS));

        assertEquals(b.b, "1");
        assertTrue(b.a < 100);  //cost no time
        assertThat(engine.getDbManager().listenerSize(), is(0));
    }
    
    @Test
    public void shouldBLPopWithValueAdd() {
        ScheduledFuture<?> future = scheduler.schedule(() -> {
            engine.execute(ListRedisMessage.newBuilder()
                    .append(FullBulkValueRedisMessage.ofString("lpush"))
                    .append(FullBulkValueRedisMessage.ofString("n"))
                    .append(FullBulkValueRedisMessage.ofString(serialize("001")))
                    .append(FullBulkValueRedisMessage.ofString(serialize("002"))).build(), embeddedClient());
        }, 300, TimeUnit.MILLISECONDS);

        Tuple<Long, Object> b = block(() -> l().leftPop("n", 1, TimeUnit.SECONDS));

        assertEquals(b.b, "002");
        assertTrue(b.a > 290);  //cost at least 500mills
        assertTrue(b.a < 500);  //cost no more than 1S
        assertThat(engine.getDbManager().listenerSize(), is(0));
        future.cancel(true);

    }

    @Test
    public void shouldBRPopWithValueAdd() {
        ScheduledFuture<?> future = scheduler.schedule(() -> {
            engine.execute(ListRedisMessage.newBuilder()
                    .append(FullBulkValueRedisMessage.ofString("lpush"))
                    .append(FullBulkValueRedisMessage.ofString("n"))
                    .append(FullBulkValueRedisMessage.ofString(serialize("001")))
                    .append(FullBulkValueRedisMessage.ofString(serialize("002"))).build(), embeddedClient());
        }, 300, TimeUnit.MILLISECONDS);

        Tuple<Long, Object> b = block(() -> l().rightPop("n", 1, TimeUnit.SECONDS));

        assertEquals(b.b, "001");
        assertTrue(b.a > 300);  //cost at least 500mills
        assertTrue(b.a < 500);  //cost no more than 1S
        assertThat(engine.getDbManager().listenerSize(), is(0));
        future.cancel(true);
    }

    @Test
    public void shouldBLPopWithTimeout() {
        Tuple<Long, Object> b = block(() -> l().leftPop("n", 300, TimeUnit.MILLISECONDS));

        assertNull(b.b);
        assertTrue(b.a > 300);
        assertThat(engine.getDbManager().listenerSize(), is(0));
    }

    @Test
    public void shouldLPopCount() {
        l().leftPush("key", "1");
        l().leftPush("key", "2");
        l().leftPush("key", "3");

        RedisMessage msg = engine.execute(ListRedisMessage.ofString("lpop key 4"), embeddedClient());
        ListRedisMessage list = (ListRedisMessage) msg;
        assertThat(list.children().size(), is(3));
        assertThat(list.getAt(0).bytes(), is(serialize("3")));
        assertThat(list.getAt(1).bytes(), is(serialize("2")));
        assertThat(list.getAt(2).bytes(), is(serialize("1")));
    }

    @Test
    public void shouldLPopCountSize1() {
        l().leftPush("key", "1");
        l().leftPush("key", "2");
        l().leftPush("key", "3");

        RedisMessage msg = engine.execute(ListRedisMessage.ofString("lpop key 1"), embeddedClient());
        ListRedisMessage list = (ListRedisMessage) msg;
        assertThat(list.children().size(), is(1));
        assertThat(list.getAt(0).bytes(), is(serialize("3")));
    }

    @Test
    public void shouldLPopNoCount() {
        l().leftPush("key", "1");
        l().leftPush("key", "2");
        l().leftPush("key", "3");

        RedisMessage msg = engine.execute(ListRedisMessage.ofString("lpop key"), embeddedClient());
        FullBulkValueRedisMessage m = (FullBulkValueRedisMessage) msg;

        assertThat(m.bytes(), is(serialize("3")));
    }


    @Test
    public void shouldRPopCount() {
        l().leftPush("key", "1");
        l().rightPush("key", "2");
        l().leftPush("key", "3");

        RedisMessage msg = engine.execute(ListRedisMessage.ofString("rpop key 4"), embeddedClient());
        ListRedisMessage list = (ListRedisMessage) msg;
        assertThat(list.children().size(), is(3));
        assertThat(list.getAt(0).bytes(), is(serialize("2")));
        assertThat(list.getAt(1).bytes(), is(serialize("1")));
        assertThat(list.getAt(2).bytes(), is(serialize("3")));
    }

    @Test
    public void shouldRPopCountSize1() {
        l().leftPush("key", "1");
        l().leftPush("key", "2");
        l().leftPush("key", "3");

        RedisMessage msg = engine.execute(ListRedisMessage.ofString("rpop key 1"), embeddedClient());
        ListRedisMessage list = (ListRedisMessage) msg;
        assertThat(list.children().size(), is(1));
        assertThat(list.getAt(0).bytes(), is(serialize("1")));
    }

    @Test
    public void shouldRPopNoCount() {
        l().leftPush("key", "1");
        l().leftPush("key", "2");
        l().leftPush("key", "3");

        RedisMessage msg = engine.execute(ListRedisMessage.ofString("rpop key"), embeddedClient());
        FullBulkValueRedisMessage m = (FullBulkValueRedisMessage) msg;

        assertThat(m.bytes(), is(serialize("1")));
    }

    @Test
    public void shouldRpopLpushNormal() {
        l().rightPushAll("source", "a", "b");
        l().rightPushAll("dest", "1", "2");

        ExpectedEvents e1 = listen("source");
        ExpectedEvents e2 = listen("dest");

        Object o = l().rightPopAndLeftPush("source", "dest");

        assertEquals(o, "b");
        assertThat(l().size("source").intValue(), is(1));
        assertThat(l().size("dest").intValue(), is(3));
        assertThat(e1.events.size(), is(2));
        assertThat(e1.triggerTimes, is(1));
        assertTrue(e1.events.stream().allMatch(e -> e.type == DbManager.EventType.UPDATE));
        assertThat(e2.events.size(), is(2));
        assertThat(e2.triggerTimes, is(1));
        assertTrue(e2.events.stream().allMatch(e -> e.type == DbManager.EventType.UPDATE));
        assertThat(l().leftPop("dest"), equalTo("b"));
    }

    @Test
    public void shouldLpopLpushNormal() {
        l().rightPushAll("source", "a", "b");
        l().rightPushAll("dest", "1", "2");

        ExpectedEvents e1 = listen("source");
        ExpectedEvents e2 = listen("dest");

        RedisMessage msg = engine.execute(ListRedisMessage.ofString("lmove source dest left left"), embeddedClient());

        assertThat(((FullBulkValueRedisMessage) msg).bytes(), is(serialize("a")));
        assertThat(l().size("source").intValue(), is(1));
        assertThat(l().size("dest").intValue(), is(3));
        assertThat(e1.events.size(), is(2));
        assertTrue(e1.events.stream().allMatch(e -> e.type == DbManager.EventType.UPDATE));
        assertThat(e1.triggerTimes, is(1));

        assertThat(e2.events.size(), is(2));
        assertTrue(e2.events.stream().allMatch(e -> e.type == DbManager.EventType.UPDATE));
        assertThat(e2.triggerTimes, is(1));
    }

    @Test
    public void shouldLpopLpushSame() {
        l().rightPushAll("source", "a", "b", "c", "d");

        ExpectedEvents e1 = listen("source");

        RedisMessage msg = engine.execute(ListRedisMessage.ofString("lmove source source left right"), embeddedClient());

        assertThat(((FullBulkValueRedisMessage) msg).bytes(), is(serialize("a")));
        assertThat(l().size("source").intValue(), is(4));

        assertThat(e1.events.size(), is(1));
        assertTrue(e1.events.stream().allMatch(e -> e.type == DbManager.EventType.UPDATE));
        assertThat(e1.triggerTimes, is(1));
    }

    @Test
    public void shouldErrorLpopLpush() {
        RedisMessage msg = engine.execute(ListRedisMessage.ofString("lmove source source left w"), embeddedClient());

        assertThat(msg, sameInstance(Constants.ERR_SYNTAX));
    }

    @Test
    public void shouldRpopLpushNormal_Lmove() {
        l().rightPushAll("source", "a", "b");
        l().rightPushAll("dest", "1", "2");

        ExpectedEvents e1 = listen("source");
        ExpectedEvents e2 = listen("dest");

        RedisMessage msg = engine.execute(ListRedisMessage.ofString("lmove source dest right left"), embeddedClient());

        assertThat(((FullBulkValueRedisMessage) msg).bytes(), is(serialize("b")));
        assertThat(l().size("source").intValue(), is(1));
        assertThat(l().size("dest").intValue(), is(3));
        assertThat(e1.events.size(), is(2));
        assertTrue(e1.events.stream().allMatch(e -> e.type == DbManager.EventType.UPDATE));
        assertThat(e1.triggerTimes, is(1));

        assertThat(e2.events.size(), is(2));
        assertTrue(e2.events.stream().allMatch(e -> e.type == DbManager.EventType.UPDATE));
        assertThat(e2.triggerTimes, is(1));
    }

    @Test
    public void shouldRpopLpushRNotNil() {
        l().rightPushAll("source", "a", "b", "c");

        ExpectedEvents e1 = listen("source");
        ExpectedEvents e2 = listen("dest");

        Object o = l().rightPopAndLeftPush("source", "dest");

        assertEquals(o, "c");
        assertThat(l().size("source").intValue(), is(2));
        assertThat(l().size("dest").intValue(), is(1));
        assertThat(e1.events.size(), is(2));
        assertThat(e2.events.size(), is(2));
        assertThat(e1.filter("source").get(0).type, is(DbManager.EventType.UPDATE));
        assertThat(e1.filter("dest").get(0).type, is(DbManager.EventType.NEW_OR_REPLACE));
    }

    @Test
    public void shouldRpopLpushAllNil() {
        ExpectedEvents e1 = listen("source");
        ExpectedEvents e2 = listen("source");

        Object o = l().rightPopAndLeftPush("source", "dest");

        assertNull(o);
        assertThat(l().size("source").intValue(), is(0));
        assertThat(l().size("dest").intValue(), is(0));
        assertThat(e1.events.size(), is(0));
        assertThat(e2.events.size(), is(0));
    }

    @Test
    public void shouldBLMoveWithValueAdd() {
        ScheduledFuture<?> future = scheduler.schedule(() -> {
            engine.execute(ListRedisMessage.newBuilder()
                    .append(FullBulkValueRedisMessage.ofString("lpush"))
                    .append(FullBulkValueRedisMessage.ofString("n"))
                    .append(FullBulkValueRedisMessage.ofString(serialize("001")))
                    .append(FullBulkValueRedisMessage.ofString(serialize("002"))).build(), embeddedClient());
        }, 500, TimeUnit.MILLISECONDS);

        Tuple<Long, Object> b = block(() -> l().rightPopAndLeftPush("n", "des", 1, TimeUnit.SECONDS));

        assertEquals(b.b, "001");
        assertTrue(b.a > 490);  //cost at least 500mills
        assertTrue(b.a < 700);  //cost no more than 1S
        assertThat(engine.getDbManager().listenerSize(), is(0));
        assertThat(l().rightPop("des"), equalTo("001"));
        assertThat(l().size("des"), is(0L));

        future.cancel(true);
    }



    @Test
    public void shouldLInsert() {
        l().rightPushAll("key", "a", "b", "c", "d", "e");

        ExpectedEvents events = listen("key");
        Long v = t().execute((RedisCallback<Long>) con -> con.lInsert(bytes("key"), RedisListCommands.Position.AFTER,
                serialize("c"), serialize("X")));
        if (!isRedisson()) {
            //call: lpos
            assertEquals(l().indexOf("key", "X").longValue(), 3L);
        }
        assertEquals(v.longValue(), 6L);
        assertEquals(events.triggerTimes, 1);
    }

    @Test
    public void shouldLInsertNoPivot() {
        l().rightPushAll("key", "a", "b", "c", "d", "e");

        ExpectedEvents events = listen("key");
        Long v = t().execute((RedisCallback<Long>) con -> con.lInsert(bytes("key"), RedisListCommands.Position.AFTER,
                serialize("NOT_EXIST"), serialize("X")));
        if (!isRedisson()) {
            //call: lpos
            assertNull(l().indexOf("key", "NOT_EXIST"));
        }
        assertEquals(v.longValue(), -1L);
        assertEquals(events.triggerTimes, 0);
    }

    @Test
    public void shouldLSet() {
        l().rightPushAll("key", "a", "b", "c", "d", "e");

        ExpectedEvents events = listen("key");

        l().set("key", 2, "X");
        List<Object> k = l().range("key", 2, 2);
        assertEquals(k.get(0), "X");
        assertEquals(events.triggerTimes, 1);
    }

    @Test
    public void shouldLSetOutOfRange() {
        l().rightPushAll("key", "a", "b", "c", "d", "e");

        ExpectedEvents events = listen("key");
        try {
            l().set("key", 20, "X");
        } catch (Exception e) {
            assertTrue(e.getCause().getMessage().contains("ERR index out of range"));
        }
        assertEquals(events.triggerTimes, 0);
    }

    @Test
    public void shouldLRem() {
        l().rightPushAll("key", "a", "b", "1", "b", "e");

        ExpectedEvents events = listen("key");
        Long v = l().remove("key", 2, "b");

        List<Object> values = l().range("key", 0, -1);
        assertEquals(v.longValue(), 2L);
        assertEquals(events.triggerTimes, 1);
        assertEquals(values.size(), 3);
        assertEquals(values.get(0), "a");
        assertEquals(values.get(1), "1");
        assertEquals(values.get(2), "e");
    }

    @Test
    public void shouldLRemNotExist() {
        l().rightPushAll("key", "a", "b", "1", "b", "e");

        ExpectedEvents events = listen("key");
        Long v = l().remove("key", 2, "X");

        List<Object> values = l().range("key", 0, -1);
        assertEquals(v.longValue(), 0L);
        assertEquals(events.triggerTimes, 0);
        assertEquals(values.size(), 5);
    }

    @Test
    public void shouldLRem2() {
        l().rightPushAll("key", "a", "b", "1", "b", "e");

        ExpectedEvents events = listen("key");
        Long v = l().remove("key", -1, "b");

        List<Object> values = l().range("key", 0, -1);
        assertEquals(v.longValue(), 1L);
        assertEquals(events.triggerTimes, 1);
        assertEquals(values.size(), 4);
        assertEquals(values.get(0), "a");
        assertEquals(values.get(1), "b");
        assertEquals(values.get(2), "1");
        assertEquals(values.get(3), "e");
    }

    @Test
    public void shouldLRemDel() {
        l().rightPushAll("key", "b", "b", "b", "b");

        ExpectedEvents events = listen("key");
        Long v = l().remove("key", 10, "b");

        assertEquals(v.longValue(), 4L);
        assertNull(engine.getDb(embeddedClient()).get(embeddedClient(), bytes("key")));
        assertEquals(events.triggerTimes, 1);
        assertEquals(events.events.get(0).type, DbManager.EventType.DEL);
    }

    @Test
    public void shouldLTrimNormal() {
        l().rightPushAll("key", "a", "b", "c", "d");

        ExpectedEvents events = listen("key");
        l().trim("key", 1, -2);

        List<Object> obj = l().range("key", 0, -1);
        assertEquals(obj.size(), 2);
        assertEquals(obj.get(0), "b");
        assertEquals(obj.get(1), "c");
        assertEquals(events.triggerTimes, 1);
        assertEquals(events.events.get(0).type, DbManager.EventType.UPDATE);
    }

    @Test
    public void shouldLTrimDel() {
        l().rightPushAll("key", "a", "b", "c", "d");

        ExpectedEvents events = listen("key");
        l().trim("key", 3, -2);

        assertNull(engine.getDb(embeddedClient()).get(embeddedClient(), bytes("key")));
        assertEquals(events.triggerTimes, 1);
        assertEquals(events.events.get(0).type, DbManager.EventType.DEL);
    }

    @Test
    public void shouldLTrimFireEvents() {
        l().rightPushAll("key", "a", "b", "c", "d");
        ExpectedEvents events = listen("key");
        l().trim("key", 0, -1);

        List<Object> obj = l().range("key", 0, -1);
        assertEquals(obj.size(), 4);
        assertEquals(events.triggerTimes, 1);
        assertEquals(events.events.get(0).type, DbManager.EventType.UPDATE);
    }

    @Test
    public void shouldLTrimNil() {

        ExpectedEvents events = listen("key");
        l().trim("key", 0, -1);

        assertNull(engine.getDb(embeddedClient()).get(embeddedClient(), bytes("key")));
        assertEquals(events.triggerTimes, 0);
    }

}
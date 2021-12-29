package cn.deepmax.redis.core.module;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.DbManager;
import cn.deepmax.redis.base.BaseTemplateTest;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.utils.Tuple;
import io.netty.handler.codec.redis.RedisMessage;
import org.junit.Test;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.*;
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
    public void shouldLPushXNil() {
        Long v = l().leftPushIfPresent("not", "1");

        assertEquals(v.longValue(), 0L);
        assertEquals(l().size("not").longValue(), 0L);
    }

    @Test
    public void shouldLPushX2() {
        Long v1 = l().leftPush("n", "1");
        Long v2 = l().leftPushIfPresent("n", "2");

        assertEquals(v1.longValue(), 1L);
        assertEquals(v2.longValue(), 2L);
    }

    @Test
    public void shouldRPushNil() {
        Long v = l().rightPush("not", "1");

        assertEquals(v.longValue(), 1L);
    }

    @Test
    public void shouldRPush2() {
        Long v1 = l().rightPush("n", "1");
        Long v2 = l().rightPushAll("n", "2", "3", "4");

        assertEquals(v1.longValue(), 1L);
        assertEquals(v2.longValue(), 4L);
    }

    @Test
    public void shouldRPushXNil() {
        Long v = l().rightPushIfPresent("not", "1");

        assertEquals(v.longValue(), 0L);
    }

    @Test
    public void shouldRPushX2() {
        Long v1 = l().rightPush("n", "1");
        Long v2 = l().rightPushIfPresent("n", "2");

        assertEquals(v1.longValue(), 1L);
        assertEquals(v2.longValue(), 2L);
        assertEquals(l().size("n").longValue(), 2L);
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
    public void shouldLPop() {
        l().leftPush("key", "1");
        l().leftPush("key", "2");
        l().leftPush("key", "3");

        Object obj = l().leftPop("key");
        assertEquals(obj, "3");

        obj = l().leftPop("key");
        assertEquals(obj, "2");

        obj = l().leftPop("key");
        assertEquals(obj, "1");

        obj = l().leftPop("key");
        assertNull(obj);
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
    public void shouldRPop() {
        l().leftPush("key", "1");
        l().leftPush("key", "2");
        l().leftPush("key", "3");

        Object obj = l().rightPop("key");
        assertEquals(obj, "1");

        obj = l().rightPop("key");
        assertEquals(obj, "2");
        assertEquals(l().size("key").longValue(), 1L);


        obj = l().rightPop("key");
        assertEquals(obj, "3");

        obj = l().rightPop("key");
        assertNull(obj);
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


    private <T> Tuple<Long, T> block(Supplier<T> action) {
        long start = System.nanoTime();
        T v = action.get();
        long cost = System.nanoTime() - start;
        long mills = cost / 1000_000;
        return new Tuple<>(mills, v);
    }
}
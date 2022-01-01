package cn.deepmax.redis.core.template;

import cn.deepmax.redis.base.BasePureTemplateTest;
import org.junit.Test;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ListModuleTemplateTest extends BasePureTemplateTest {
    public ListModuleTemplateTest(RedisTemplate<String, Object> redisTemplate) {
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
    public void shouldLposNil() {
        if (isRedisson()) {
            return;
        }
        Object obj = t().execute((RedisCallback<Object>) con -> con.lPos(bytes("nil"), bytes("em")));

        assertNull(obj);
    }

    @Test
    public void shouldLposNormal() {
        if (isRedisson()) {
            return;
        }
        l().rightPushAll("key", "a", "b", "c", "1", "c", "b");
        Object obj = t().execute((RedisCallback<Object>) con -> con.listCommands().lPos(bytes("key"), serialize("c")));

        assertEquals(obj, 2L);
    }

    @Test
    public void shouldLposAllArg() {
        if (isRedisson()) {
            return;
        }
        l().rightPushAll("key", "a", "b", "c", "1", "c", "b");
        List<Long> obj = (List<Long>) t().execute((RedisCallback<Object>) con -> con.listCommands().lPos(bytes("key"), serialize("c"),
                -1, 2));

        assertEquals(obj.size(), 2);
        assertEquals(obj.get(0).longValue(), 4L);
        assertEquals(obj.get(1).longValue(), 2L);
    }

    @Test
    public void shouldLposAllArgInteger() {
        if (isRedisson()) {
            return;
        }
        l().rightPushAll("key", "a", "b", "c", "1", "c", "b");
        Object obj = t().execute((RedisCallback<Object>) con -> con.listCommands().lPos(bytes("key"), serialize("c")));

        assertEquals(obj, 2L);
    }

    @Test
    public void shouldLposAllArgNil() {
        if (isRedisson()) {
            return;
        }
        l().rightPushAll("key", "a", "b", "c", "1", "c", "b");
        Object obj = t().execute((RedisCallback<Object>) con -> con.listCommands().lPos(bytes("key"), serialize("x")));

        assertNull(obj);
    }

    @Test
    public void shouldLposAllArgEmptyArray() {
        if (isRedisson()) {
            return;
        }
        l().rightPushAll("key", "a", "b", "c", "1", "c", "b");
        List<Long> obj = (List<Long>) t().execute((RedisCallback<Object>) con -> con.listCommands().lPos(bytes("key"), serialize("c"),
                3, 2));

        assertEquals(obj.size(), 0);
    }

    @Test
    public void shouldLRangeNil() {
        List<Object> o = l().range("nil", 0, 0);
        assertEquals(o.size(), 0);
    }

    @Test
    public void shouldLRangeNormal() {
        l().rightPushAll("key", "a", "b", "c", "d", "e");

        List<Object> list = l().range("key", 2, -1);

        assertEquals(list.size(), 3);
        assertEquals(list.get(0), "c");
        assertEquals(list.get(1), "d");
        assertEquals(list.get(2), "e");
    }

    @Test
    public void shouldLRangeNormal2() {
        l().rightPushAll("key", "a", "b", "c", "d", "e");

        List<Object> list = l().range("key", 20, 5);

        assertEquals(list.size(), 0);
    }

    @Test
    public void shouldLIndex() {
        l().rightPushAll("key", "a", "b", "c", "d", "e");

        Object obj;
        obj = l().index("key", 2);
        assertEquals(obj, "c");

        obj = l().index("key", -1);
        assertEquals(obj, "e");

        obj = l().index("key", -10);
        assertNull(obj);

        obj = l().index("nil", 0);
        assertNull(obj);
    }

}


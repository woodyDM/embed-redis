package cn.deepmax.redis.core.template;

import cn.deepmax.redis.base.BasePureTemplateTest;
import cn.deepmax.redis.utils.NumberUtils;
import org.junit.Test;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.*;

import static org.junit.Assert.*;

public class HashModuleTemplateTest extends BasePureTemplateTest {
    public HashModuleTemplateTest(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    @Test
    public void shouldHSetAndGet() {
        h().put("key", "a1", "value1");
        Object v = h().get("key", "a1");
        assertEquals(v, "value1");

        assertNull(h().get("not-exist", "any"));
        assertNull(h().get("key", "hashKey-not-exist"));
    }

    @Test
    public void shouldHSetAllAndGet() {
        Map<Object, Object> m = new HashMap<>();
        m.put("m1", "v1");
        m.put("m2", "v2");
        h().putAll("key", m);
        Object v = h().get("key", "m1");
        assertEquals(v, "v1");

        v = h().get("key", "m2");
        assertEquals(v, "v2");

        assertNull(h().get("not-exist", "any"));
        assertNull(h().get("key", "hashKey-not-exist"));

    }

    @Test
    public void shouldHMGet() {
        Map<Object, Object> m = new HashMap<>();
        m.put("m1", "v1");
        m.put("m2", "v2");
        m.put("m3", "v3");
        h().putAll("key", m);

        List<Object> v = h().multiGet("key", Arrays.asList("m1", "m2", "mnotexist"));
        assertEquals(v.get(0), "v1");
        assertEquals(v.get(1), "v2");
        assertNull(v.get(2));

        v = h().multiGet("key-not-ex", Arrays.asList("m1", "m2", "mnotexist"));
        assertTrue(v.stream().allMatch(Objects::isNull));
        assertEquals(v.size(), 3L);
    }

    @Test
    public void shouldHGetAll() {
        Map<Object, Object> m = new HashMap<>();
        m.put("m1", "v1");
        m.put("m2", "v2");
        m.put("m3", "v3");
        h().putAll("key", m);

        Map<byte[], byte[]> r = t().execute((RedisCallback<Map<byte[], byte[]>>) con -> con.hGetAll(bytes("key")));

        assertEquals(r.size(), 3);
        r.forEach((k, v) -> {
            if (Arrays.equals(k, serialize("m1"))) {
                assertArrayEquals(v, bytes("v1"));
            } else if (Arrays.equals(k, serialize("m2"))) {
                assertArrayEquals(v, bytes("v2"));
            } else if (Arrays.equals(k, serialize("m3"))) {
                assertArrayEquals(v, bytes("v3"));
            }
        });
    }

    @Test
    public void shouldHGetAllEmpty() {
        Map<byte[], byte[]> r = t().execute((RedisCallback<Map<byte[], byte[]>>) con -> con.hGetAll(bytes("key-not-e")));

        assertEquals(r.size(), 0);
    }

    @Test
    public void shouldHKeys() {
        Map<Object, Object> m = new HashMap<>();
        m.put("m1", "v1");
        m.put("m2", "v2");
        m.put("m3", "v3");
        h().putAll("key", m);

        Set<Object> key = h().keys("key");
        assertEquals(key.size(), 3);
        assertTrue(key.contains("m1"));
        assertTrue(key.contains("m2"));
        assertTrue(key.contains("m3"));

    }

    @Test
    public void shouldHKeysNil() {
        Set<Object> key = h().keys("key-not-e");
        assertEquals(key.size(), 0);

    }

    @Test
    public void shouldHVals() {
        Map<Object, Object> m = new HashMap<>();
        m.put("m1", "v1");
        m.put("m2", "v2");
        m.put("m3", "v3");
        h().putAll("key", m);

        List<Object> key = h().values("key");
        assertEquals(key.size(), 3);
        assertEquals(key.get(0), "v1");
        assertEquals(key.get(1), "v2");
        assertEquals(key.get(2), "v3");

    }

    @Test
    public void shouldHValsNil() {
        List<Object> key = h().values("key-not-e");
        assertEquals(key.size(), 0);

    }

    @Test
    public void shouldHDel() {
        Map<Object, Object> m = new HashMap<>();
        m.put("m1", "v1");
        m.put("m2", "v2");
        m.put("m3", "v3");
        h().putAll("key", m);

        Long eff = h().delete("key", "m1", "m2", "m-not-e");
        assertEquals(eff.intValue(), 2);

        Set<Object> key = h().keys("key");
        assertEquals(key.size(), 1);
        assertTrue(key.contains("m3"));

    }

    @Test
    public void shouldHDelAll() {
        Map<Object, Object> m = new HashMap<>();
        m.put("m1", "v1");
        m.put("m2", "v2");
        m.put("m3", "v3");
        h().putAll("key", m);

        Long eff = h().delete("key", "m1", "m2", "m3", "m-not-e");
        assertEquals(eff.intValue(), 3);

        Set<Object> key = h().keys("key");
        assertEquals(key.size(), 0);
        //hash deleted
        assertNull(v().get("key"));
    }

    @Test
    public void shouldHDelEmpty() {
        Long eff = h().delete("key-not0e", "m1", "m2", "m-not-e");
        assertEquals(eff.intValue(), 0);

    }

    @Test
    public void shouldHExists() {
        Map<Object, Object> m = new HashMap<>();
        m.put("m1", "v1");
        m.put("m2", "v2");
        m.put("m3", "v3");
        h().putAll("key", m);

        assertTrue(h().hasKey("key", "m1"));
        assertTrue(h().hasKey("key", "m2"));
        assertTrue(h().hasKey("key", "m3"));
        assertFalse(h().hasKey("key", "m-not-e"));

        assertFalse(h().hasKey("key-not-e", "any"));

    }

    @Test
    public void shouldHLen() {
        Map<Object, Object> m = new HashMap<>();
        m.put("m1", "v1");
        m.put("m2", "v2");
        h().putAll("key", m);

        assertEquals(h().size("key").intValue(), 2);
        assertEquals(h().size("key-not-e").intValue(), 0);
    }

    @Test
    public void shouldHSetNx() {
        Map<Object, Object> m = new HashMap<>();
        m.put("m1", "v1");
        m.put("m2", "v2");
        h().putAll("key", m);

        Boolean put = h().putIfAbsent("key", "m1", "newV");
        assertFalse(put);

        put = h().putIfAbsent("key", "mn", "newV");
        assertTrue(put);
        assertEquals(h().size("key").intValue(), 3L);
    }

    @Test
    public void shouldHSetNxToEmpty() {
        Boolean put = h().putIfAbsent("key", "m1", "newV");
        assertTrue(put);
        assertEquals(h().size("key").intValue(), 1L);
    }

    @Test
    public void shouldHStrLenToEmpty() {
        Long len = h().lengthOfValue("key", "m1");

        assertEquals(len.intValue(), 0);
    }

    @Test
    public void shouldHStrLen() {
        Map<Object, Object> m = new HashMap<>();
        m.put("m1", "v1");
        m.put("m2", "v223");
        h().putAll("key", m);

        assertEquals(h().lengthOfValue("key", "m-not-e").intValue(), 0);
        assertEquals(h().lengthOfValue("key", "m1").intValue(), 2);
        assertEquals(h().lengthOfValue("key", "m2").intValue(), 4);
    }

    @Test
    public void shouldIncrNormal() {
        Map<Object, Object> m = new HashMap<>();
        m.put("m1", "2");
        h().putAll("key", m);

        Long after = h().increment("key", "m1", -43);
        assertEquals(after.intValue(), -41);

        after = h().increment("key", "m2", 450);
        assertEquals(after.intValue(), 450);
    }

    @Test
    public void shouldIncrNil() {
        Long after = h().increment("key", "m1", 47);
        assertEquals(after.intValue(), 47);
        assertEquals(h().size("key").intValue(), 1);
    }

    @Test
    public void shouldIncrByFloatNormal() {
        Map<Object, Object> m = new HashMap<>();
        m.put("m1", "2.5");
        h().putAll("key", m);

        Double after = h().increment("key", "m1", -4.7D);
        assertEquals(NumberUtils.formatDouble(after), "-2.2");

        after = h().increment("key", "m2", 100.2D);
        assertEquals(NumberUtils.formatDouble(after), "100.2");
    }

    @Test
    public void shouldIncrByFloatNil() {
        Double after = h().increment("key", "m1", 48.9435);
        assertEquals(NumberUtils.formatDouble(after), "48.9435");
        assertEquals(h().size("key").intValue(), 1);
    }

    @Test
    public void shouldRandomField() {
        if (isRedisson()) {
            return; //StackOverflowError
        }
        Map<Object, Object> m = new HashMap<>();
        m.put("m11", "v1");
        m.put("m12", "v2");
        m.put("m13", "v3");
        h().putAll("key", m);

        Object obj = h().randomKey("key");
        assertTrue(Arrays.asList("m11", "m12", "m13").contains(obj));
    }

    @Test
    public void shouldRandomField2() {
        if (isRedisson()) {
            return; //StackOverflowError
        }
        Map<Object, Object> m = new HashMap<>();
        m.put("m11", "v1");
        m.put("m12", "v2");
        m.put("m13", "v3");
        h().putAll("key", m);

        Map.Entry<Object, Object> obj = h().randomEntry("key");
        assertTrue(Arrays.asList("m11", "m12", "m13").contains(obj.getKey()));
        assertTrue(Arrays.asList("v1", "v2", "v3").contains(obj.getValue()));
    }

    @Test
    public void shouldRandom2() {
        if (isRedisson()) {
            return; //StackOverflowError
        }
        Map<Object, Object> m = new HashMap<>();
        m.put("m11", "v1");
        m.put("m12", "v2");
        m.put("m13", "v3");
        h().putAll("key", m);

        List<Object> obj = h().randomKeys("key", 1L);
        assertTrue(Arrays.asList("m11", "m12", "m13").contains(obj.get(0)));
    }

    @Test
    public void shouldRandomV() {
        if (isRedisson()) {
            return; //StackOverflowError
        }
        Map<Object, Object> m = new HashMap<>();
        m.put("m11", "v1");
        m.put("m12", "v2");
        m.put("m13", "v3");
        h().putAll("key", m);

        List<Object> obj = h().randomKeys("key", 10L);
        assertEquals(obj.size(), 3);
        assertTrue(Arrays.asList("m11", "m12", "m13").contains(obj.get(0)));
        assertTrue(Arrays.asList("m11", "m12", "m13").contains(obj.get(1)));
        assertTrue(Arrays.asList("m11", "m12", "m13").contains(obj.get(2)));
    }

    @Test
    public void shouldRandomVal() {
        if (isRedisson()) {
            return; //StackOverflowError
        }
        Map<Object, Object> m = new HashMap<>();
        m.put("m11", "v1");
        m.put("m12", "v2");
        m.put("你好😊2", "你好😊");
        h().putAll("key", m);

        List<Map.Entry<byte[], byte[]>> list = t().execute((RedisCallback<List<Map.Entry<byte[], byte[]>>>)
                con -> con.hRandFieldWithValues(bytes("key"), -33));
        if (!isJedis()) {
            assertEquals(list.size(), 33);
        }
        list.forEach(e -> {
            if (Arrays.equals(serialize("m11"), e.getKey())) assertArrayEquals(bytes("v1"), e.getValue());
            if (Arrays.equals(serialize("m12"), e.getKey())) assertArrayEquals(bytes("v2"), e.getValue());
            if (Arrays.equals(serialize("你好😊2"), e.getKey())) assertArrayEquals(bytes("你好😊"), e.getValue());
        });
    }
}

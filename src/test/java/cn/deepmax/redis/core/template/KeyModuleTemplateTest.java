package cn.deepmax.redis.core.template;

import cn.deepmax.redis.base.BasePureTemplateTest;
import cn.deepmax.redis.utils.NumberUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.ValueEncoding;
import org.springframework.data.redis.core.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author wudi
 * @date 2022/1/12
 */
public class KeyModuleTemplateTest extends BasePureTemplateTest {
    public KeyModuleTemplateTest(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldNotCopyWhenSourceNotExist() {
        if (isRedisson()) {
            return;
        }
        Boolean ok = t().copy("nit", "when", true);

        assertFalse(ok);
    }

    @Test
    public void shouldNotCopyWhenNotReplace() {
        if (isRedisson()) {
            return;
        }
        v().set("s", "v");
        v().set("dd", "v");
        Boolean ok = t().copy("s", "dd", false);

        assertFalse(ok);
    }

    @Test
    public void shouldCopyWhenReplace() {
        if (isRedisson()) {
            return;
        }
        v().set("s", "v");
        v().set("dd", "v2");
        Boolean ok = t().copy("s", "dd", true);

        assertTrue(ok);
        assertEquals(v().get("dd"), "v");
    }

    @Test
    public void shouldCopyList() {
        if (isRedisson()) {
            return;
        }
        l().rightPushAll("s", "a", "b", "c", "d");

        Boolean ok = t().copy("s", "dd", false);

        assertTrue(ok);
        List<Object> list = l().leftPop("dd", 5);
        assertEquals(list.get(0), "a");
        assertEquals(list.get(1), "b");
        assertEquals(list.get(2), "c");
        assertEquals(list.get(3), "d");
    }

    @Test
    public void shouldCopySet() {
        if (isRedisson()) {
            return;
        }
        s().add("s", "1", "a", "2", "b");

        Boolean ok = t().copy("s", "dd", false);

        assertTrue(ok);
        Set<Object> list = s().members("dd");
        assertEquals(list.size(), 4);
        assertTrue(list.contains("1"));
        assertTrue(list.contains("2"));
        assertTrue(list.contains("a"));
        assertTrue(list.contains("b"));
    }

    @Test
    public void shouldCopyHash() {
        if (isRedisson()) {
            return;
        }
        h().put("s", "k1", "v1");
        h().put("s", "k2", "v2");

        Boolean ok = t().copy("s", "dd", false);

        assertTrue(ok);
        List<Object> obj = h().multiGet("dd", Arrays.asList("k1", "k2"));
        assertEquals(obj.size(), 2);
        assertEquals(obj.get(0), "v1");
        assertEquals(obj.get(1), "v2");
    }

    @Test
    public void shouldCopySortedSet() {
        if (isRedisson()) {
            return;
        }

        z().add("s", "v1", 1.2D);
        z().add("s", "v2", 1.0D);
        z().add("s", "v3", 2D);

        Boolean ok = t().copy("s", "dd", false);

        assertTrue(ok);
        ArrayList<ZSetOperations.TypedTuple<Object>> list = new ArrayList<>(z().rangeWithScores("dd", 0, -1));
        assertEquals(list.size(), 3);
        assertEquals(list.get(0).getValue(), "v2");
        assertEquals(list.get(1).getValue(), "v1");
        assertEquals(list.get(2).getValue(), "v3");
        assertEquals(NumberUtils.formatDouble(list.get(0).getScore()), "1");
        assertEquals(NumberUtils.formatDouble(list.get(1).getScore()), "1.2");
        assertEquals(NumberUtils.formatDouble(list.get(2).getScore()), "2");
    }

    @Test
    public void shouldKeys() {
        v().set("hello", "1");
        h().put("hallo", "k", "v");
        z().add("hkllo", "a", 1D);
        l().rightPushAll("hllo", "1", "2");
        l().rightPushAll("hllo234243", "1", "2");

        Set<String> keys = t().keys("h*llo");
        assertEquals(keys.size(), 4);
        assertTrue(keys.contains("hello"));
        assertTrue(keys.contains("hallo"));
        assertTrue(keys.contains("hkllo"));
        assertTrue(keys.contains("hllo"));
    }

    @Test
    public void shouldRdKey0() {
        String s = t().randomKey();
        assertNull(s);
    }

    @Test
    public void shouldRdKey1() {
        v().set("k", "v");
        String s1 = t().randomKey();

        assertEquals(s1, "k");
    }

    @Test
    public void shouldRdKeyN() {
        v().set("hello", "1");
        h().put("hallo", "k", "v");
        z().add("hkllo", "a", 1D);

        assertTrue(Arrays.asList("hello", "hallo", "hkllo").contains(t().randomKey()));
    }

    @Test
    public void shouldNotMove() {
        assertFalse(t().move("not-e", 1));
    }
    
    @Test
    public void shouldMove2() {
        v().set("key", "any");

        assertTrue(t().move("key", 1));
        assertNull(v().get("key"));
        //only jedis support select on connection
        if (isJedis()) {
            try (RedisConnection con = t().getConnectionFactory().getConnection()) {
                con.select(1);
                byte[] v = con.get(bytes("key"));
                assertArrayEquals(v, serialize("any"));
                con.select(0);
            }
        }
    }

    @Test
    public void shouldObjectEncoding() {
        if (!isEmbededRedis()) {
            return;
        }
        v().set("hello", "1");
        h().put("hallo", "k", "v");
        z().add("hkllo", "a", 1D);
        l().rightPushAll("hllo", "1", "2");
        s().add("s", "se");

        ValueEncoding e = t().execute((RedisCallback<ValueEncoding>) con -> con.encodingOf(bytes("not-e")));
        assertEquals(e, ValueEncoding.RedisValueEncoding.VACANT);

        e = t().execute((RedisCallback<ValueEncoding>) con -> con.encodingOf(bytes("hello")));
        assertEquals(e, ValueEncoding.RedisValueEncoding.RAW);

        e = t().execute((RedisCallback<ValueEncoding>) con -> con.encodingOf(bytes("hallo")));
        assertEquals(e, ValueEncoding.RedisValueEncoding.HASHTABLE);

        e = t().execute((RedisCallback<ValueEncoding>) con -> con.encodingOf(bytes("hkllo")));
        assertEquals(e, ValueEncoding.RedisValueEncoding.SKIPLIST);

        e = t().execute((RedisCallback<ValueEncoding>) con -> con.encodingOf(bytes("hllo")));
        assertEquals(e, ValueEncoding.RedisValueEncoding.LINKEDLIST);

        e = t().execute((RedisCallback<ValueEncoding>) con -> con.encodingOf(bytes("s")));
        assertEquals(e, ValueEncoding.RedisValueEncoding.HASHTABLE);
    }

    @Test
    public void shouldType() {
        v().set("hello", "1");
        h().put("hallo", "k", "v");
        z().add("hkllo", "a", 1D);
        l().rightPushAll("hllo", "1", "2");
        s().add("s", "se");

        DataType e = t().type("not-e");
        assertEquals(e, DataType.NONE);

        e = t().type("hello");
        assertEquals(e, DataType.STRING);

        e = t().type("hallo");
        assertEquals(e, DataType.HASH);

        e = t().type("hkllo");
        assertEquals(e, DataType.ZSET);

        e = t().type("hllo");
        assertEquals(e, DataType.LIST);

        e = t().type("s");
        assertEquals(e, DataType.SET);

    }

    @Test
    public void shouldTouch() {
        v().set("hello", "1");
        h().put("hallo", "k", "v");

        Long c = t().execute((RedisCallback<Long>) con -> con.touch(bytes("hkllo"),
                bytes("hello"), bytes("hallo"), bytes("a"), bytes("b")));

        assertEquals(c.intValue(), 2);
    }

    @Test
    public void shouldRename() {
        v().set("s", "v");
        t().expire("s", 3600, TimeUnit.SECONDS);

        t().rename("s", "n");

        assertNull(v().get("s"));
        assertEquals(v().get("n"), "v");
        assertTrue(t().getExpire("n") > 3500);
    }

    @Test
    public void shouldRename3() {
        v().set("s", "v");
        v().set("n", "v2222");
        t().expire("s", 3600, TimeUnit.SECONDS);

        t().rename("s", "n");

        assertNull(v().get("s"));
        assertEquals(v().get("n"), "v");
        assertTrue(t().getExpire("n") > 3500);
    }

    @Test
    public void shouldRenameNx() {
        v().set("s", "v");
        v().set("n", "v2222");
        t().expire("s", 3600, TimeUnit.SECONDS);

        t().renameIfAbsent("s", "n");

        assertEquals(v().get("s"), "v");
        assertEquals(v().get("n"), "v2222");
        assertTrue(t().getExpire("s") > 3500);
        assertTrue(t().getExpire("n") == -1);
    }


    @Test
    public void shouldRename2() {
        v().set("s", "v");
        t().expire("s", 3600, TimeUnit.SECONDS);

        t().rename("s", "s");

        assertEquals(v().get("s"), "v");
        assertTrue(t().getExpire("s") > 3500);
    }

    @Test
    public void shouldRenameNx2() {
        v().set("s", "v");
        t().expire("s", 3600, TimeUnit.SECONDS);

        t().renameIfAbsent("s", "s");

        assertEquals(v().get("s"), "v");
        assertTrue(t().getExpire("s") > 3500);
    }

    @Test
    public void shouldRenameErr() {
        if (isLettuce()) {
            expectedException.expect(RedisSystemException.class);
        } else {
            expectedException.expect(InvalidDataAccessApiUsageException.class);
        }
        expectedException.expectMessage("no such key");

        t().rename("s-not-e", "s");

    }

    @Test
    public void shouldRenameNxErr() {
        if (isLettuce()) {
            expectedException.expect(RedisSystemException.class);
        } else {
            expectedException.expect(InvalidDataAccessApiUsageException.class);
        }
        expectedException.expectMessage("no such key");

        t().renameIfAbsent("s-not-e", "s");

    }

    @Test
    public void shouldScan() {
        v().set("a", "1");
        v().set("ab", "1");
        z().add("c", "v", 1.4D);
        z().add("c1", "v", 1.4D);
        z().add("c2", "v", 1.4D);
        l().rightPushAll("d1", "1", "2");
        l().rightPushAll("d2", "1", "2");
        s().add("e1", "v1");
        s().add("e2", "v1");
        h().put("f2", "h", "va");
        h().put("f3", "h", "va");

        Cursor<byte[]> c = t().execute((RedisCallback<Cursor<byte[]>>) con -> con.scan(ScanOptions.scanOptions()
                .type(DataType.STRING).count(1).match("a*")
                .build()));
        List<byte[]> list = new ArrayList<>();
        while (c.hasNext()) list.add(c.next());
        assertEquals(list.size(), 2);
        assertArrayEquals(list.get(0), bytes("a"));
        assertArrayEquals(list.get(1), bytes("ab"));


        c = t().execute((RedisCallback<Cursor<byte[]>>) con -> con.scan(ScanOptions.scanOptions()
                .type(DataType.ZSET).count(1).match("c*")
                .build()));
        list = new ArrayList<>();
        while (c.hasNext()) list.add(c.next());
        assertEquals(list.size(), 3);
        assertArrayEquals(list.get(0), bytes("c"));
        assertArrayEquals(list.get(1), bytes("c1"));
        assertArrayEquals(list.get(2), bytes("c2"));

        c = t().execute((RedisCallback<Cursor<byte[]>>) con -> con.scan(ScanOptions.scanOptions()
                .type(DataType.LIST).count(1).match("d*")
                .build()));
        list = new ArrayList<>();
        while (c.hasNext()) list.add(c.next());
        assertEquals(list.size(), 2);
        assertArrayEquals(list.get(0), bytes("d1"));
        assertArrayEquals(list.get(1), bytes("d2"));

        c = t().execute((RedisCallback<Cursor<byte[]>>) con -> con.scan(ScanOptions.scanOptions()
                .type(DataType.SET).count(1).match("e*")
                .build()));
        list = new ArrayList<>();
        while (c.hasNext()) list.add(c.next());
        assertEquals(list.size(), 2);
        assertArrayEquals(list.get(0), bytes("e1"));
        assertArrayEquals(list.get(1), bytes("e2"));

        c = t().execute((RedisCallback<Cursor<byte[]>>) con -> con.scan(
                KeyScanOptions.scanOptions(DataType.HASH).count(1).match("f*").build()));
        list = new ArrayList<>();
        while (c.hasNext()) list.add(c.next());
        assertEquals(list.size(), 2);
        assertArrayEquals(list.get(0), bytes("f2"));
        assertArrayEquals(list.get(1), bytes("f3"));

    }
}

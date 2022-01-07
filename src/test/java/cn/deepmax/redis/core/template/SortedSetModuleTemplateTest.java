package cn.deepmax.redis.core.template;

import cn.deepmax.redis.base.BasePureTemplateTest;
import cn.deepmax.redis.base.BlockTest;
import cn.deepmax.redis.utils.NumberUtils;
import cn.deepmax.redis.utils.Tuple;
import org.junit.Test;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author wudi
 * @date 2021/12/30
 */
public class SortedSetModuleTemplateTest extends BasePureTemplateTest implements BlockTest {
    public SortedSetModuleTemplateTest(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    @Test
    public void shouldAddAndRangeNormal() {
        z().add("key", "a", 1.0D);
        z().add("key", "b", 1.1D);
        z().add("key", "c", 1.2D);
        z().add("key", "d", 1.3D);
        z().add("key", "e", 1.15D);
        z().add("key", "f", 1.25D);

        z().add("key", "b", 2.0D);
        // a e c f d b
        Set<ZSetOperations.TypedTuple<Object>> values = z().rangeWithScores("key", 1, -3);
        List<ZSetOperations.TypedTuple<Object>> list = new ArrayList<>(values);
        assertEquals(list.size(), 3);
        assertEquals(list.get(0).getValue(), "e");
        assertEquals(list.get(1).getValue(), "c");
        assertEquals(list.get(2).getValue(), "f");

        assertEquals(NumberUtils.formatDouble(list.get(0).getScore()), "1.15");
        assertEquals(NumberUtils.formatDouble(list.get(1).getScore()), "1.2");
        assertEquals(NumberUtils.formatDouble(list.get(2).getScore()), "1.25");
    }

    @Test
    public void shouldZCard() {
        z().add("key", "a", 1.0D);
        z().add("key", "b", 1.1D);

        assertEquals(z().size("key").longValue(), 2);
        assertEquals(z().size("key-not-exist").longValue(), 0);
    }

    @Test
    public void shouldAddAndRangeNormalNoScore() {
        z().add("key", "a", 1.0D);
        z().add("key", "b", 1.1D);
        z().add("key", "c", 1.2D);
        z().add("key", "d", 1.3D);
        z().add("key", "e", 1.15D);
        z().add("key", "f", 1.25D);

        z().add("key", "b", 2.0D);
        // a e c f d b
        Set<Object> values = z().range("key", 1, -3);
        List<Object> list = new ArrayList<>(values);
        assertEquals(list.size(), 3);
        assertEquals(list.get(0), "e");
        assertEquals(list.get(1), "c");
        assertEquals(list.get(2), "f");

    }

    @Test
    public void shouldZCount() {
        z().add("key", "a", 1.0D);
        z().add("key", "b", 1.1D);
        z().add("key", "c", 1.2D);
        z().add("key", "d", 1.3D);
        z().add("key", "e", 1.15D);
        z().add("key", "f", 1.25D);

        z().add("key", "b", 2.0D);
        //a    e    c   f       d       b
        //1.0  1.15 1.2 1.25    1.3     2
        assertEquals(z().count("key", 1.15D, 1.3D).intValue(), 4);
        assertEquals(z().count("key", 2D, 3D).intValue(), 1);
        assertEquals(z().count("key", 3D, 3D).intValue(), 0);
        assertEquals(z().count("key", -33D, -3D).intValue(), 0);
        assertEquals(z().count("key", -33D, 1D).intValue(), 1);
    }

    @Test
    public void shouldZCount2() {
        z().add("key", "a", 1.0D);
        z().add("key", "b", 1.1D);
        z().add("key", "c", 1.2D);
        z().add("key", "d", 1.3D);
        z().add("key", "e", 1.15D);
        z().add("key", "f", 1.25D);

        z().add("key", "b", 2.0D);
        //a    e    c   f       d       b
        //1.0  1.15 1.2 1.25    1.3     2
        Long key = t().execute((RedisCallback<Long>) con -> con.zCount(bytes("key"), RedisZSetCommands.Range.range().gt(1.2D)));
        assertEquals(key.intValue(), 3);
        key = t().execute((RedisCallback<Long>) con -> con.zCount(bytes("key"), RedisZSetCommands.Range.range().lt(1.25D)));
        assertEquals(key.intValue(), 3);
        key = t().execute((RedisCallback<Long>) con -> con.zCount(bytes("key"), RedisZSetCommands.Range.range()));
        assertEquals(key.intValue(), 6);
    }

    @Test
    public void shouldZPopMax() {
        if (isRedisson()) {
            return; //StackOverflowError not imple
        }
        z().add("key", "a", 1.0D);
        z().add("key", "b", 1.1D);
        z().add("key", "c", 1.2D);
        z().add("key", "d", 1.3D);
        z().add("key", "e", 1.15D);
        z().add("key", "f", 1.25D);

        z().add("key", "b", 2.0D);
        //a    e    c   f       d       b
        //1.0  1.15 1.2 1.25    1.3     2
        ZSetOperations.TypedTuple<Object> t = z().popMax("key");
        assertEquals(t.getValue(), "b");
        assertEquals(NumberUtils.formatDouble(t.getScore()), "2");

        List<ZSetOperations.TypedTuple<Object>> list = new ArrayList<>(z().popMax("key", 3));
        assertEquals(list.size(), 3);
        assertEquals(list.get(0).getValue(), "d");
        assertEquals(list.get(1).getValue(), "f");
        assertEquals(list.get(2).getValue(), "c");
        assertEquals(NumberUtils.formatDouble(list.get(0).getScore()), "1.3");
        assertEquals(NumberUtils.formatDouble(list.get(1).getScore()), "1.25");
        assertEquals(NumberUtils.formatDouble(list.get(2).getScore()), "1.2");

        list = new ArrayList<>(z().popMax("key", 4));
        assertEquals(list.size(), 2);
        assertEquals(list.get(0).getValue(), "e");
        assertEquals(list.get(1).getValue(), "a");
        assertEquals(NumberUtils.formatDouble(list.get(0).getScore()), "1.15");
        assertEquals(NumberUtils.formatDouble(list.get(1).getScore()), "1");

        list = new ArrayList<>(z().popMax("key", 3));
        assertEquals(list.size(), 0);
    }

    @Test
    public void shouldZPopMin() {
        if (isRedisson()) {
            return; //StackOverflowError not imple
        }
        z().add("key", "a", 1.0D);
        z().add("key", "b", 1.1D);
        z().add("key", "c", 1.2D);
        z().add("key", "d", 1.3D);
        z().add("key", "e", 1.15D);
        z().add("key", "f", 1.25D);

        z().add("key", "b", 2.0D);
        //a    e    c   f       d       b
        //1.0  1.15 1.2 1.25    1.3     2
        ZSetOperations.TypedTuple<Object> t = z().popMin("key");
        assertEquals(t.getValue(), "a");
        assertEquals(NumberUtils.formatDouble(t.getScore()), "1");

        List<ZSetOperations.TypedTuple<Object>> list = new ArrayList<>(z().popMin("key", 4));
        assertEquals(list.size(), 4);
        assertEquals(list.get(0).getValue(), "e");
        assertEquals(list.get(1).getValue(), "c");
        assertEquals(list.get(2).getValue(), "f");
        assertEquals(list.get(3).getValue(), "d");
        assertEquals(NumberUtils.formatDouble(list.get(0).getScore()), "1.15");
        assertEquals(NumberUtils.formatDouble(list.get(1).getScore()), "1.2");
        assertEquals(NumberUtils.formatDouble(list.get(2).getScore()), "1.25");
        assertEquals(NumberUtils.formatDouble(list.get(3).getScore()), "1.3");

        list = new ArrayList<>(z().popMin("key", 4));
        assertEquals(list.size(), 1);
        assertEquals(list.get(0).getValue(), "b");
        assertEquals(NumberUtils.formatDouble(list.get(0).getScore()), "2");

        list = new ArrayList<>(z().popMin("key", 3));
        assertEquals(list.size(), 0);
    }

    @Test
    public void shouldBZPopMinNormal() {
        if (isRedisson()) {
            return; //StackOverflowError not imple
        }
        z().add("key", "a", 1.0D);

        Tuple<Long, ZSetOperations.TypedTuple<Object>> v1 = block(() -> z().popMin("key", 1, TimeUnit.SECONDS));
        assertEquals(v1.b.getValue(), "a");
        assertEquals(NumberUtils.formatDouble(v1.b.getScore()), "1");
        assertTrue(v1.a < 1000);

        v1 = block(() -> z().popMin("key", 100, TimeUnit.MILLISECONDS));
        assertNull(v1.b);
        assertTrue(v1.a > 100);
    }

    @Test
    public void shouldAddAndRangeRevNormal() {
        z().add("key", "a", 1.0D);
        z().add("key", "b", 1.1D);
        z().add("key", "c", 1.2D);
        z().add("key", "d", 1.3D);
        z().add("key", "e", 1.15D);
        z().add("key", "f", 1.25D);

        z().add("key", "b", 2.0D);
        // a e c f d b
        Set<ZSetOperations.TypedTuple<Object>> values = z().reverseRangeWithScores("key", 1, -3);
        List<ZSetOperations.TypedTuple<Object>> list = new ArrayList<>(values);
        assertEquals(list.size(), 3);
        assertEquals(list.get(0).getValue(), "d");
        assertEquals(list.get(1).getValue(), "f");
        assertEquals(list.get(2).getValue(), "c");

        assertEquals(NumberUtils.formatDouble(list.get(0).getScore()), "1.3");
        assertEquals(NumberUtils.formatDouble(list.get(1).getScore()), "1.25");
        assertEquals(NumberUtils.formatDouble(list.get(2).getScore()), "1.2");
    }

    @Test
    public void shouldAddAndRangeRevNormalNoScore() {
        z().add("key", "a", 1.0D);
        z().add("key", "b", 1.1D);
        z().add("key", "c", 1.2D);
        z().add("key", "d", 1.3D);
        z().add("key", "e", 1.15D);
        z().add("key", "f", 1.25D);

        z().add("key", "b", 2.0D);
        // a e c f d b
        Set<Object> values = z().reverseRange("key", 1, -3);
        List<Object> list = new ArrayList<>(values);
        assertEquals(list.size(), 3);
        assertEquals(list.get(0), "d");
        assertEquals(list.get(1), "f");
        assertEquals(list.get(2), "c");
    }

    @Test
    public void shouldAddAndRangeByScoreNormal() {
        z().add("key", "a", 1.0D);
        z().add("key", "b", 1.1D);
        z().add("key", "c", 1.2D);
        z().add("key", "d", 1.3D);
        z().add("key", "e", 1.15D);
        z().add("key", "f", 1.25D);

        z().add("key", "b", 2.0D);
        //a    e    c   f       d       b
        //1.0  1.15 1.2 1.25    1.3     2
        Set<ZSetOperations.TypedTuple<Object>> values = z().rangeByScoreWithScores("key", 1.14, 1.26);
        List<ZSetOperations.TypedTuple<Object>> list = new ArrayList<>(values);
        assertEquals(list.size(), 3);
        assertEquals(list.get(0).getValue(), "e");
        assertEquals(list.get(1).getValue(), "c");
        assertEquals(list.get(2).getValue(), "f");

        assertEquals(NumberUtils.formatDouble(list.get(0).getScore()), "1.15");
        assertEquals(NumberUtils.formatDouble(list.get(1).getScore()), "1.2");
        assertEquals(NumberUtils.formatDouble(list.get(2).getScore()), "1.25");
    }

    @Test
    public void shouldZDiffNormal() {
        if (isRedisson()) {
            return; //StackOverflowError
        }
        z().add("key", "a", 1.0D);
        z().add("key", "b", 1.1D);
        z().add("key", "c", 1.2D);
        z().add("key", "d", 1.3D);
        z().add("key", "e", 1.15D);
        z().add("key", "f", 1.25D);

        z().add("key2", "b", 1.1D);
        z().add("key3", "e", 1.1D);
        z().add("key4", "f", 1.1D);

        Set<Object> diff = z().difference("key", Arrays.asList("key2", "key3", "key4"));
        assertEquals(diff.size(), 3);
        assertTrue(diff.contains("a"));
        assertTrue(diff.contains("c"));
        assertTrue(diff.contains("d"));
    }

    @Test
    public void shouldZDiffStoreNormal() {
        if (isRedisson()) {
            return; //StackOverflowError
        }
        z().add("key", "a", 1.0D);
        z().add("key", "b", 1.1D);
        z().add("key", "c", 1.2D);
        z().add("key", "d", 1.3D);
        z().add("key", "e", 1.15D);
        z().add("key", "f", 1.25D);

        z().add("key2", "b", 1.1D);
        z().add("key3", "e", 1.1D);
        z().add("key4", "f", 1.1D);

        Long diff = z().differenceAndStore("key", Arrays.asList("key2", "key3", "key4"), "dest");
        LinkedList<ZSetOperations.TypedTuple<Object>> dest = new LinkedList<>(z().rangeWithScores("dest", 0, -1));

        assertEquals(diff.intValue(), 3);
        assertEquals(dest.get(0).getValue(), "a");
        assertEquals(dest.get(1).getValue(), "c");
        assertEquals(dest.get(2).getValue(), "d");
        assertEquals(NumberUtils.formatDouble(dest.get(0).getScore()), "1");
        assertEquals(NumberUtils.formatDouble(dest.get(1).getScore()), "1.2");
        assertEquals(NumberUtils.formatDouble(dest.get(2).getScore()), "1.3");

    }

    @Test
    public void shouldZDiffStoreNormalEmpty() {
        if (isRedisson()) {
            return; //StackOverflowError
        }
        z().add("key", "a", 1.0D);
        z().add("key", "c", 1.2D);
        z().add("key", "d", 1.3D);

        Long diff = z().differenceAndStore("key", Collections.emptyList(), "dest");
        LinkedList<ZSetOperations.TypedTuple<Object>> dest = new LinkedList<>(z().rangeWithScores("dest", 0, -1));

        assertEquals(diff.intValue(), 3);
        assertEquals(dest.get(0).getValue(), "a");
        assertEquals(dest.get(1).getValue(), "c");
        assertEquals(dest.get(2).getValue(), "d");
        assertEquals(NumberUtils.formatDouble(dest.get(0).getScore()), "1");
        assertEquals(NumberUtils.formatDouble(dest.get(1).getScore()), "1.2");
        assertEquals(NumberUtils.formatDouble(dest.get(2).getScore()), "1.3");

    }

    @Test
    public void shouldZDiffNormalToEmpty() {
        if (isRedisson()) {
            return; //StackOverflowError
        }
        z().add("key", "a", 1.0D);
        z().add("key", "b", 1.1D);
        z().add("key", "c", 1.2D);
        z().add("key", "d", 1.3D);
        z().add("key", "e", 1.15D);
        z().add("key", "f", 1.25D);

        z().add("key2", "b", 1.1D);
        z().add("key3", "e", 1.1D);
        z().add("key4", "f", 1.1D);
        z().add("key5", "a", 1.1D);
        z().add("key5", "c", 1.1D);
        z().add("key5", "d", 1.1D);

        Set<Object> diff = z().difference("key", Arrays.asList("key2", "key3", "key4", "key5", "key-not-exist"));
        assertEquals(diff.size(), 0);
    }

    @Test
    public void shouldZDiffNormalWithScore() {
        if (isRedisson()) {
            return; //StackOverflowError
        }
        z().add("key", "a", 1.0D);
        z().add("key", "b", 1.1D);
        z().add("key", "c", 1.2D);
        z().add("key", "d", 1.3D);
        z().add("key", "e", 1.15D);
        z().add("key", "f", 1.25D);

        z().add("key2", "b", 1.1D);
        z().add("key3", "e", 1.1D);
        z().add("key4", "f", 1.1D);

        Set<ZSetOperations.TypedTuple<Object>> diff = z().differenceWithScores("key", Arrays.asList("key2", "key3", "key4"));
        assertEquals(diff.size(), 3);
        assertEquals(NumberUtils.formatDouble(diff.stream().filter(s -> s.getValue().equals("a")).findFirst().get().getScore()), "1");
        assertEquals(NumberUtils.formatDouble(diff.stream().filter(s -> s.getValue().equals("c")).findFirst().get().getScore()), "1.2");
        assertEquals(NumberUtils.formatDouble(diff.stream().filter(s -> s.getValue().equals("d")).findFirst().get().getScore()), "1.3");
    }

    @Test
    public void shouldAddAndRangeByScoreNormalOffSet() {
        z().add("key", "a", 1.0D);
        z().add("key", "b", 1.1D);
        z().add("key", "c", 1.2D);
        z().add("key", "d", 1.3D);
        z().add("key", "e", 1.15D);
        z().add("key", "f", 1.25D);

        z().add("key", "b", 2.0D);
        //a    e    c   f       d       b
        //1.0  1.15 1.2 1.25    1.3     2
        Set<ZSetOperations.TypedTuple<Object>> values = z().rangeByScoreWithScores("key", 1.14, 3, 1, 3);
        List<ZSetOperations.TypedTuple<Object>> list = new ArrayList<>(values);
        assertEquals(list.size(), 3);
        assertEquals(list.get(0).getValue(), "c");
        assertEquals(list.get(1).getValue(), "f");
        assertEquals(list.get(2).getValue(), "d");

        assertEquals(NumberUtils.formatDouble(list.get(0).getScore()), "1.2");
        assertEquals(NumberUtils.formatDouble(list.get(1).getScore()), "1.25");
        assertEquals(NumberUtils.formatDouble(list.get(2).getScore()), "1.3");
    }

    @Test
    public void shouldAddAndRangeByScoreNormalOffSet2() {
        z().add("key", "a", 1.0D);
        z().add("key", "b", 1.1D);
        z().add("key", "c", 1.2D);
        z().add("key", "d", 1.3D);
        z().add("key", "e", 1.15D);
        z().add("key", "f", 1.25D);

        z().add("key", "b", 2.0D);
        //a    e    c   f       d       b
        //1.0  1.15 1.2 1.25    1.3     2
        Set<ZSetOperations.TypedTuple<Object>> values = z().rangeByScoreWithScores("key", 1.15, 2, 1, -1);
        List<ZSetOperations.TypedTuple<Object>> list = new ArrayList<>(values);
        assertEquals(list.size(), 4);
        assertEquals(list.get(0).getValue(), "c");
        assertEquals(list.get(1).getValue(), "f");
        assertEquals(list.get(2).getValue(), "d");
        assertEquals(list.get(3).getValue(), "b");

        assertEquals(NumberUtils.formatDouble(list.get(0).getScore()), "1.2");
        assertEquals(NumberUtils.formatDouble(list.get(1).getScore()), "1.25");
        assertEquals(NumberUtils.formatDouble(list.get(2).getScore()), "1.3");
        assertEquals(NumberUtils.formatDouble(list.get(3).getScore()), "2");
    }

    @Test
    public void shouldAddAndRangeByScoreNormalOffSet3() {
        z().add("key", "a", 1.0D);
        z().add("key", "b", 1.1D);
        z().add("key", "c", 1.2D);
        z().add("key", "d", 1.3D);
        z().add("key", "e", 1.15D);
        z().add("key", "f", 1.25D);

        z().add("key", "b", 2.0D);
        //a    e    c   f       d       b
        //1.0  1.15 1.2 1.25    1.3     2
        RedisZSetCommands.Range range = RedisZSetCommands.Range.range().gt(1.15D).lte(2D);  //Use Double value
        RedisZSetCommands.Limit limit = RedisZSetCommands.Limit.limit().offset(1).count(3);
        Set<byte[]> keys = t().execute((RedisCallback<Set<byte[]>>) con -> con.zRevRangeByScore(bytes("key"), range, limit));

        List<byte[]> list = new ArrayList<>(keys);
        assertEquals(list.size(), 3);

        assertArrayEquals(list.get(0), serialize("d"));
        assertArrayEquals(list.get(1), serialize("f"));
        assertArrayEquals(list.get(2), serialize("c"));

    }

    @Test
    public void shouldAddAndRevRangeByScoreNormalOffSet3() {
        z().add("key", "a", 1.0D);
        z().add("key", "b", 1.1D);
        z().add("key", "c", 1.2D);
        z().add("key", "d", 1.3D);
        z().add("key", "e", 1.15D);
        z().add("key", "f", 1.25D);

        z().add("key", "b", 2.0D);
        //a    e    c   f       d       b
        //1.0  1.15 1.2 1.25    1.3     2
        Set<byte[]> keys = t().execute((RedisCallback<Set<byte[]>>) con -> con.zRangeByScore(bytes("key"), "(1.15", "(2", 1, 3));

        List<byte[]> list = new ArrayList<>(keys);
        assertEquals(list.size(), 2);
        //Jedis treat value as string. but we use JDKSerializer
        if (isJedis()) {
            assertArrayEquals(list.get(0), new String(serialize("f"), StandardCharsets.UTF_8).getBytes());
            assertArrayEquals(list.get(1), new String(serialize("d"), StandardCharsets.UTF_8).getBytes());
        } else {
            assertArrayEquals(list.get(0), serialize("f"));
            assertArrayEquals(list.get(1), serialize("d"));
        }
    }

    @Test
    public void shouldAddAndRangeByScoreInf() {
        z().add("key", "a", 1.0D);
        z().add("key", "b", 1.1D);
        z().add("key", "c", 1.2D);
        z().add("key", "d", 1.3D);
        z().add("key", "e", 1.15D);
        z().add("key", "f", 1.25D);

        z().add("key", "b", 2.0D);
        //a    e    c   f       d       b
        //1.0  1.15 1.2 1.25    1.3     2
        Set<byte[]> keys = t().execute((RedisCallback<Set<byte[]>>) con -> con.zRangeByScore(bytes("key"), "-inf", "inf", 1, -1));

        List<byte[]> list = new ArrayList<>(keys);
        assertEquals(list.size(), 5);
        //Jedis treat value as string. but we use JDKSerializer
        if (isJedis()) {
            assertArrayEquals(list.get(0), new String(serialize("e"), StandardCharsets.UTF_8).getBytes());
            assertArrayEquals(list.get(1), new String(serialize("c"), StandardCharsets.UTF_8).getBytes());
            assertArrayEquals(list.get(2), new String(serialize("f"), StandardCharsets.UTF_8).getBytes());
            assertArrayEquals(list.get(3), new String(serialize("d"), StandardCharsets.UTF_8).getBytes());
            assertArrayEquals(list.get(4), new String(serialize("b"), StandardCharsets.UTF_8).getBytes());
        } else {
            assertArrayEquals(list.get(0), serialize("e"));
            assertArrayEquals(list.get(1), serialize("c"));
            assertArrayEquals(list.get(2), serialize("f"));
            assertArrayEquals(list.get(3), serialize("d"));
            assertArrayEquals(list.get(4), serialize("b"));
        }
    }

    @Test
    public void shouldAddAndRangeByLexNormal() {
        if (isRedisson()) {
            return; // redisson zRangeByLex bug.
        }
        z().add("key", "b", 1.0D);
        z().add("key", "a", 1.0D);
        z().add("key", "c", 1.0D);
        z().add("key", "e", 1.0D);
        z().add("key", "d", 1.0D);
        z().add("key", "f", 1.0D);

        //a b c d e f
        //0 1 2 3 4 5

        RedisZSetCommands.Range range = RedisZSetCommands.Range.range().gt(serialize("a")).lte(serialize("e"));
        Set<Object> keys = z().rangeByLex("key", range);

        List<Object> list = new ArrayList<>(keys);
        assertEquals(list.size(), 4);
        assertEquals((String) list.get(0), ("b"));
        assertEquals((String) list.get(1), ("c"));
        assertEquals((String) list.get(2), ("d"));
        assertEquals((String) list.get(3), ("e"));
    }

    @Test
    public void shouldAddAndRangeByLexNormalInf() {
        if (isRedisson()) {
            return; // redisson zRangeByLex bug. [[[B@47dbb1e2]
        }
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("key"), 1.0, bytes("a")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("key"), 1.0, bytes("c")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("key"), 1.0, bytes("f")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("key"), 1.0, bytes("b")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("key"), 1.0, bytes("d")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("key"), 1.0, bytes("e")));
        //a b c d e f
        //0 1 2 3 4 5
        RedisZSetCommands.Range range = RedisZSetCommands.Range.unbounded().lt("d");
        Set<byte[]> keys = t().execute((RedisCallback<Set<byte[]>>) con -> con.zRangeByLex(bytes("key"), range));

        List<Object> list = new ArrayList<>(keys);
        assertEquals(list.size(), 3);
        assertArrayEquals((byte[]) list.get(0), bytes("a"));
        assertArrayEquals((byte[]) list.get(1), bytes("b"));
        assertArrayEquals((byte[]) list.get(2), bytes("c"));
    }

    @Test
    public void shouldAddAndRevRangeByLexNormalInf() {
        if (isRedisson()) {
            return; // redisson zRangeByLex bug.
        }
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("key"), 1.0, bytes("a")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("key"), 1.0, bytes("c")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("key"), 1.0, bytes("f")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("key"), 1.0, bytes("b")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("key"), 1.0, bytes("d")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("key"), 1.0, bytes("e")));
        //a b c d e f
        //0 1 2 3 4 5
        RedisZSetCommands.Range range = RedisZSetCommands.Range.unbounded().gte("d");
        Set<byte[]> keys = t().execute((RedisCallback<Set<byte[]>>) con -> con.zRevRangeByLex(bytes("key"), range));

        List<Object> list = new ArrayList<>(keys);
        assertEquals(list.size(), 3);
        assertArrayEquals((byte[]) list.get(0), bytes("f"));
        assertArrayEquals((byte[]) list.get(1), bytes("e"));
        assertArrayEquals((byte[]) list.get(2), bytes("d"));
    }

    //zrangestore
    @Test
    public void shouldZRangestoreNormal() {
        if (!isJedis()) {
            //redisson & lettuce does not support zrangestore
            return;
        }
        v().set("dest", "value may be relpaced");
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, serialize("a")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 2.0, serialize("b")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 3.0, serialize("c")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 4.0, serialize("d")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 3.0, serialize("e")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 2.0, serialize("f")));
        //a b   f   c   e   d
        //1 2   2   3   3   4
        Object keys = t().execute((RedisCallback<Object>) con -> con.execute("zrangestore",
                new byte[][]{bytes("dest"), bytes("src"), bytes("2"), bytes("4")}));

        assertEquals(keys, 3L);
        Set<Object> r = z().range("dest", 0, -1);
        ArrayList<Object> list = new ArrayList<>(r);
        assertEquals(list.size(), 3);
        assertEquals(list.get(0), "f");
        assertEquals(list.get(1), "c");
        assertEquals(list.get(2), "e");
    }

    @Test
    public void shouldZRankNormal() {
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, serialize("a")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 2.0, serialize("b")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 3.0, serialize("c")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 4.0, serialize("d")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 3.0, serialize("e")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 2.0, serialize("f")));
        //a b   f   c   e   d
        //1 2   2   3   3   4
        assertEquals(z().rank("src", "a").longValue(), 0L);
        assertEquals(z().rank("src", "b").longValue(), 1L);
        assertEquals(z().rank("src", "f").longValue(), 2L);
        assertEquals(z().rank("src", "c").longValue(), 3L);
        assertEquals(z().rank("src", "e").longValue(), 4L);
        assertEquals(z().rank("src", "d").longValue(), 5L);

        assertNull(z().rank("src", "not-exist"));

    }

    @Test
    public void shouldZRevRankNormal() {
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, serialize("a")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 2.0, serialize("b")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 3.0, serialize("c")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 4.0, serialize("d")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 3.0, serialize("e")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 2.0, serialize("f")));
        //a b   f   c   e   d
        //1 2   2   3   3   4
        assertEquals(z().reverseRank("src", "a").longValue(), 5L);
        assertEquals(z().reverseRank("src", "b").longValue(), 4L);
        assertEquals(z().reverseRank("src", "f").longValue(), 3L);
        assertEquals(z().reverseRank("src", "c").longValue(), 2L);
        assertEquals(z().reverseRank("src", "e").longValue(), 1L);
        assertEquals(z().reverseRank("src", "d").longValue(), 0L);

        assertNull(z().rank("src", "not-exist"));

    }

    @Test
    public void shouldZRemRangeByRankNormal() {
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, serialize("a")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 2.0, serialize("b")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 3.0, serialize("c")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 4.0, serialize("d")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 3.0, serialize("e")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 2.0, serialize("f")));
        //a b   f   c   e   d
        //1 2   2   3   3   4
        Long removed = z().removeRange("src", 1, -2);
        assertEquals(removed.longValue(), 4L);

        ArrayList<Object> ele = new ArrayList<>(z().range("src", 0, -1));
        assertEquals(ele.size(), 2);
        assertEquals(ele.get(0), "a");
        assertEquals(ele.get(1), "d");

    }

    @Test
    public void shouldZRemRangeByRankAll() {
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, serialize("a")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 2.0, serialize("b")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 3.0, serialize("c")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 4.0, serialize("d")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 3.0, serialize("e")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 2.0, serialize("f")));
        //a b   f   c   e   d
        //1 2   2   3   3   4
        Long removed = z().removeRange("src", 0, -1);
        assertEquals(removed.longValue(), 6L);

        ArrayList<Object> ele = new ArrayList<>(z().range("src", 0, -1));
        assertEquals(ele.size(), 0);
        //key is removed,so can call Get
        assertNull(v().get("src"));
    }

    @Test
    public void shouldZRemRangeByScoreAll() {
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, serialize("a")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 2.0, serialize("b")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 3.0, serialize("c")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 4.0, serialize("d")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 3.0, serialize("e")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 2.0, serialize("f")));
        //a b   f   c   e   d
        //1 2   2   3   3   4
        Long removed = z().removeRangeByScore("src", 0, 5);
        assertEquals(removed.longValue(), 6L);

        ArrayList<Object> ele = new ArrayList<>(z().range("src", 0, -1));
        assertEquals(ele.size(), 0);
        //key is removed,so can call Get
        assertNull(v().get("src"));
    }

    @Test
    public void shouldZRemRangeByScoreEmpty1() {
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, serialize("a")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 2.0, serialize("b")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 3.0, serialize("c")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 4.0, serialize("d")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 3.0, serialize("e")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 2.0, serialize("f")));
        //a b   f   c   e   d
        //1 2   2   3   3   4
        Long removed = z().removeRangeByScore("src", -10, 0);
        assertEquals(removed.longValue(), 0L);

        ArrayList<Object> ele = new ArrayList<>(z().range("src", 0, -1));
        assertEquals(ele.size(), 6);

    }

    @Test
    public void shouldZRemRangeByScoreNormal() {
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, serialize("a")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 2.0, serialize("b")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 3.0, serialize("c")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 4.0, serialize("d")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 3.0, serialize("e")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 2.0, serialize("f")));
        //a b   f   c   e   d
        //1 2   2   3   3   4
        Long removed = z().removeRangeByScore("src", 2.0, 3.0);
        ArrayList<Object> ele = new ArrayList<>(z().range("src", 0, -1));

        assertEquals(removed.longValue(), 4L);
        assertEquals(ele.size(), 2);
        assertEquals(ele.get(0), "a");
        assertEquals(ele.get(1), "d");
    }

    @Test
    public void shouldZRemRangeByScoreNormal2() {
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, serialize("a")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 2.0, serialize("b")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 3.0, serialize("c")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 4.0, serialize("d")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 3.0, serialize("e")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 2.0, serialize("f")));
        //a b   f   c   e   d
        //1 2   2   3   3   4
        Long removed = t().execute((RedisCallback<Long>) con -> {
            RedisZSetCommands.Range range = RedisZSetCommands.Range.range().gt(2);
            return con.zRemRangeByScore(bytes("src"), range);
        });
        ArrayList<Object> ele = new ArrayList<>(z().range("src", 0, -1));

        assertEquals(removed.longValue(), 3L);
        assertEquals(ele.size(), 3);
        assertEquals(ele.get(0), "a");
        assertEquals(ele.get(1), "b");
        assertEquals(ele.get(2), "f");
    }

    @Test
    public void shouldZRemRangeByLexNormal() {
        if (isRedisson()) {
            return;// redisson bug : StackOverflowError when calling  zRemRangeByLex
        }
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, bytes("a")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, bytes("b")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, bytes("c")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, bytes("d")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, bytes("e")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, bytes("f")));
        //a b c d e f

        RedisZSetCommands.Range range = RedisZSetCommands.Range.range().gt("b").lte("e");
        Long removed = z().removeRangeByLex("src", range);
        Set<byte[]> l = t().execute((RedisCallback<Set<byte[]>>) con -> con.zRange(bytes("src"), 0, -1));
        ArrayList<byte[]> ele = new ArrayList<>(l);

        assertEquals(removed.longValue(), 3L);
        assertEquals(ele.size(), 3);
        assertArrayEquals(ele.get(0), bytes("a"));
        assertArrayEquals(ele.get(1), bytes("b"));
        assertArrayEquals(ele.get(2), bytes("f"));
    }

    @Test
    public void shouldZRemRangeByLexNormalWithEmptyRange() {
        if (isRedisson()) {
            return;// redisson bug : StackOverflowError when calling  zRemRangeByLex
        }
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, bytes("a")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, bytes("b")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, bytes("c")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, bytes("d")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, bytes("e")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, bytes("f")));
        //a b c d e f

        RedisZSetCommands.Range range = RedisZSetCommands.Range.range().gt("g");
        Long removed = z().removeRangeByLex("src", range);
        Set<byte[]> l = t().execute((RedisCallback<Set<byte[]>>) con -> con.zRange(bytes("src"), 0, -1));
        ArrayList<byte[]> ele = new ArrayList<>(l);

        assertEquals(removed.longValue(), 0L);
        assertEquals(ele.size(), 6);
    }

    @Test
    public void shouldZRemRangeByLexNormalToEmpty() {
        if (isRedisson()) {
            return;// redisson bug : StackOverflowError when calling  zRemRangeByLex
        }
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, bytes("a")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, bytes("b")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, bytes("c")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, bytes("d")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, bytes("e")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, bytes("f")));
        //a b c d e f

        RedisZSetCommands.Range range = RedisZSetCommands.Range.range();
        Long removed = z().removeRangeByLex("src", range);
        Set<byte[]> l = t().execute((RedisCallback<Set<byte[]>>) con -> con.zRange(bytes("src"), 0, -1));
        ArrayList<byte[]> ele = new ArrayList<>(l);

        assertEquals(removed.longValue(), 6L);
        assertEquals(ele.size(), 0);
    }

    @Test
    public void shouldZRemNormal() {
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.0, serialize("a")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 2.0, serialize("b")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 3.0, serialize("c")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 4.0, serialize("d")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 3.0, serialize("e")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 2.0, serialize("f")));
        //a b   f   c   e   d
        //1 2   2   3   3   4

        Long removed = z().remove("src", "a", "c", "g");
        Set<byte[]> l = t().execute((RedisCallback<Set<byte[]>>) con -> con.zRange(bytes("src"), 0, -1));
        ArrayList<byte[]> ele = new ArrayList<>(l);

        assertEquals(removed.longValue(), 2L);
        assertEquals(ele.size(), 4);
        assertArrayEquals(ele.get(0), serialize("b"));
        assertArrayEquals(ele.get(1), serialize("f"));
        assertArrayEquals(ele.get(2), serialize("e"));
        assertArrayEquals(ele.get(3), serialize("d"));
    }

    @Test
    public void shouldZMScore() {
        if (isRedisson()) {
            return; //redissonConnection zMScore not exist. StackOverflowError
        }
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.1, serialize("a")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 2.0, serialize("b")));
        //a b   f   c   e   d
        //1 2   2   3   3   4
        List<Double> s = z().score("src", "a", "d");
        assertEquals(s.size(), 2);
        assertEquals(NumberUtils.formatDouble(s.get(0)), "1.1");
        assertNull(s.get(1));
        assertEquals(z().score("not-exist", "a", "b").size(), 0);
    }

    @Test
    public void shouldZScore() {
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 1.1, serialize("a")));
        t().execute((RedisCallback<Object>) con -> con.zAdd(bytes("src"), 2.0, serialize("b")));

        assertEquals(NumberUtils.formatDouble(z().score("src", "a")), "1.1");
        assertNull(z().score("src", "d"));
        assertNull(z().score("not-exist-key", "d"));
    }

    @Test
    public void shouldZIncreBy0() {
        z().add("key", "a", 1.0D);
        z().add("key", "b", 1.1D);
        z().add("key", "c", 1.2D);
        z().add("key", "d", 1.3D);
        z().add("key", "e", 1.15D);
        z().add("key", "f", 1.25D);

        z().add("key", "b", 2.0D);
        //a    e    c   f       d       b
        //1.0  1.15 1.2 1.25    1.3     2
        Double newV = z().incrementScore("key", "a", 3D);
        assertEquals(NumberUtils.formatDouble(newV), "4");

        if (isRedisson()) {
            return;
        }
        List<ZSetOperations.TypedTuple<Object>> key = new ArrayList<>(z().popMax("key", 1));
        assertEquals(key.size(), 1);
        assertEquals(key.get(0).getValue(), "a");

    }

    @Test
    public void shouldZIncreBy1() {
        Double newV = z().incrementScore("key", "a", 3D);
        assertEquals(NumberUtils.formatDouble(newV), "3");
        assertEquals(z().size("key").intValue(), 1);
    }

    @Test
    public void shouldZIncreBy2() {
        z().add("key", "a", 4.5D);

        Double newV = z().incrementScore("key", "b", 3D);
        assertEquals(NumberUtils.formatDouble(newV), "3");
        assertEquals(z().size("key").intValue(), 2);
    }

    @Test
    public void shouldZUnionMin() {
        if (isRedisson()) {
            return;//StackOverflowError
        }
        z().add("key1", "a", 1.5D);
        z().add("key1", "b", 2.5D);
        z().add("key1", "c", 3.5D);

        z().add("key2", "a", 2.5D);
        z().add("key2", "b", 3.5D);
        z().add("key2", "d", 4.5D);

        z().add("key3", "a", 4.5D);
        z().add("key3", "b", 5.5D);
        z().add("key3", "e", 6.5D);
        
        List<ZSetOperations.TypedTuple<Object>> l = new LinkedList<>(z().unionWithScores("key1", Arrays.asList("key2", "key3"), RedisZSetCommands.Aggregate.MIN,
                RedisZSetCommands.Weights.of(2, 2, 1)));
        //a 3
        //b 5
        //e 6.5
        //c 7
        //d 9
        assertEquals(l.size(), 5);
        assertEquals(l.get(0).getValue(),"a");
        assertEquals(l.get(1).getValue(),"b");
        assertEquals(l.get(2).getValue(),"e");
        assertEquals(l.get(3).getValue(),"c");
        assertEquals(l.get(4).getValue(),"d");
        assertEquals(NumberUtils.formatDouble(l.get(0).getScore()),"3");
        assertEquals(NumberUtils.formatDouble(l.get(1).getScore()),"5");
        assertEquals(NumberUtils.formatDouble(l.get(2).getScore()),"6.5");
        assertEquals(NumberUtils.formatDouble(l.get(3).getScore()),"7");
        assertEquals(NumberUtils.formatDouble(l.get(4).getScore()),"9");
    }

    @Test
    public void shouldZInterMin() {
        if (isRedisson()) {
            return;//StackOverflowError
        }
        z().add("key1", "a", 1.5D);
        z().add("key1", "b", 2.5D);
        z().add("key1", "c", 3.5D);

        z().add("key2", "a", 2.5D);
        z().add("key2", "b", 3.5D);
        z().add("key2", "d", 4.5D);

        z().add("key3", "a", 4.5D);
        z().add("key3", "b", 5.5D);
        z().add("key3", "e", 6.5D);

        List<ZSetOperations.TypedTuple<Object>> l = new LinkedList<>(z().intersectWithScores("key1", Arrays.asList("key2", "key3"), RedisZSetCommands.Aggregate.MIN,
                RedisZSetCommands.Weights.of(2, 2, 1)));
        //a 3
        //b 5
      
        assertEquals(l.size(), 2);
        assertEquals(l.get(0).getValue(),"a");
        assertEquals(l.get(1).getValue(),"b");
        assertEquals(NumberUtils.formatDouble(l.get(0).getScore()),"3");
        assertEquals(NumberUtils.formatDouble(l.get(1).getScore()),"5");
         
    }

    @Test
    public void shouldZUnionMax() {
        if (isRedisson()) {
            return;//StackOverflowError
        }
        z().add("key1", "a", 1.5D);
        z().add("key1", "b", 2.5D);
        z().add("key1", "c", 3.5D);

        z().add("key2", "a", 2.5D);
        z().add("key2", "b", 3.5D);
        z().add("key2", "d", 4.5D);

        z().add("key3", "a", 4.5D);
        z().add("key3", "b", 5.5D);
        z().add("key3", "e", 6.5D);
        
        List<ZSetOperations.TypedTuple<Object>> l = new LinkedList<>(z().unionWithScores("key1", Arrays.asList("key2", "key3"), RedisZSetCommands.Aggregate.MAX,
                RedisZSetCommands.Weights.of(2, 2, 1)));
        //a 5
        //e 6.5
        //b 7
        //c 7
        //d 9
        assertEquals(l.size(), 5);
        assertEquals(l.get(0).getValue(),"a");
        assertEquals(l.get(1).getValue(),"e");
        assertEquals(l.get(2).getValue(),"b");
        assertEquals(l.get(3).getValue(),"c");
        assertEquals(l.get(4).getValue(),"d");
        assertEquals(NumberUtils.formatDouble(l.get(0).getScore()),"5");
        assertEquals(NumberUtils.formatDouble(l.get(1).getScore()),"6.5");
        assertEquals(NumberUtils.formatDouble(l.get(2).getScore()),"7");
        assertEquals(NumberUtils.formatDouble(l.get(3).getScore()),"7");
        assertEquals(NumberUtils.formatDouble(l.get(4).getScore()),"9");
    }

    @Test
    public void shouldZInterMax() {
        if (isRedisson()) {
            return;//StackOverflowError
        }
        z().add("key1", "a", 1.5D);
        z().add("key1", "b", 2.5D);
        z().add("key1", "c", 3.5D);

        z().add("key2", "a", 2.5D);
        z().add("key2", "b", 3.5D);
        z().add("key2", "d", 4.5D);

        z().add("key3", "a", 4.5D);
        z().add("key3", "b", 5.5D);
        z().add("key3", "e", 6.5D);

        List<ZSetOperations.TypedTuple<Object>> l = new LinkedList<>(z().intersectWithScores("key1", Arrays.asList("key2", "key3"), RedisZSetCommands.Aggregate.MAX,
                RedisZSetCommands.Weights.of(2, 2, 1)));
        //a 5
        //b 7

        assertEquals(l.size(), 2);
        assertEquals(l.get(0).getValue(),"a");
        assertEquals(l.get(1).getValue(),"b");
        assertEquals(NumberUtils.formatDouble(l.get(0).getScore()),"5");
        assertEquals(NumberUtils.formatDouble(l.get(1).getScore()),"7");

    }
    
    @Test
    public void shouldZUnionSum() {
        if (isRedisson()) {
            return;//StackOverflowError
        }
        z().add("key1", "a", 1.5D);
        z().add("key1", "b", 2.5D);
        z().add("key1", "c", 3.5D);

        z().add("key2", "a", 2.5D);
        z().add("key2", "b", 3.5D);
        z().add("key2", "d", 4.5D);

        z().add("key3", "a", 4.5D);
        z().add("key3", "b", 5.5D);
        z().add("key3", "e", 6.5D);
        
        List<ZSetOperations.TypedTuple<Object>> l = new LinkedList<>(z().unionWithScores("key1", Arrays.asList("key2", "key3"), RedisZSetCommands.Aggregate.SUM,
                RedisZSetCommands.Weights.of(2, 2, 1)));
        //e 6.5
        //c 7
        //d 9
        //a 12.5
        //b 17.5
        assertEquals(l.size(), 5);
        assertEquals(l.get(0).getValue(),"e");
        assertEquals(l.get(1).getValue(),"c");
        assertEquals(l.get(2).getValue(),"d");
        assertEquals(l.get(3).getValue(),"a");
        assertEquals(l.get(4).getValue(),"b");
        assertEquals(NumberUtils.formatDouble(l.get(0).getScore()),"6.5");
        assertEquals(NumberUtils.formatDouble(l.get(1).getScore()),"7");
        assertEquals(NumberUtils.formatDouble(l.get(2).getScore()),"9");
        assertEquals(NumberUtils.formatDouble(l.get(3).getScore()),"12.5");
        assertEquals(NumberUtils.formatDouble(l.get(4).getScore()),"17.5");
    }

    @Test
    public void shouldZInterSum() {
        if (isRedisson()) {
            return;//StackOverflowError
        }
        z().add("key1", "a", 1.5D);
        z().add("key1", "b", 2.5D);
        z().add("key1", "c", 3.5D);

        z().add("key2", "a", 2.5D);
        z().add("key2", "b", 3.5D);
        z().add("key2", "d", 4.5D);

        z().add("key3", "a", 4.5D);
        z().add("key3", "b", 5.5D);
        z().add("key3", "e", 6.5D);

        List<ZSetOperations.TypedTuple<Object>> l = new LinkedList<>(z().intersectWithScores("key1", Arrays.asList("key2", "key3"), RedisZSetCommands.Aggregate.SUM,
                RedisZSetCommands.Weights.of(2, 2, 1)));
        //a 12.5
        //b 17.5

        assertEquals(l.size(), 2);
        assertEquals(l.get(0).getValue(),"a");
        assertEquals(l.get(1).getValue(),"b");
        assertEquals(NumberUtils.formatDouble(l.get(0).getScore()),"12.5");
        assertEquals(NumberUtils.formatDouble(l.get(1).getScore()),"17.5");

    }
}
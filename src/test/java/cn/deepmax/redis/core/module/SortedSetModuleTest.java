package cn.deepmax.redis.core.module;

import cn.deepmax.redis.base.BaseTemplateTest;
import cn.deepmax.redis.utils.NumberUtils;
import org.junit.Test;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * @author wudi
 * @date 2021/12/30
 */
public class SortedSetModuleTest extends BaseTemplateTest {
    public SortedSetModuleTest(RedisTemplate<String, Object> redisTemplate) {
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
        RedisZSetCommands.Range range = RedisZSetCommands.Range.unbounded().lt("d");
        Set<byte[]> keys = t().execute((RedisCallback<Set<byte[]>>) con -> con.zRangeByLex(bytes("key"), range));

        List<Object> list = new ArrayList<>(keys);
        assertEquals(list.size(), 3);
        assertArrayEquals((byte[]) list.get(0), bytes("a"));
        assertArrayEquals((byte[]) list.get(1), bytes("b"));
        assertArrayEquals((byte[]) list.get(2), bytes("c"));
    }

}
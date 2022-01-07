package cn.deepmax.redis.core.module;

import cn.deepmax.redis.base.ByteHelper;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.utils.NumberUtils;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.*;

/**
 * @author wudi
 * @date 2022/1/7
 */
public class SortedSetModuleUnitTest implements ByteHelper {
    @Test
    public void shouldParseNormal() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZINTER 3 key1 key2 key3");

        Optional<SortedSetModule.ComplexArg> arg = SortedSetModule.parseZArg(msg, 1);
        assertTrue(arg.isPresent());
        assertEquals(arg.get().keys.get(0),new Key(bytes("key1")));
        assertEquals(arg.get().keys.get(1),new Key(bytes("key2")));
        assertEquals(arg.get().keys.get(2),new Key(bytes("key3")));
        assertTrue(arg.get().weights.isEmpty());
        assertFalse(arg.get().withScores);
        assertEquals(arg.get().type, SortedSetModule.AggType.SUM);
    }

    @Test
    public void shouldParseNormalWithscores() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZINTER 3 key1 key2 key3 withscores");

        Optional<SortedSetModule.ComplexArg> arg = SortedSetModule.parseZArg(msg, 1);
        assertTrue(arg.isPresent());
        assertEquals(arg.get().keys.get(0),new Key(bytes("key1")));
        assertEquals(arg.get().keys.get(1),new Key(bytes("key2")));
        assertEquals(arg.get().keys.get(2),new Key(bytes("key3")));
        assertTrue(arg.get().weights.isEmpty());
        assertTrue(arg.get().withScores);
        assertEquals(arg.get().type, SortedSetModule.AggType.SUM);
    }

    @Test
    public void shouldParseNormalWeight() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZINTER 3 key1 key2 key3 weights 1 2 3.1");

        Optional<SortedSetModule.ComplexArg> arg = SortedSetModule.parseZArg(msg, 1);
        assertTrue(arg.isPresent());
        assertEquals(arg.get().keys.get(0),new Key(bytes("key1")));
        assertEquals(arg.get().keys.get(1),new Key(bytes("key2")));
        assertEquals(arg.get().keys.get(2),new Key(bytes("key3")));
        assertEquals(NumberUtils.formatDouble(arg.get().weights.get(0)),"1");
        assertEquals(NumberUtils.formatDouble(arg.get().weights.get(1)),"2");
        assertEquals(NumberUtils.formatDouble(arg.get().weights.get(2)),"3.1");

        assertFalse(arg.get().withScores);
        assertEquals(arg.get().type, SortedSetModule.AggType.SUM);
    }

    @Test
    public void shouldParseNormalWeightAgg() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZINTER 3 key1 key2 key3 weights 1 2 3.1 aggregate min");

        Optional<SortedSetModule.ComplexArg> arg = SortedSetModule.parseZArg(msg, 1);
        assertTrue(arg.isPresent());
        assertEquals(arg.get().keys.get(0),new Key(bytes("key1")));
        assertEquals(arg.get().keys.get(1),new Key(bytes("key2")));
        assertEquals(arg.get().keys.get(2),new Key(bytes("key3")));
        assertEquals(NumberUtils.formatDouble(arg.get().weights.get(0)),"1");
        assertEquals(NumberUtils.formatDouble(arg.get().weights.get(1)),"2");
        assertEquals(NumberUtils.formatDouble(arg.get().weights.get(2)),"3.1");

        assertFalse(arg.get().withScores);
        assertEquals(arg.get().type, SortedSetModule.AggType.MIN);
    }

    @Test
    public void shouldParseNormalWeightAggWithScores() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZINTER 3 key1 key2 key3 weights 1 2 3.1 aggregate min withscores");

        Optional<SortedSetModule.ComplexArg> arg = SortedSetModule.parseZArg(msg, 1);
        assertTrue(arg.isPresent());
        assertEquals(arg.get().keys.get(0),new Key(bytes("key1")));
        assertEquals(arg.get().keys.get(1),new Key(bytes("key2")));
        assertEquals(arg.get().keys.get(2),new Key(bytes("key3")));
        assertEquals(NumberUtils.formatDouble(arg.get().weights.get(0)),"1");
        assertEquals(NumberUtils.formatDouble(arg.get().weights.get(1)),"2");
        assertEquals(NumberUtils.formatDouble(arg.get().weights.get(2)),"3.1");

        assertTrue(arg.get().withScores);
        assertEquals(arg.get().type, SortedSetModule.AggType.MIN);
    }

    @Test
    public void shouldParseNormalWeightWithScores() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZINTER 3 key1 key2 key3 weights 1 2 3.1 withscores");

        Optional<SortedSetModule.ComplexArg> arg = SortedSetModule.parseZArg(msg, 1);
        assertTrue(arg.isPresent());
        assertEquals(arg.get().keys.get(0),new Key(bytes("key1")));
        assertEquals(arg.get().keys.get(1),new Key(bytes("key2")));
        assertEquals(arg.get().keys.get(2),new Key(bytes("key3")));
        assertEquals(NumberUtils.formatDouble(arg.get().weights.get(0)),"1");
        assertEquals(NumberUtils.formatDouble(arg.get().weights.get(1)),"2");
        assertEquals(NumberUtils.formatDouble(arg.get().weights.get(2)),"3.1");

        assertTrue(arg.get().withScores);
        assertEquals(arg.get().type, SortedSetModule.AggType.SUM);
    }

    @Test
    public void shouldParseNormalWeightWithScores2() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZINTER 3 key1 key2 key3 weights 1 2 3.1 withscores aggregate min");

        Optional<SortedSetModule.ComplexArg> arg = SortedSetModule.parseZArg(msg, 1);
        assertTrue(arg.isPresent());
        assertEquals(arg.get().keys.get(0),new Key(bytes("key1")));
        assertEquals(arg.get().keys.get(1),new Key(bytes("key2")));
        assertEquals(arg.get().keys.get(2),new Key(bytes("key3")));
        assertEquals(NumberUtils.formatDouble(arg.get().weights.get(0)),"1");
        assertEquals(NumberUtils.formatDouble(arg.get().weights.get(1)),"2");
        assertEquals(NumberUtils.formatDouble(arg.get().weights.get(2)),"3.1");

        assertTrue(arg.get().withScores);
        assertEquals(arg.get().type, SortedSetModule.AggType.MIN);
    }

    @Test
    public void shouldErrorWhenNumberMismatch() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZINTER 3 key1 key2");

        Optional<SortedSetModule.ComplexArg> arg = SortedSetModule.parseZArg(msg, 1);
        assertFalse(arg.isPresent());
    }

    @Test
    public void shouldErrorWhenWeightNumberMismatch() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZINTER 3 key1 key2 key3 weights 1 2.3");

        Optional<SortedSetModule.ComplexArg> arg = SortedSetModule.parseZArg(msg, 1);
        assertFalse(arg.isPresent());
    }

    @Test
    public void shouldErrorWhenAggTypeError() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZINTER 3 key1 key2 key3 weights 1 2 3.2 aggregate any");

        Optional<SortedSetModule.ComplexArg> arg = SortedSetModule.parseZArg(msg, 1);
        assertFalse(arg.isPresent());
    }

    @Test
    public void shouldErrorWhenAggTypeNotExist() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZINTER 3 key1 key2 key3 weights 1 2 3.2 aggregate");

        Optional<SortedSetModule.ComplexArg> arg = SortedSetModule.parseZArg(msg, 1);
        assertFalse(arg.isPresent());
    }

    @Test
    public void shouldErrorWhenScores() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZINTER 3 key1 key2 key3 weights 1 2 3.2 withscoresFalse");

        Optional<SortedSetModule.ComplexArg> arg = SortedSetModule.parseZArg(msg, 1);
        assertFalse(arg.isPresent());
    }

    @Test
    public void shouldErrorWhenScoresNotLast() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZINTER 3 key1 key2 key3 weights 1 2 3.2 withscores other");

        Optional<SortedSetModule.ComplexArg> arg = SortedSetModule.parseZArg(msg, 1);
        assertFalse(arg.isPresent());
    }
}

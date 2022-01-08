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
        assertEquals(arg.get().keys.get(0), new Key(bytes("key1")));
        assertEquals(arg.get().keys.get(1), new Key(bytes("key2")));
        assertEquals(arg.get().keys.get(2), new Key(bytes("key3")));
        assertTrue(arg.get().weights.isEmpty());
        assertTrue(arg.get().withScores);
        assertEquals(arg.get().type, SortedSetModule.AggType.SUM);
    }

    @Test
    public void shouldErrorParseNormalWithscores() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZINTER 3 key1 key2 key3 withscores");

        Optional<SortedSetModule.ComplexArg> arg = SortedSetModule.parseZArgStore(msg, 1);
        assertFalse(arg.isPresent());
    }

    @Test
    public void shouldParseNormalWeight() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZINTER 3 key1 key2 key3 weights 1 2 3.1");

        Optional<SortedSetModule.ComplexArg> arg = SortedSetModule.parseZArg(msg, 1);
        assertTrue(arg.isPresent());
        assertEquals(arg.get().keys.get(0), new Key(bytes("key1")));
        assertEquals(arg.get().keys.get(1), new Key(bytes("key2")));
        assertEquals(arg.get().keys.get(2), new Key(bytes("key3")));
        assertEquals(NumberUtils.formatDouble(arg.get().weights.get(0)), "1");
        assertEquals(NumberUtils.formatDouble(arg.get().weights.get(1)), "2");
        assertEquals(NumberUtils.formatDouble(arg.get().weights.get(2)), "3.1");

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

    @Test
    public void shouldParseZAddNormal() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZADD key 1.3 one 2.3 two 3 three");

        Optional<SortedSetModule.ZAddArg> arg = SortedSetModule.parseZAdd(msg, 2);

        assertTrue(arg.isPresent());
        SortedSetModule.ZAddArg ag = arg.get();
        assertEquals(ag.values.get(0).ele, new Key(bytes("one")));
        assertEquals(ag.values.get(1).ele, new Key(bytes("two")));
        assertEquals(ag.values.get(2).ele, new Key(bytes("three")));
        assertEquals(NumberUtils.formatDouble(ag.values.get(0).score), "1.3");
        assertEquals(NumberUtils.formatDouble(ag.values.get(1).score), "2.3");
        assertEquals(NumberUtils.formatDouble(ag.values.get(2).score), "3");
        assertFalse(ag.errorMsg().isPresent());
    }

    @Test
    public void shouldParseZAddNormalNx() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZADD key nx 1.3 one 2.3 two 3 three");

        Optional<SortedSetModule.ZAddArg> arg = SortedSetModule.parseZAdd(msg, 2);

        assertTrue(arg.isPresent());
        SortedSetModule.ZAddArg ag = arg.get();
        assertEquals(ag.values.get(0).ele, new Key(bytes("one")));
        assertEquals(ag.values.get(1).ele, new Key(bytes("two")));
        assertEquals(ag.values.get(2).ele, new Key(bytes("three")));
        assertEquals(NumberUtils.formatDouble(ag.values.get(0).score), "1.3");
        assertEquals(NumberUtils.formatDouble(ag.values.get(1).score), "2.3");
        assertEquals(NumberUtils.formatDouble(ag.values.get(2).score), "3");
        assertTrue(ag.nx);
        assertFalse(ag.errorMsg().isPresent());
    }

    @Test
    public void shouldParseZAddNormalXx() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZADD key xx 1.3 one 2.3 two 3 three");

        Optional<SortedSetModule.ZAddArg> arg = SortedSetModule.parseZAdd(msg, 2);

        assertTrue(arg.isPresent());
        SortedSetModule.ZAddArg ag = arg.get();
        assertEquals(ag.values.get(0).ele, new Key(bytes("one")));
        assertEquals(ag.values.get(1).ele, new Key(bytes("two")));
        assertEquals(ag.values.get(2).ele, new Key(bytes("three")));
        assertEquals(NumberUtils.formatDouble(ag.values.get(0).score), "1.3");
        assertEquals(NumberUtils.formatDouble(ag.values.get(1).score), "2.3");
        assertEquals(NumberUtils.formatDouble(ag.values.get(2).score), "3");
        assertTrue(ag.xx);
        assertFalse(ag.errorMsg().isPresent());
    }

    @Test
    public void shouldParseZAddNormalGt() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZADD key xx gt 1.3 one 2.3 two 3 three");

        Optional<SortedSetModule.ZAddArg> arg = SortedSetModule.parseZAdd(msg, 2);

        assertTrue(arg.isPresent());
        SortedSetModule.ZAddArg ag = arg.get();
        assertEquals(ag.values.get(0).ele, new Key(bytes("one")));
        assertEquals(ag.values.get(1).ele, new Key(bytes("two")));
        assertEquals(ag.values.get(2).ele, new Key(bytes("three")));
        assertEquals(NumberUtils.formatDouble(ag.values.get(0).score), "1.3");
        assertEquals(NumberUtils.formatDouble(ag.values.get(1).score), "2.3");
        assertEquals(NumberUtils.formatDouble(ag.values.get(2).score), "3");
        assertTrue(ag.xx);
        assertTrue(ag.gt);
        assertFalse(ag.errorMsg().isPresent());
    }

    @Test
    public void shouldParseZAddNormallt() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZADD key lt xx 1.3 one 2.3 two 3 three");

        Optional<SortedSetModule.ZAddArg> arg = SortedSetModule.parseZAdd(msg, 2);

        assertTrue(arg.isPresent());
        SortedSetModule.ZAddArg ag = arg.get();
        assertEquals(ag.values.get(0).ele, new Key(bytes("one")));
        assertEquals(ag.values.get(1).ele, new Key(bytes("two")));
        assertEquals(ag.values.get(2).ele, new Key(bytes("three")));
        assertEquals(NumberUtils.formatDouble(ag.values.get(0).score), "1.3");
        assertEquals(NumberUtils.formatDouble(ag.values.get(1).score), "2.3");
        assertEquals(NumberUtils.formatDouble(ag.values.get(2).score), "3");
        assertTrue(ag.xx);
        assertTrue(ag.lt);
        assertFalse(ag.errorMsg().isPresent());
    }

    @Test
    public void shouldParseZAddNormalLtCh() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZADD key ch lt xx 1.3 one 2.3 two 3 three");

        Optional<SortedSetModule.ZAddArg> arg = SortedSetModule.parseZAdd(msg, 2);

        assertTrue(arg.isPresent());
        SortedSetModule.ZAddArg ag = arg.get();
        assertEquals(ag.values.get(0).ele, new Key(bytes("one")));
        assertEquals(ag.values.get(1).ele, new Key(bytes("two")));
        assertEquals(ag.values.get(2).ele, new Key(bytes("three")));
        assertEquals(NumberUtils.formatDouble(ag.values.get(0).score), "1.3");
        assertEquals(NumberUtils.formatDouble(ag.values.get(1).score), "2.3");
        assertEquals(NumberUtils.formatDouble(ag.values.get(2).score), "3");
        assertTrue(ag.xx);
        assertTrue(ag.lt);
        assertTrue(ag.ch);
        assertFalse(ag.errorMsg().isPresent());
    }

    @Test
    public void shouldParseZAddNormalLtIncr() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZADD key incr lt xx 1.3 one");

        Optional<SortedSetModule.ZAddArg> arg = SortedSetModule.parseZAdd(msg, 2);

        assertTrue(arg.isPresent());
        SortedSetModule.ZAddArg ag = arg.get();
        assertEquals(ag.values.get(0).ele, new Key(bytes("one")));
        assertEquals(NumberUtils.formatDouble(ag.values.get(0).score), "1.3");
        assertTrue(ag.xx);
        assertTrue(ag.lt);
        assertTrue(ag.incr);
        assertFalse(ag.errorMsg().isPresent());
    }

    @Test
    public void shouldParseZAddNormalLtIncrError() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZADD key incr lt xx 1.3 one 2 two");

        Optional<SortedSetModule.ZAddArg> arg = SortedSetModule.parseZAdd(msg, 2);

        assertTrue(arg.isPresent());
        SortedSetModule.ZAddArg ag = arg.get();
        assertEquals(ag.values.get(0).ele, new Key(bytes("one")));
        assertEquals(NumberUtils.formatDouble(ag.values.get(0).score), "1.3");
        assertTrue(ag.xx);
        assertTrue(ag.lt);
        assertTrue(ag.incr);
        assertTrue(ag.errorMsg().get().contains("INCR option supports a single increment-element pair"));
    }

    @Test
    public void shouldParseZAddNormalLtIncrError2() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZADD key incr lt xx 1.3 one 2 two 333");

        Optional<SortedSetModule.ZAddArg> arg = SortedSetModule.parseZAdd(msg, 2);

        assertFalse(arg.isPresent());
    }


    @Test
    public void shouldErrorParseZAddNormalNxXX() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZADD key xx nx 1.3 one 2.3 two 3 three");

        Optional<SortedSetModule.ZAddArg> arg = SortedSetModule.parseZAdd(msg, 2);

        assertTrue(arg.isPresent());
        SortedSetModule.ZAddArg ag = arg.get();
        assertEquals(ag.values.get(0).ele, new Key(bytes("one")));
        assertEquals(ag.values.get(1).ele, new Key(bytes("two")));
        assertEquals(ag.values.get(2).ele, new Key(bytes("three")));
        assertEquals(NumberUtils.formatDouble(ag.values.get(0).score), "1.3");
        assertEquals(NumberUtils.formatDouble(ag.values.get(1).score), "2.3");
        assertEquals(NumberUtils.formatDouble(ag.values.get(2).score), "3");
        assertTrue(ag.xx);
        assertTrue(ag.nx);
        assertTrue(ag.errorMsg().get().contains("XX and NX options at the same time are not compatible"));
    }

    @Test
    public void shouldErrorParseZAddNormalGtNx() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZADD key gt nx 1.3 one 2.3 two 3 three");

        Optional<SortedSetModule.ZAddArg> arg = SortedSetModule.parseZAdd(msg, 2);

        assertTrue(arg.isPresent());
        SortedSetModule.ZAddArg ag = arg.get();
        assertEquals(ag.values.get(0).ele, new Key(bytes("one")));
        assertEquals(ag.values.get(1).ele, new Key(bytes("two")));
        assertEquals(ag.values.get(2).ele, new Key(bytes("three")));
        assertEquals(NumberUtils.formatDouble(ag.values.get(0).score), "1.3");
        assertEquals(NumberUtils.formatDouble(ag.values.get(1).score), "2.3");
        assertEquals(NumberUtils.formatDouble(ag.values.get(2).score), "3");
        assertTrue(ag.gt);
        assertTrue(ag.nx);
        assertTrue(ag.errorMsg().get().contains("GT, LT, and/or NX options at the same time are not compatible"));
    }

    @Test
    public void shouldErrorParseZAddNormalLtNx() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZADD key nx lt 1.3 one 2.3 two 3 three");

        Optional<SortedSetModule.ZAddArg> arg = SortedSetModule.parseZAdd(msg, 2);

        assertTrue(arg.isPresent());
        SortedSetModule.ZAddArg ag = arg.get();
        assertEquals(ag.values.get(0).ele, new Key(bytes("one")));
        assertEquals(ag.values.get(1).ele, new Key(bytes("two")));
        assertEquals(ag.values.get(2).ele, new Key(bytes("three")));
        assertEquals(NumberUtils.formatDouble(ag.values.get(0).score), "1.3");
        assertEquals(NumberUtils.formatDouble(ag.values.get(1).score), "2.3");
        assertEquals(NumberUtils.formatDouble(ag.values.get(2).score), "3");
        assertTrue(ag.lt);
        assertTrue(ag.nx);
        assertTrue(ag.errorMsg().get().contains("GT, LT, and/or NX options at the same time are not compatible"));
    }

    @Test
    public void shouldErrorParseZAddNormalGtLt() {
        ListRedisMessage msg = ListRedisMessage.ofString("ZADD key gt lt 1.3 one 2.3 two 3 three");

        Optional<SortedSetModule.ZAddArg> arg = SortedSetModule.parseZAdd(msg, 2);

        assertTrue(arg.isPresent());
        SortedSetModule.ZAddArg ag = arg.get();
        assertEquals(ag.values.get(0).ele, new Key(bytes("one")));
        assertEquals(ag.values.get(1).ele, new Key(bytes("two")));
        assertEquals(ag.values.get(2).ele, new Key(bytes("three")));
        assertEquals(NumberUtils.formatDouble(ag.values.get(0).score), "1.3");
        assertEquals(NumberUtils.formatDouble(ag.values.get(1).score), "2.3");
        assertEquals(NumberUtils.formatDouble(ag.values.get(2).score), "3");
        assertTrue(ag.lt);
        assertTrue(ag.gt);
        assertTrue(ag.errorMsg().get().contains("GT, LT, and/or NX options at the same time are not compatible"));
    }

}

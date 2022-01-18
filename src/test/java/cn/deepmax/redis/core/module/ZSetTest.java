package cn.deepmax.redis.core.module;

import cn.deepmax.redis.utils.Range;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.*;

/**
 * @author wudi
 */
public class ZSetTest {

    @Test
    public void shouldInsert() {
        for (int i = 0; i < 1000; i++) {
            testOnce();
            testAlotValue();
            testAlotValueByScore();
            testAlotValueByScoreLastInrange();
        }
    }

    private void testAlotValue() {
        ZSet.ZSkipList<BigDecimal, String> list = ZSet.ZSkipList.newInstance();
        //do 1
        list.insert(BigDecimal.valueOf(1), "A");
        list.insert(BigDecimal.valueOf(2), "B");
        list.insert(BigDecimal.valueOf(3), "C");
        list.insert(BigDecimal.valueOf(4), "D");
        list.insert(BigDecimal.valueOf(5), "E");
        list.insert(BigDecimal.valueOf(6), "F");
        list.insert(BigDecimal.valueOf(7), "G");
        list.insert(BigDecimal.valueOf(6), "H");
        list.insert(BigDecimal.valueOf(5), "I");
        list.insert(BigDecimal.valueOf(4), "J");
        list.insert(BigDecimal.valueOf(3), "K");

        assertEquals(list.getByIndex(0).ele, "A");
        assertEquals(list.getByIndex(1).ele, "B");
        assertEquals(list.getByIndex(2).ele, "C");
        assertEquals(list.getByIndex(3).ele, "K");
        assertEquals(list.getByIndex(4).ele, "D");
        assertEquals(list.getByIndex(5).ele, "J");
        assertEquals(list.getByIndex(6).ele, "E");
        assertEquals(list.getByIndex(7).ele, "I");
        assertEquals(list.getByIndex(8).ele, "F");
        assertEquals(list.getByIndex(9).ele, "H");
        assertEquals(list.getByIndex(10).ele, "G");
        assertNull(list.getByIndex(11));
    }

    private void testAlotValueByScore() {
        ZSet.ZSkipList<BigDecimal, String> list = ZSet.ZSkipList.newInstance();
        //do 1
        list.insert(BigDecimal.valueOf(1), "A");
        list.insert(BigDecimal.valueOf(2), "B");
        list.insert(BigDecimal.valueOf(3), "C");
        list.insert(BigDecimal.valueOf(4), "D");
        list.insert(BigDecimal.valueOf(5), "E");
        list.insert(BigDecimal.valueOf(6), "F");
        list.insert(BigDecimal.valueOf(7), "G");
        list.insert(BigDecimal.valueOf(6), "H");
        list.insert(BigDecimal.valueOf(5), "I");
        list.insert(BigDecimal.valueOf(4), "J");
        list.insert(BigDecimal.valueOf(3), "K");

        assertNull(list.zslLastInRange(endRange(-1, true)));
        assertNull(list.zslLastInRange(endRange(-1, false)));
        
        assertNull(list.zslLastInRange(endRange(1, true)));
        assertEquals(list.zslLastInRange(endRange(1, false)).ele, "A");

        assertEquals(list.zslLastInRange(endRange(2, true)).ele, "A");
        assertEquals(list.zslLastInRange(endRange(2, false)).ele, "B");

        assertEquals(list.zslLastInRange(endRange(3, true)).ele, "B");
        assertEquals(list.zslLastInRange(endRange(3, false)).ele, "K");

        assertEquals(list.zslLastInRange(endRange(4, true)).ele, "K");
        assertEquals(list.zslLastInRange(endRange(4, false)).ele, "J");
        
        assertEquals(list.zslLastInRange(endRange(5, true)).ele, "J");
        assertEquals(list.zslLastInRange(endRange(5, false)).ele, "I");

        assertEquals(list.zslLastInRange(endRange(6, true)).ele, "I");
        assertEquals(list.zslLastInRange(endRange(6, false)).ele, "H");

        assertEquals(list.zslLastInRange(endRange(7, true)).ele, "H");
        assertEquals(list.zslLastInRange(endRange(7, false)).ele, "G");

        assertEquals(list.zslLastInRange(endRange(8, true)).ele, "G");
        assertEquals(list.zslLastInRange(endRange(8, false)).ele, "G");
    }

    private void testAlotValueByScoreLastInrange() {
        ZSet.ZSkipList<BigDecimal, String> list = ZSet.ZSkipList.newInstance();
        //do 1
        list.insert(BigDecimal.valueOf(1), "A");
        list.insert(BigDecimal.valueOf(2), "B");
        list.insert(BigDecimal.valueOf(3), "C");
        list.insert(BigDecimal.valueOf(4), "D");
        list.insert(BigDecimal.valueOf(5), "E");
        list.insert(BigDecimal.valueOf(6), "F");
        list.insert(BigDecimal.valueOf(7), "G");
        list.insert(BigDecimal.valueOf(6), "H");
        list.insert(BigDecimal.valueOf(5), "I");
        list.insert(BigDecimal.valueOf(4), "J");
        list.insert(BigDecimal.valueOf(3), "K");

        assertEquals(list.zslFirstInRange(startRange(-1, false)).ele, "A");
        assertEquals(list.zslFirstInRange(startRange(-1, true)).ele, "A");

        assertEquals(list.zslFirstInRange(startRange(1, false)).ele, "A");
        assertEquals(list.zslFirstInRange(startRange(1, true)).ele, "B");

        assertEquals(list.zslFirstInRange(startRange(2, false)).ele, "B");
        assertEquals(list.zslFirstInRange(startRange(2, true)).ele, "C");

        assertEquals(list.zslFirstInRange(startRange(3, false)).ele, "C");
        assertEquals(list.zslFirstInRange(startRange(3, true)).ele, "D");

        assertEquals(list.zslFirstInRange(startRange(4, false)).ele, "D");
        assertEquals(list.zslFirstInRange(startRange(4, true)).ele, "E");

        assertEquals(list.zslFirstInRange(startRange(5, false)).ele, "E");
        assertEquals(list.zslFirstInRange(startRange(5, true)).ele, "F");

        assertEquals(list.zslFirstInRange(startRange(6, false)).ele, "F");
        assertEquals(list.zslFirstInRange(startRange(6, true)).ele, "G");

        assertEquals(list.zslFirstInRange(startRange(7, false)).ele, "G");
        assertNull(list.zslFirstInRange(startRange(7, true)));

        assertNull(list.zslFirstInRange(startRange(8, true)));
        assertNull(list.zslFirstInRange(startRange(8, false)));

    }

    private Range<BigDecimal> startRange(int i, boolean open) {
        return new Range<>(BigDecimal.valueOf(i), BigDecimal.valueOf(10000000L), open, true);
    }


    private Range<BigDecimal> endRange(int i, boolean open) {
        return new Range<>(BigDecimal.valueOf(-10000000L), BigDecimal.valueOf(i), true, open);
    }

    private void testOnce() {
        ZSet.ZSkipList<BigDecimal, String> list = ZSet.ZSkipList.newInstance();
        //do 1
        list.insert(BigDecimal.valueOf(5), "A");
        list.insert(BigDecimal.valueOf(10), "B");
        list.insert(BigDecimal.valueOf(1), "A");
        //then 1
        assertEquals(list.length, 3L);
        assertEquals(list.getRank("A", BigDecimal.valueOf(1)), 1L);
        assertEquals(list.getRank("A", BigDecimal.valueOf(5)), 2L);
        assertEquals(list.getRank("B", BigDecimal.valueOf(10)), 3L);
        assertEquals(list.getRank("B", BigDecimal.valueOf(11)), 0L);

        ZSet.ZSkipListNode<BigDecimal, String> node;
        node = list.getByIndex(0);
        assertEquals(node.ele, "A");
        assertEquals(node.score, BigDecimal.valueOf(1));
        node = list.getByIndex(1);
        assertEquals(node.ele, "A");
        assertEquals(node.score, BigDecimal.valueOf(5));
        node = list.getByIndex(2);
        assertEquals(node.ele, "B");
        assertEquals(node.score, BigDecimal.valueOf(10));
        node = list.getByIndex(3);
        assertNull(node);
        assertSame(list.getByIndex(0), list.getByIndex(-3));
        assertSame(list.getByIndex(1), list.getByIndex(-2));
        assertSame(list.getByIndex(2), list.getByIndex(-1));
        //do 2
        list.delete(BigDecimal.valueOf(5), "A");
        //then 2
        assertEquals(list.length, 2L);
        assertEquals(list.getRank("A", BigDecimal.valueOf(1)), 1L);
        assertEquals(list.getRank("B", BigDecimal.valueOf(10)), 2L);
        node = list.getByIndex(0);
        assertEquals(node.ele, "A");
        assertEquals(node.score, BigDecimal.valueOf(1));
        node = list.getByIndex(1);
        assertEquals(node.ele, "B");
        assertEquals(node.score, BigDecimal.valueOf(10));
        assertSame(list.getByIndex(1), list.getByIndex(-1));
        //do 3
        list.updateScore("B", BigDecimal.valueOf(10), BigDecimal.valueOf(-1));
        //then 3
        assertEquals(list.length, 2L);
        assertEquals(list.getRank("B", BigDecimal.valueOf(-1)), 1L);
        assertEquals(list.getRank("A", BigDecimal.valueOf(1)), 2L);
        node = list.getByIndex(0);
        assertEquals(node.ele, "B");
        assertEquals(node.score, BigDecimal.valueOf(-1));
        node = list.getByIndex(1);
        assertEquals(node.ele, "A");
        assertEquals(node.score, BigDecimal.valueOf(1));
    }

    @Test
    public void shouldAll() {

        ZSet.ZSkipList<BigDecimal, String> list = ZSet.ZSkipList.newInstance();

        for (int i = 0; i < 1500; i++) {
            BigDecimal v = BigDecimal.valueOf(Math.random() * 100);
            BigDecimal v2 = BigDecimal.valueOf(Math.random() * 100);
            BigDecimal v3 = BigDecimal.valueOf(Math.random() * 100);

            list.insert(v, "a");
            list.insert(BigDecimal.valueOf(Math.random() * 100), "a1");
            list.insert(v2, "a2");
            list.insert(BigDecimal.valueOf(Math.random() * 100), "a3");
            list.insert(BigDecimal.valueOf(44L), "d");
            list.insert(BigDecimal.valueOf(45L), "d");
            list.insert(BigDecimal.valueOf(46L), "d");
            list.delete(v, "a");
            list.updateScore("a2", v2, v3);
            assertTrue(list.getRank("a2", v3) > 0);
        }
        assertEquals(list.length, 1500 * 6);
    }
}
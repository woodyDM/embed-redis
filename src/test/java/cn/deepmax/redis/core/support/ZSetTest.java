package cn.deepmax.redis.core.support;

import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author wudi
 * @date 2021/12/20
 */
public class ZSetTest {


    @Test
    public void shouldInsert() {
        ZSet.ZSkipList<String> list = ZSet.ZSkipList.newInstance();
        //do 1
        list.insert("A", BigDecimal.valueOf(5));
        list.insert("B", BigDecimal.valueOf(10));
        list.insert("A", BigDecimal.valueOf(1));
        //then 1
        assertEquals(list.length, 3L);
        assertEquals(list.getRank("A", BigDecimal.valueOf(1)), 1L);
        assertEquals(list.getRank("A", BigDecimal.valueOf(5)), 2L);
        assertEquals(list.getRank("B", BigDecimal.valueOf(10)), 3L);
        assertEquals(list.getRank("B", BigDecimal.valueOf(11)), 0L);
        //do 2
        list.delete("A", BigDecimal.valueOf(5));
        //then 2
        assertEquals(list.length, 2L);
        assertEquals(list.getRank("A", BigDecimal.valueOf(1)), 1L);
        assertEquals(list.getRank("B", BigDecimal.valueOf(10)), 2L);
        //do 3
        list.updateScore("B", BigDecimal.valueOf(10), BigDecimal.valueOf(-1));
        //then 3
        assertEquals(list.length, 2L);
        assertEquals(list.getRank("B", BigDecimal.valueOf(-1)), 1L);
        assertEquals(list.getRank("A", BigDecimal.valueOf(1)), 2L);
    }

    @Test
    public void shouldAll() {

        ZSet.ZSkipList<String> list = ZSet.ZSkipList.newInstance();

        for (int i = 0; i < 1500; i++) {
            BigDecimal v = BigDecimal.valueOf(Math.random() * 100);
            BigDecimal v2 = BigDecimal.valueOf(Math.random() * 100);
            BigDecimal v3 = BigDecimal.valueOf(Math.random() * 100);
            
            list.insert("a", v);
            list.insert("a1", BigDecimal.valueOf(Math.random() * 100));
            list.insert("a2", v2);
            list.insert("a3", BigDecimal.valueOf(Math.random() * 100));
            list.insert("d", BigDecimal.valueOf(44L));
            list.insert("d", BigDecimal.valueOf(45L));
            list.insert("d", BigDecimal.valueOf(46L));
            list.delete("a", v);
            list.updateScore("a2", v2, v3);
            assertTrue(list.getRank("a2", v3) > 0);
        }
        assertEquals(list.length, 1500 * 6);
    }
}
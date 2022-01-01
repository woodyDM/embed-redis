package cn.deepmax.redis.core;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class KeyTest {
    @Test
    public void shouldNormal() {
        Key k1 = new Key(new byte[]{1, 2, 3});
        Key k2 = new Key(new byte[]{1, 2, 3});
        assertEquals(k1, k2);
        assertEquals(k1.compareTo(k2), 0);
    }


    @Test
    public void shouldNormal1() {
        Key k1 = new Key(new byte[]{1, 2, 3});
        Key k2 = new Key(new byte[]{1, 2, 3, 4});
        assertNotEquals(k1, k2);
        assertEquals(k1.compareTo(k2), -1);
    }

    @Test
    public void shouldNormal2() {
        Key k1 = new Key(new byte[]{1, 2, 3, 4});
        Key k2 = new Key(new byte[]{1, 2, 3});
        assertNotEquals(k1, k2);
        assertEquals(k1.compareTo(k2), 1);
    }

    @Test
    public void shouldNormal3() {
        Key k1 = new Key(new byte[]{1});
        Key k2 = new Key(new byte[]{0});
        assertNotEquals(k1, k2);
        assertEquals(k1.compareTo(k2), 1);
    }

    @Test
    public void shouldNormal4() {
        Key k1 = new Key(new byte[]{});
        Key k2 = new Key(new byte[]{1});
        assertNotEquals(k1, k2);
        assertEquals(k1.compareTo(k2), -1);
    }

    @Test
    public void shouldNormal5() {
        Key k1 = new Key(new byte[]{});
        Key k2 = new Key(new byte[]{});
        assertEquals(k1, k2);
        assertEquals(k1.compareTo(k2), 0);
    }

    @Test
    public void shouldNormal6() {
        Key k1 = new Key(new byte[]{2, 2, 3});
        Key k2 = new Key(new byte[]{1, 2, 3});
        assertNotEquals(k1, k2);
        assertEquals(k1.compareTo(k2), 1);
    }

    @Test
    public void shouldNormal7() {
        Key k1 = new Key(new byte[]{0, 2, 3});
        Key k2 = new Key(new byte[]{1, 2, 3});
        assertNotEquals(k1, k2);
        assertEquals(k1.compareTo(k2), -1);
    }

    @Test
    public void shouldInf0() {
        Key k2 = new Key(new byte[]{1, 2, 3});
        Key k1 = Key.INF;
        assertNotEquals(k1, k2);
        assertEquals(k1.compareTo(k2), 1);
    }

    @Test
    public void shouldInf2() {
        Key k2 = new Key(new byte[]{1, 2, 3});
        Key k1 = Key.NEG_INF;
        assertNotEquals(k1, k2);
        assertEquals(k1.compareTo(k2), -1);
    }

    @Test
    public void shouldInf3() {
        Key k1 = Key.NEG_INF;
        Key k2 = new Key(new byte[]{});
        assertNotEquals(k1, k2);
        assertEquals(k1.compareTo(k2), -1);
    }

    @Test
    public void shouldInf4() {
        Key k1 = Key.INF;
        Key k2 = new Key(new byte[]{});
        assertNotEquals(k1, k2);
        assertEquals(k1.compareTo(k2), 1);
    }
}
package cn.deepmax.redis.core.module;

import cn.deepmax.redis.support.MockTimeProvider;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

/**
 * @author wudi
 * @date 2021/12/22
 */
public class RStringTest {

    @Test
    public void shouldSetBit() {

        RString s = new RString(new MockTimeProvider());

        int bit = s.getBit(1000000);
        assertEquals(bit, 0);
        assertEquals(s.bitCount(0, 10), 0L);

        int i = s.setBit(10, 1);
        assertEquals(i, 0);
        assertEquals(s.bitCount(0, 10), 1L);
        assertEquals(s.bitCount(0, 0), 0L);
        assertEquals(s.bitCount(1, 1), 1L);

        i = s.getBit(10);
        assertEquals(i, 1);

        i = s.setBit(10, 1);
        assertEquals(i, 1);

        i = s.setBit(10, 0);
        assertEquals(i, 1);

        i = s.getBit(10);
        assertEquals(i, 0);

        i = s.setBit(10, 0);
        assertEquals(i, 0);

        i = s.getBit(10);
        assertEquals(i, 0);

        i = s.setBit(0, 1);
        assertEquals(i, 0);

        i = s.setBit(0, 1);
        assertEquals(i, 1);

    }

    @Test
    public void shouldBitCount() {
        RString s = new RString(new MockTimeProvider());
        s.setBit(0, 1);
        s.setBit(7, 1);  //pos 0
        s.setBit(8, 1);
        s.setBit(14, 1); //pos 1
        s.setBit(19, 1);                            //pos 2

        assertEquals(s.bitCount(0, 0), 2L);
        assertEquals(s.bitCount(0, 1), 4L);
        assertEquals(s.bitCount(0, 2), 5L);
        assertEquals(s.bitCount(1, 0), 0L);
        assertEquals(s.bitCount(1, 1), 2L);
        assertEquals(s.bitCount(1, 2), 3L);
        assertEquals(s.bitCount(2, 3), 1L);
        assertEquals(s.bitCount(0, -1), 5L);
        assertEquals(s.bitCount(0, -2), 4L);
        assertEquals(s.bitCount(0, -3), 2L);
    }

    @Test
    public void shouldBitOpAnd() {

        RString s0 = new RString(new MockTimeProvider());
        s0.setBit(0, 1);
        s0.setBit(8, 1);
        s0.setBit(19, 1);
        s0.setBit(17, 1);

        RString s1 = new RString(new MockTimeProvider());
        s1.setBit(0, 1);
        s1.setBit(8, 1);
        s1.setBit(19, 1);
        s1.setBit(6, 1);
        s1.setBit(15, 1);

        RString s2 = new RString(new MockTimeProvider());
        s2.setBit(0, 1);
        s2.setBit(8, 1);
        s2.setBit(6, 1);
        s2.setBit(19, 1);
        s2.setBit(14, 1);
        s2.setBit(144, 1);
        s2.setBit(145, 1);
        s2.setBit(56, 1);


        RString r = RString.bitOpAnd(Arrays.asList(s0, s1, s2));

        assertThat(r.length(), is(s2.length()));
        assertThat(r.getBit(0), is(1));
        assertThat(r.getBit(8), is(1));
        assertThat(r.getBit(19), is(1));
        assertThat(r.getBit(6), is(0));
        assertThat(r.getBit(15), is(0));
        assertThat(r.getBit(7), is(0));
        assertThat(r.getBit(17), is(0));
        assertThat(r.getBit(14), is(0));
        assertThat(r.getBit(144), is(0));
        assertThat(r.getBit(145), is(0));
        assertThat(r.getBit(56), is(0));
        assertThat(r.bitCount(0, -1), is(3L));

    }

    @Test
    public void shouldBitOpAndWithNullData() {

        RString s0 = new RString(new MockTimeProvider());
        s0.setBit(0, 1);
        s0.setBit(8, 1);
        s0.setBit(19, 1);
        s0.setBit(17, 1);

        RString s1 = new RString(new MockTimeProvider());
        s1.setBit(0, 1);
        s1.setBit(8, 1);
        s1.setBit(19, 1);
        s1.setBit(6, 1);
        s1.setBit(15, 1);

        RString s2 = new RString(new MockTimeProvider());
        s2.setBit(0, 1);
        s2.setBit(8, 1);
        s2.setBit(6, 1);
        s2.setBit(19, 1);
        s2.setBit(14, 1);
        s2.setBit(144, 1);
        s2.setBit(145, 1);
        s2.setBit(56, 1);


        RString r = RString.bitOpAnd(Arrays.asList(s0, s1, s2, null));

        assertThat(r.length(), is(s2.length()));
        assertThat(r.bitCount(0, -1), is(0L));

    }

    @Test
    public void shouldBitOpOr() {
        RString s0 = new RString(new MockTimeProvider());
        s0.setBit(0, 1);
        s0.setBit(8, 1);
        s0.setBit(19, 1);
        s0.setBit(17, 1);

        RString s1 = new RString(new MockTimeProvider());
        s1.setBit(0, 1);
        s1.setBit(8, 1);
        s1.setBit(19, 1);
        s1.setBit(6, 1);
        s1.setBit(15, 1);

        RString s2 = new RString(new MockTimeProvider());
        s2.setBit(0, 1);
        s2.setBit(8, 1);
        s2.setBit(19, 1);
        s2.setBit(6, 1);
        s2.setBit(14, 1);
        s2.setBit(56, 1);
        s2.setBit(144, 1);
        s2.setBit(145, 1);

        RString r = RString.bitOpOr(Arrays.asList(s0, s1, s2));

        assertThat(r.length(), is(s2.length()));
        assertThat(r.getBit(0), is(1));
        assertThat(r.getBit(8), is(1));
        assertThat(r.getBit(19), is(1));
        assertThat(r.getBit(17), is(1));
        assertThat(r.getBit(6), is(1));
        assertThat(r.getBit(15), is(1));
        assertThat(r.getBit(14), is(1));
        assertThat(r.getBit(56), is(1));
        assertThat(r.getBit(144), is(1));
        assertThat(r.getBit(145), is(1));
        assertThat(r.bitCount(0, -1), is(10L));

        assertThat(r.getBit(7), is(0));

    }

    @Test
    public void shouldBitOpXor() {
        RString s0 = new RString(new MockTimeProvider());
        s0.setBit(0, 1);
        s0.setBit(8, 1);
        s0.setBit(17, 1);
        s0.setBit(19, 1);

        RString s1 = new RString(new MockTimeProvider());
        s1.setBit(0, 1);
        s1.setBit(6, 1);
        s1.setBit(8, 1);
        s1.setBit(15, 1);
        s1.setBit(19, 1);

        RString s2 = new RString(new MockTimeProvider());
        s2.setBit(0, 1);
        s2.setBit(6, 1);
        s2.setBit(8, 1);
        s2.setBit(17, 1);
        s2.setBit(19, 1);
        s2.setBit(56, 1);
        s2.setBit(144, 1);
        s2.setBit(145, 1);

        RString r = RString.bitOpXor(Arrays.asList(s0, s1, s2));

        assertThat(r.length(), is(s2.length()));
        assertThat(r.getBit(0), is(1));
        assertThat(r.getBit(6), is(0));
        assertThat(r.getBit(8), is(1));
        assertThat(r.getBit(15), is(1));
        assertThat(r.getBit(17), is(0));
        assertThat(r.getBit(19), is(1));
        assertThat(r.getBit(56), is(1));
        assertThat(r.getBit(144), is(1));
        assertThat(r.getBit(145), is(1));
        assertThat(r.bitCount(0, -1), is(7L));

        assertThat(r.getBit(7), is(0));
        assertThat(r.getBit(77), is(0));
    }

    @Test
    public void shouldBitOpNot() {
        RString s = new RString(new MockTimeProvider());
        s.setBit(0, 1);
        s.setBit(6, 1);
        s.setBit(8, 1);
        s.setBit(15, 1);
        s.setBit(19, 1);

        RString r = RString.bitOpNot(s);

        assertThat(r.length(), is(s.length()));
        assertThat(r.getBit(0), is(0));
        assertThat(r.getBit(6), is(0));
        assertThat(r.getBit(8), is(0));
        assertThat(r.getBit(15), is(0));
        assertThat(r.getBit(19), is(0));

        assertThat(r.getBit(1), is(1));
        assertThat(r.getBit(2), is(1));
        assertThat(r.getBit(3), is(1));
        assertThat(r.getBit(4), is(1));
        assertThat(r.getBit(16), is(1));
        assertThat(r.bitCount(0, -1), is(24L - 5L));
    }

    @Test
    public void shouldBitOpAndNull() {
        RString r = RString.bitOpAnd(Arrays.asList(null, null));

        assertNull(r);
    }

    @Test
    public void shouldBitOpAndOne() {
        RString s = new RString(new MockTimeProvider());
        s.setBit(0, 1);
        s.setBit(8, 1);
        s.setBit(19, 1);
        s.setBit(6, 1);
        s.setBit(15, 1);

        RString r = RString.bitOpAnd(Arrays.asList(s));

        assertThat(r.length(), is(s.length()));
        assertThat(r.getBit(0), is(1));
        assertThat(r.getBit(8), is(1));
        assertThat(r.getBit(19), is(1));
        assertThat(r.getBit(6), is(1));
        assertThat(r.getBit(15), is(1));
        assertThat(r.bitCount(0, -1), is(5L));
    }

    @Test
    public void shouldBitOpOrOne() {
        RString s = new RString(new MockTimeProvider());
        s.setBit(0, 1);
        s.setBit(8, 1);
        s.setBit(19, 1);
        s.setBit(6, 1);
        s.setBit(15, 1);

        RString r = RString.bitOpOr(Arrays.asList(s));

        assertThat(r.length(), is(s.length()));
        assertThat(r.getBit(0), is(1));
        assertThat(r.getBit(8), is(1));
        assertThat(r.getBit(19), is(1));
        assertThat(r.getBit(6), is(1));
        assertThat(r.getBit(15), is(1));
        assertThat(r.bitCount(0, -1), is(5L));
    }

    @Test
    public void shouldBitOpXorOne() {
        RString s = new RString(new MockTimeProvider());
        s.setBit(0, 1);
        s.setBit(8, 1);
        s.setBit(19, 1);
        s.setBit(6, 1);
        s.setBit(15, 1);

        RString r = RString.bitOpXor(Arrays.asList(s));

        assertThat(r.length(), is(s.length()));
        assertThat(r.getBit(0), is(1));
        assertThat(r.getBit(8), is(1));
        assertThat(r.getBit(19), is(1));
        assertThat(r.getBit(6), is(1));
        assertThat(r.getBit(15), is(1));
        assertThat(r.bitCount(0, -1), is(5L));
    }

    @Test
    public void shouldGetRange() {
        byte[] data = {1, 2, 3, 4};
        RString s = new RString(new MockTimeProvider(), data);

        byte[] range = s.getRange(0, 3);
        assertTrue(Arrays.equals(range, data));
    }

    @Test
    public void shouldGetRange2() {
        byte[] data = {1, 2, 3, 4};
        RString s = new RString(new MockTimeProvider(), data);

        byte[] range = s.getRange(0, 2);
        assertTrue(Arrays.equals(range, new byte[]{1, 2, 3}));
    }


    @Test
    public void shouldGetRange3() {
        byte[] data = {1, 2, 3, 4};
        RString s = new RString(new MockTimeProvider(), data);

        byte[] range = s.getRange(3, 10);
        assertTrue(Arrays.equals(range, new byte[]{4}));
    }

    @Test
    public void shouldGetRange4() {
        byte[] data = {1, 2, 3, 4};
        RString s = new RString(new MockTimeProvider(), data);

        byte[] range = s.getRange(4, 10);
        assertTrue(Arrays.equals(range, new byte[]{}));
    }

    @Test
    public void shouldSetRange() {
        byte[] data = {1, 2, 3, 4};
        RString s = new RString(new MockTimeProvider(), data);

        s.setRange(new byte[]{2, 2, 3, 5}, 0);
        assertTrue(Arrays.equals(s.getS(), new byte[]{2, 2, 3, 5}));
    }

    @Test
    public void shouldSetRange2() {
        byte[] data = {1, 2, 3, 4};
        RString s = new RString(new MockTimeProvider(), data);

        s.setRange(new byte[]{}, 0);
        assertTrue(Arrays.equals(s.getS(), new byte[]{1, 2, 3, 4}));
    }

    @Test
    public void shouldSetRange3() {
        byte[] data = {1, 2, 3, 4};
        RString s = new RString(new MockTimeProvider(), data);

        s.setRange(new byte[]{1, 2, 3}, 3);
        assertTrue(Arrays.equals(s.getS(), new byte[]{1, 2, 3, 1, 2, 3}));
    }

    @Test
    public void shouldSetRange4() {
        byte[] data = {1, 2, 3, 4};
        RString s = new RString(new MockTimeProvider(), data);

        s.setRange(new byte[]{1, 2, 3}, 5);
        assertTrue(Arrays.equals(s.getS(), new byte[]{1, 2, 3, 4, 0, 1, 2, 3}));
    }

    @Test
    public void shouldSetRange5() {
        byte[] data = {1, 2, 3, 4};
        RString s = new RString(new MockTimeProvider(), data);

        s.setRange(new byte[]{5, 6, 7, 8}, 3);
        assertTrue(Arrays.equals(s.getS(), new byte[]{1, 2, 3, 5, 6, 7, 8}));
    }

    @Test
    public void shouldBitPos() {
        byte[] data = new byte[]{(byte) 0xff,(byte) 0xf0,(byte) 0x00,};
        RString s = new RString(new MockTimeProvider(), data);

        assertEquals(s.bitPos(0, -1, 0, false), 12);
    }

    @Test
    public void shouldBitPos2() {
        byte[] data = new byte[]{(byte) 0B10111111,(byte) 0xf0,(byte) 0x00,};
        RString s = new RString(new MockTimeProvider(), data);

        assertEquals(s.bitPos(0, -1, 0, false), 1);
    }

    @Test
    public void shouldBitPos3() {
        byte[] data = new byte[]{(byte) 0B11111111,(byte) 0B10110101,(byte) 0x00,};
        RString s = new RString(new MockTimeProvider(), data);

        assertEquals(s.bitPos(0, -1, 0, false), 9);
    }

    @Test
    public void shouldBitPos4() {
        byte[] data = new byte[]{(byte) 0x00,(byte) 0B00110000,(byte) 0x00,};
        RString s = new RString(new MockTimeProvider(), data);

        assertEquals(s.bitPos(0, -1, 1, false), 10);
    }

    @Test
    public void shouldBitPos5() {
        byte[] data = new byte[]{(byte) 0xff,(byte) 0B00110000,(byte) 0x00,};
        RString s = new RString(new MockTimeProvider(), data);

        assertEquals(s.bitPos(1, -1, 1, false), 10);
    }

    @Test
    public void shouldBitPos6() {
        byte[] data = new byte[]{(byte) 0x00,(byte) 0B11100000,(byte) 0x00,};
        RString s = new RString(new MockTimeProvider(), data);

        assertEquals(s.bitPos(1, -1, 0, false), 11);
    }
    
    @Test
    public void shouldBitPos7() {
        byte[] data = new byte[]{(byte) 0x00,(byte) 0x00,(byte) 0x00,};
        RString s = new RString(new MockTimeProvider(), data);

        assertEquals(s.bitPos(1, -1, 0, true), 8);
    }

    @Test
    public void shouldBitPos8() {
        byte[] data = new byte[]{(byte) 0x00,(byte) 0x00,(byte) 0x00,};
        RString s = new RString(new MockTimeProvider(), data);

        assertEquals(s.bitPos(1, -1, 1, true), -1);
    }

    @Test
    public void shouldBitPos9() {
        byte[] data = new byte[]{(byte) 0x00,(byte) 0x00,(byte) 0x00,};
        RString s = new RString(new MockTimeProvider(), data);

        assertEquals(s.bitPos(0, -1, 1, false), -1);
    }

    @Test
    public void shouldBitPos10() {
        byte[] data = new byte[]{(byte) 0xff,(byte) 0xff,(byte) 0xff,};
        RString s = new RString(new MockTimeProvider(), data);

        assertEquals(s.bitPos(0, -1, 0, false), 24);
    }

    @Test
    public void shouldBitPos11() {
        byte[] data = new byte[]{(byte) 0x00,(byte) 0x00,(byte) 0xff,};
        RString s = new RString(new MockTimeProvider(), data);

        assertEquals(s.bitPos(0, 1, 1, true), -1);
    }
}
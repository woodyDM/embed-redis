package cn.deepmax.redis.core.module;

import cn.deepmax.redis.core.Key;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

/**
 * @author wudi
 */
public class NavMapTest {

    @Test
    public void shouldAddFirst() {
        ScanMap<Key, String> map = new ScanMap<>();

        map.set(k("a"), "1");
        map.set(k("b"), "2");
        map.set(k("c"), "3");

        assertEquals(map.size(), 3);
        assertEquals(map.get(k("a")), "1");
        assertEquals(map.get(k("b")), "2");
        assertEquals(map.get(k("c")), "3");
        assertEquals(map.get(1L).value, "1");
        assertEquals(map.get(2L).value, "2");
        assertEquals(map.get(3L).value, "3");
        assertNull(map.get(1L).pre);
        assertSame(map.get(1L).next, map.get(2L));
        assertSame(map.get(2L).pre, map.get(1L));
        assertSame(map.get(2L).next, map.get(3L));
        assertSame(map.get(3L).pre, map.get(2L));
        assertNull(map.get(3L).next);
        assertSame(map.get(3L), map.tail);
        assertSame(map.get(1L), map.head);

    }

    @Test
    public void shouldAddAndRemove() {
        ScanMap<Key, String> map = new ScanMap<>();


        map.set(k("a"), "1");
        map.set(k("b"), "2");
        map.set(k("c"), "3");

        map.delete(k("a"));

        assertEquals(map.size(), 2);
        assertNull(map.get(k("a")));
        assertEquals(map.get(k("b")), "2");
        assertEquals(map.get(k("c")), "3");
        assertNull(map.get(1L));
        assertEquals(map.get(2L).value, "2");
        assertEquals(map.get(3L).value, "3");
        assertNull(map.get(2L).pre);
        assertSame(map.get(2L).next, map.get(3L));
        assertSame(map.get(3L).pre, map.get(2L));
        assertNull(map.get(3L).next);
        assertSame(map.get(3L), map.tail);
        assertSame(map.get(2L), map.head);

    }

    @Test
    public void shouldAddAndRemoveMid() {
        ScanMap<Key, String> map = new ScanMap<>();


        map.set(k("a"), "1");
        map.set(k("b"), "2");
        map.set(k("c"), "3");
        map.delete(k("b"));

        assertEquals(map.size(), 2);
        assertEquals(map.get(k("a")), "1");
        assertNull(map.get(k("b")));
        assertEquals(map.get(k("c")), "3");
        assertEquals(map.get(1L).value, "1");
        assertNull(map.get(2L));
        assertEquals(map.get(3L).value, "3");

        assertNull(map.get(1L).pre);
        assertSame(map.get(1L).next, map.get(3L));
        assertSame(map.get(3L).pre, map.get(1L));
        assertNull(map.get(3L).next);
        assertSame(map.get(3L), map.tail);
        assertSame(map.get(1L), map.head);

    }

    @Test
    public void shouldAddAndRemoveLast() {
        ScanMap<Key, String> map = new ScanMap<>();


        map.set(k("a"), "1");
        map.set(k("b"), "2");
        map.set(k("c"), "3");
        map.delete(k("c"));

        assertEquals(map.size(), 2);
        assertEquals(map.get(k("a")), "1");
        assertEquals(map.get(k("b")), "2");
        assertNull(map.get(k("c")));
        assertEquals(map.get(1L).value, "1");
        assertEquals(map.get(2L).value, "2");
        assertNull(map.get(3L));

        assertNull(map.get(1L).pre);
        assertSame(map.get(1L).next, map.get(2L));
        assertSame(map.get(2L).pre, map.get(1L));
        assertSame(map.get(2L), map.tail);
        assertSame(map.get(1L), map.head);

    }

    @Test
    public void shouldAddAndSetTail() {
        ScanMap<Key, String> map = new ScanMap<>();

        map.set(k("a"), "1");
        map.set(k("b"), "2");
        map.set(k("c"), "3");
        map.set(k("c"), "33");

        assertEquals(map.size(), 3);
        assertEquals(map.get(k("a")), "1");
        assertEquals(map.get(k("b")), "2");
        assertEquals(map.get(k("c")), "33");
         
        assertEquals(map.get(1L).value, "1");
        assertEquals(map.get(2L).value, "2");
        assertNull(map.get(3L));
        assertEquals(map.get(4L).value, "33");


        assertNull(map.get(1L).pre);
        assertSame(map.get(1L).next, map.get(2L));
        assertSame(map.get(2L).pre, map.get(1L));
        assertSame(map.get(2L).next, map.get(4L));
        assertSame(map.get(4L).pre, map.get(2L));
        assertNull(map.get(3L));
        assertSame(map.get(4L), map.tail);
        assertSame(map.get(1L), map.head);

    }

    @Test
    public void shouldAddAndSetHead() {
        ScanMap<Key, String> map = new ScanMap<>();

        map.set(k("a"), "1");
        map.set(k("b"), "2");
        map.set(k("c"), "3");
        map.set(k("a"), "11");

        assertEquals(map.size(), 3);
        assertEquals(map.get(k("a")), "11");
        assertEquals(map.get(k("b")), "2");
        assertEquals(map.get(k("c")), "3");

        assertNull(map.get(1L));
        assertEquals(map.get(2L).value, "2");
        assertEquals(map.get(3L).value, "3");
        assertEquals(map.get(4L).value, "11");


        assertNull(map.get(2L).pre);
        assertSame(map.get(2L).next, map.get(3L));
        assertSame(map.get(3L).pre, map.get(2L));
        assertSame(map.get(3L).next, map.get(4L));
        assertSame(map.get(4L).pre, map.get(3L));

        assertSame(map.get(2L), map.head);

    }

    @Test
    public void shouldAddAndSetHead2() {
        ScanMap<Key, String> map = new ScanMap<>();

        map.set(k("a"), "1");
        map.set(k("a"), "11");

        assertEquals(map.size(), 1);
        assertEquals(map.get(k("a")), "11");


        assertNull(map.get(1L));
        assertEquals(map.get(2L).value, "11");

        assertNull(map.head.next);
        assertNull(map.head.pre);
        assertNull(map.tail.pre);
        assertNull(map.tail.next);
        assertSame(map.get(2L), map.head);
        assertSame(map.get(2L), map.tail);

    }

    @Test
    public void shouldAddAndSetMid() {
        ScanMap<Key, String> map = new ScanMap<>();

        map.set(k("a"), "1");
        map.set(k("b"), "2");
        map.set(k("c"), "3");
        map.set(k("b"), "22");

        assertEquals(map.size(), 3);
        assertEquals(map.get(k("a")), "1");
        assertEquals(map.get(k("b")), "22");
        assertEquals(map.get(k("c")), "3");

        assertEquals(map.get(1L).value, "1");
        assertNull(map.get(2L));
        assertEquals(map.get(3L).value, "3");
        assertEquals(map.get(4L).value, "22");

        assertNull(map.get(1L).pre);
        assertSame(map.get(1L).next, map.get(3L));
        assertSame(map.get(3L).pre, map.get(1L));
        assertSame(map.get(3L).next, map.get(4L));
        assertSame(map.get(4L).pre, map.get(3L));

        assertSame(map.get(4L), map.tail);
        assertSame(map.get(1L), map.head);

    }

    private Key k(String s) {
        return new Key(s.getBytes(StandardCharsets.UTF_8));
    }
}
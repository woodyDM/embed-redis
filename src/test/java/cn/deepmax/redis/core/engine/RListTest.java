package cn.deepmax.redis.core.engine;

import cn.deepmax.redis.base.BaseMemEngineTest;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.module.RList;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RListTest extends BaseMemEngineTest {

    @Test
    public void shouldLPos() {
        RList list = new RList(engine().timeProvider());
        list.rpush(new Key(bytes("a")));
        list.rpush(new Key(bytes("b")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("1")));
        list.rpush(new Key(bytes("2")));
        list.rpush(new Key(bytes("3")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("c")));

        List<Integer> r = list.lpos(bytes("c"), Optional.empty(), Optional.empty(), Optional.empty());
        assertEquals(r.size(), 1);
        assertEquals(r.get(0).intValue(), 2);

        r = list.lpos(bytes("3"), Optional.empty(), Optional.empty(), Optional.empty());
        assertEquals(r.size(), 1);
        assertEquals(r.get(0).intValue(), 5);

        r = list.lpos(bytes("x"), Optional.empty(), Optional.empty(), Optional.empty());
        assertEquals(r.size(), 0);
    }

    @Test
    public void shouldLPosCount() {
        RList list = new RList(engine().timeProvider());
        list.rpush(new Key(bytes("a")));
        list.rpush(new Key(bytes("b")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("1")));
        list.rpush(new Key(bytes("2")));
        list.rpush(new Key(bytes("3")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("c")));

        List<Integer> r = list.lpos(bytes("c"), Optional.empty(), Optional.of(1L), Optional.empty());
        assertEquals(r.size(), 1);
        assertEquals(r.get(0).intValue(), 2);

        r = list.lpos(bytes("c"), Optional.empty(), Optional.of(2L), Optional.empty());
        assertEquals(r.size(), 2);
        assertEquals(r.get(0).intValue(), 2);
        assertEquals(r.get(1).intValue(), 6);

        r = list.lpos(bytes("c"), Optional.empty(), Optional.of(3L), Optional.empty());
        assertEquals(r.size(), 3);
        assertEquals(r.get(0).intValue(), 2);
        assertEquals(r.get(1).intValue(), 6);
        assertEquals(r.get(2).intValue(), 7);

        r = list.lpos(bytes("c"), Optional.empty(), Optional.of(0L), Optional.empty());
        assertEquals(r.size(), 3);
        assertEquals(r.get(0).intValue(), 2);
        assertEquals(r.get(1).intValue(), 6);
        assertEquals(r.get(2).intValue(), 7);

        r = list.lpos(bytes("c"), Optional.empty(), Optional.of(4L), Optional.empty());
        assertEquals(r.size(), 3);
        assertEquals(r.get(0).intValue(), 2);
        assertEquals(r.get(1).intValue(), 6);
        assertEquals(r.get(2).intValue(), 7);

        r = list.lpos(bytes("x"), Optional.empty(), Optional.of(4L), Optional.empty());
        assertEquals(r.size(), 0);
    }

    @Test
    public void shouldLPosMaxlen() {
        RList list = new RList(engine().timeProvider());
        list.rpush(new Key(bytes("a")));
        list.rpush(new Key(bytes("b")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("1")));
        list.rpush(new Key(bytes("2")));
        list.rpush(new Key(bytes("3")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("c")));

        List<Integer> r = list.lpos(bytes("c"), Optional.empty(), Optional.empty(), Optional.of(1L));
        assertEquals(r.size(), 0);

        r = list.lpos(bytes("c"), Optional.empty(), Optional.empty(), Optional.of(3L));
        assertEquals(r.size(), 1);
        assertEquals(r.get(0).intValue(), 2);

        r = list.lpos(bytes("c"), Optional.empty(), Optional.empty(), Optional.of(6L));
        assertEquals(r.size(), 1);
        assertEquals(r.get(0).intValue(), 2);

        r = list.lpos(bytes("c"), Optional.empty(), Optional.of(2L), Optional.of(7L));
        assertEquals(r.size(), 2);
        assertEquals(r.get(0).intValue(), 2);
        assertEquals(r.get(1).intValue(), 6);

        r = list.lpos(bytes("c"), Optional.empty(), Optional.of(3L), Optional.of(9L));
        assertEquals(r.size(), 3);
        assertEquals(r.get(0).intValue(), 2);
        assertEquals(r.get(1).intValue(), 6);
        assertEquals(r.get(2).intValue(), 7);

        r = list.lpos(bytes("c"), Optional.empty(), Optional.of(4L), Optional.of(0L));
        assertEquals(r.size(), 3);
        assertEquals(r.get(0).intValue(), 2);
        assertEquals(r.get(1).intValue(), 6);
        assertEquals(r.get(2).intValue(), 7);
    }

    @Test
    public void shouldLPosAllArg() {
        RList list = new RList(engine().timeProvider());
        list.rpush(new Key(bytes("a")));
        list.rpush(new Key(bytes("b")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("1")));
        list.rpush(new Key(bytes("2")));
        list.rpush(new Key(bytes("3")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("c")));

        List<Integer> r = list.lpos(bytes("c"), Optional.of(2L), Optional.of(2L), Optional.of(7L));
        assertEquals(r.size(), 1);
        assertEquals(r.get(0).intValue(), 6);

        r = list.lpos(bytes("c"), Optional.of(2L), Optional.of(2L), Optional.of(8L));
        assertEquals(r.size(), 2);
        assertEquals(r.get(0).intValue(), 6);
        assertEquals(r.get(1).intValue(), 7);

        r = list.lpos(bytes("c"), Optional.of(-1L), Optional.of(2L), Optional.empty());
        assertEquals(r.size(), 2);
        assertEquals(r.get(0).intValue(), 7);
        assertEquals(r.get(1).intValue(), 6);
    }

    @Test
    public void shouldLPosRank() {
        RList list = new RList(engine().timeProvider());
        list.rpush(new Key(bytes("a")));
        list.rpush(new Key(bytes("b")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("1")));
        list.rpush(new Key(bytes("2")));
        list.rpush(new Key(bytes("3")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("c")));

        List<Integer> r = list.lpos(bytes("c"), Optional.of(1L), Optional.empty(), Optional.empty());
        assertEquals(r.size(), 1);
        assertEquals(r.get(0).intValue(), 2);

        r = list.lpos(bytes("c"), Optional.of(2L), Optional.empty(), Optional.empty());
        assertEquals(r.size(), 1);
        assertEquals(r.get(0).intValue(), 6);

        r = list.lpos(bytes("c"), Optional.of(3L), Optional.empty(), Optional.empty());
        assertEquals(r.size(), 1);
        assertEquals(r.get(0).intValue(), 7);

        r = list.lpos(bytes("c"), Optional.of(4L), Optional.empty(), Optional.empty());
        assertEquals(r.size(), 0);

        r = list.lpos(bytes("c"), Optional.of(-1L), Optional.empty(), Optional.empty());
        assertEquals(r.size(), 1);
        assertEquals(r.get(0).intValue(), 7);

        r = list.lpos(bytes("c"), Optional.of(-2L), Optional.empty(), Optional.empty());
        assertEquals(r.size(), 1);
        assertEquals(r.get(0).intValue(), 6);

        r = list.lpos(bytes("c"), Optional.of(-3L), Optional.empty(), Optional.empty());
        assertEquals(r.size(), 1);
        assertEquals(r.get(0).intValue(), 2);

        r = list.lpos(bytes("c"), Optional.of(-4L), Optional.empty(), Optional.empty());
        assertEquals(r.size(), 0);

        r = list.lpos(bytes("x"), Optional.of(-1L), Optional.empty(), Optional.empty());
        assertEquals(r.size(), 0);
    }

    @Test
    public void shouldLRange() {
        RList list = new RList(engine().timeProvider());
        list.rpush(new Key(bytes("a")));
        list.rpush(new Key(bytes("b")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("1")));
        list.rpush(new Key(bytes("2")));
        list.rpush(new Key(bytes("3")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("d")));
        List<Key> range;

        range = list.lrange(0, 0);
        assertEquals(range.size(), 1);
        assertEquals(range.get(0).str(), "a");

        range = list.lrange(0, -1);
        assertEquals(range.size(), 8);
        assertEquals(range.get(0).str(), "a");
        assertEquals(range.get(7).str(), "d");

        range = list.lrange(2, 5);
        assertEquals(range.size(), 4);
        assertEquals(range.get(0).str(), "c");
        assertEquals(range.get(3).str(), "3");

        range = list.lrange(2, -3);
        assertEquals(range.size(), 4);
        assertEquals(range.get(0).str(), "c");
        assertEquals(range.get(3).str(), "3");


        range = list.lrange(-2, -1);
        assertEquals(range.size(), 2);
        assertEquals(range.get(0).str(), "c");
        assertEquals(range.get(1).str(), "d");

        range = list.lrange(50, 100);
        assertEquals(range.size(), 0);

    }

    @Test
    public void shouldInsert() {
        RList list = new RList(engine().timeProvider());
        list.rpush(new Key(bytes("a")));
        list.rpush(new Key(bytes("b")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("1")));
        list.rpush(new Key(bytes("2")));
        list.rpush(new Key(bytes("3")));

        int s = list.insert(bytes("b"), bytes("I"), 0);
        assertEquals(s, 7);
        List<Key> pos = list.lrange(1, 2);
        assertEquals(pos.get(0).str(), "I");
        assertEquals(pos.get(1).str(), "b");
    }

    @Test
    public void shouldInsertAfter() {
        RList list = new RList(engine().timeProvider());
        list.rpush(new Key(bytes("a")));
        list.rpush(new Key(bytes("b")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("1")));
        list.rpush(new Key(bytes("2")));
        list.rpush(new Key(bytes("3")));

        int s = list.insert(bytes("b"), bytes("I"), 1);
        assertEquals(s, 7);
        List<Key> pos = list.lrange(1, 3);
        assertEquals(pos.get(0).str(), "b");
        assertEquals(pos.get(1).str(), "I");
        assertEquals(pos.get(2).str(), "c");
    }

    @Test
    public void shouldNotInsert() {
        RList list = new RList(engine().timeProvider());
        list.rpush(new Key(bytes("a")));
        list.rpush(new Key(bytes("b")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("1")));
        list.rpush(new Key(bytes("2")));
        list.rpush(new Key(bytes("3")));

        int s = list.insert(bytes("X"), bytes("I"), 1);
        assertEquals(s, -1);
        assertEquals(list.size(), 6L);
    }


    @Test
    public void shouldIndex() {
        RList list = new RList(engine().timeProvider());
        list.rpush(new Key(bytes("a")));
        list.rpush(new Key(bytes("b")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("1")));
        list.rpush(new Key(bytes("2")));
        list.rpush(new Key(bytes("3")));
        Key s;
        s = list.valueAt(0);
        assertEquals(s.str(), "a");

        s = list.valueAt(1);
        assertEquals(s.str(), "b");

        s = list.valueAt(-1);
        assertEquals(s.str(), "3");

        s = list.valueAt(-2);
        assertEquals(s.str(), "2");

        s = list.valueAt(100);
        assertNull(s);

        s = list.valueAt(-6);
        assertEquals(s.str(), "a");

        s = list.valueAt(-7);
        assertNull(s);
    }

    @Test
    public void shouldLSet() {
        RList list = new RList(engine().timeProvider());
        list.rpush(new Key(bytes("a")));
        list.rpush(new Key(bytes("b")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("1")));
        list.rpush(new Key(bytes("2")));
        list.rpush(new Key(bytes("3")));

        assertEquals(list.lset(1, bytes("X")), 1);
        assertEquals(list.lpos(bytes("X")).get(0).longValue(), 1L);
    }

    @Test
    public void shouldLSetNull() {
        RList list = new RList(engine().timeProvider());
        list.rpush(new Key(bytes("a")));
        list.rpush(new Key(bytes("b")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("1")));
        list.rpush(new Key(bytes("2")));
        list.rpush(new Key(bytes("3")));

        assertEquals(list.lset(10, bytes("X")), -1);
        assertEquals(list.lpos(bytes("X")).size(), 0);
    }

    @Test
    public void shouldLRem() {
        RList list = new RList(engine().timeProvider());
        list.rpush(new Key(bytes("a")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("1")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("3")));

        int c = list.remove(bytes("c"), 0);
        assertEquals(c, 3);
        List<Key> lrange = list.lrange(0, -1);
        assertEquals(lrange.get(0).str(), "a");
        assertEquals(lrange.get(1).str(), "1");
        assertEquals(lrange.get(2).str(), "3");
    }

    @Test
    public void shouldLRemPositiveCount() {
        RList list = new RList(engine().timeProvider());
        list.rpush(new Key(bytes("a")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("1")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("3")));

        int c = list.remove(bytes("c"), 2);
        assertEquals(c, 2);
        List<Key> lrange = list.lrange(0, -1);
        assertEquals(lrange.get(0).str(), "a");
        assertEquals(lrange.get(1).str(), "1");
        assertEquals(lrange.get(2).str(), "c");
        assertEquals(lrange.get(3).str(), "3");
    }

    @Test
    public void shouldLRemNegCount() {
        RList list = new RList(engine().timeProvider());
        list.rpush(new Key(bytes("a")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("1")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("3")));

        int c = list.remove(bytes("c"), -2);
        assertEquals(c, 2);
        List<Key> lrange = list.lrange(0, -1);
        assertEquals(lrange.get(0).str(), "a");
        assertEquals(lrange.get(1).str(), "c");
        assertEquals(lrange.get(2).str(), "1");
        assertEquals(lrange.get(3).str(), "3");
    }

    @Test
    public void shouldLRemNegCount2() {
        RList list = new RList(engine().timeProvider());
        list.rpush(new Key(bytes("a")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("1")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("3")));

        int c = list.remove(bytes("c"), -5);
        assertEquals(c, 3);
        List<Key> lrange = list.lrange(0, -1);
        assertEquals(lrange.get(0).str(), "a");
        assertEquals(lrange.get(1).str(), "1");
        assertEquals(lrange.get(2).str(), "3");
    }

    @Test
    public void shouldLRemNegCount3() {
        RList list = new RList(engine().timeProvider());
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("c")));

        int c = list.remove(bytes("c"), -5);
        assertEquals(c, 3);
        List<Key> lrange = list.lrange(0, -1);
        assertEquals(lrange.size(), 0);
    }

    @Test
    public void shouldLRemNegCount4() {
        RList list = new RList(engine().timeProvider());
        list.rpush(new Key(bytes("a")));
        list.rpush(new Key(bytes("b")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("d")));

        int c = list.remove(bytes("D"), -5);
        assertEquals(c, 0);
    }

    @Test
    public void shouldLTrim1() {
        RList list = new RList(engine().timeProvider());
        list.rpush(new Key(bytes("a")));
        list.rpush(new Key(bytes("b")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("d")));

        list.trim(0, 3);
        List<Key> obj = list.lrange(0, -1);
        assertEquals(obj.size(), 4);
        assertEquals(obj.get(0).str(), "a");
        assertEquals(obj.get(1).str(), "b");
        assertEquals(obj.get(2).str(), "c");
        assertEquals(obj.get(3).str(), "d");
    }

    @Test
    public void shouldLTrim2() {
        RList list = new RList(engine().timeProvider());
        list.rpush(new Key(bytes("a")));
        list.rpush(new Key(bytes("b")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("d")));

        list.trim(0, -2);
        List<Key> obj = list.lrange(0, -1);
        assertEquals(obj.size(), 3);
        assertEquals(obj.get(0).str(), "a");
        assertEquals(obj.get(1).str(), "b");
        assertEquals(obj.get(2).str(), "c");
    }

    @Test
    public void shouldLTrim3() {
        RList list = new RList(engine().timeProvider());
        list.rpush(new Key(bytes("a")));
        list.rpush(new Key(bytes("b")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("d")));

        list.trim(2, 3);
        List<Key> obj = list.lrange(0, -1);
        assertEquals(obj.size(), 2);
        assertEquals(obj.get(0).str(), "c");
        assertEquals(obj.get(1).str(), "d");
    }

    @Test
    public void shouldLTrim4() {
        RList list = new RList(engine().timeProvider());
        list.rpush(new Key(bytes("a")));
        list.rpush(new Key(bytes("b")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("d")));

        list.trim(1, 30);
        List<Key> obj = list.lrange(0, -1);
        assertEquals(obj.size(), 3);
        assertEquals(obj.get(0).str(), "b");
        assertEquals(obj.get(1).str(), "c");
        assertEquals(obj.get(2).str(), "d");
    }

    @Test
    public void shouldLTrim5() {
        RList list = new RList(engine().timeProvider());
        list.rpush(new Key(bytes("a")));
        list.rpush(new Key(bytes("b")));
        list.rpush(new Key(bytes("c")));
        list.rpush(new Key(bytes("d")));

        list.trim(100, -1);
        List<Key> obj = list.lrange(0, -1);
        assertEquals(obj.size(), 0);
    }

}
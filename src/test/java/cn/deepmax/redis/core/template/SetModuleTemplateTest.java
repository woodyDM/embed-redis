package cn.deepmax.redis.core.template;

import cn.deepmax.redis.base.BasePureTemplateTest;
import org.junit.Test;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;

import java.util.*;

import static org.junit.Assert.*;

//key use simple bytes, but value use JDK bytes
public class SetModuleTemplateTest extends BasePureTemplateTest {
    public SetModuleTemplateTest(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    @Test
    public void shouldSAdd() {
        Long eff = s().add("kk", "v1");

        assertEquals(s().size("kk").intValue(), 1);
        assertEquals(eff.intValue(), 1);

        eff = s().add("kk", "v1");
        assertEquals(s().size("kk").intValue(), 1);
        assertEquals(eff.intValue(), 0);

        eff = s().add("kk", "v2");
        assertEquals(s().size("kk").intValue(), 2);
        assertEquals(eff.intValue(), 1);
    }

    @Test
    public void shouldSAdd2() {
        Long eff = t().execute((RedisCallback<Long>) con -> con.sAdd(bytes("kk"),
                serialize("vv1"), serialize("vv1"), serialize("v2👌"), serialize("haha")));

        assertEquals(s().size("kk").intValue(), 3);
        assertEquals(eff.intValue(), 3);
    }

    @Test
    public void shouldSIsMember() {
        s().add("kk", "v1");

        assertTrue(s().isMember("kk", "v1"));
        assertFalse(s().isMember("kk", "not-e"));
        assertFalse(s().isMember("k-not", "any"));
    }

    @Test
    public void shouldSMIsMember() {
        if (isRedisson()) {
            return;//StackOverflowError
        }

        s().add("kk", "v1");
        s().add("kk", "v2");
        s().add("kk", "你好");

        Map<Object, Boolean> m = s().isMember("kk", "v1", "NIHAO", "你好");
        assertTrue(m.get("v1"));
        assertTrue(m.get("你好"));
        assertFalse(m.get("NIHAO"));

        m = s().isMember("kk33", "v1", "NIHAO", "你好");
        assertFalse(m.get("v1"));
        assertFalse(m.get("你好"));
        assertFalse(m.get("NIHAO"));
    }

    @Test
    public void shouldSRandMember() {
        s().add("kk", "v1");
        s().add("kk", "v2");
        s().add("kk", "你好");

        for (int i = 0; i < 50; i++) {
            Object obj = s().randomMember("kk");
            assertTrue(Arrays.asList("v1", "v2", "你好").contains(obj));
        }
        //count -5
        List<Object> list = s().randomMembers("kk", 15);
        assertEquals(list.size(), 15);
        for (Object it : list) {
            assertTrue(Arrays.asList("v1", "v2", "你好").contains(it));
        }
        //count 5
        list = new ArrayList<>(s().distinctRandomMembers("kk", 5));
        assertEquals(list.size(), 3);
        for (Object it : list) {
            assertTrue(Arrays.asList("v1", "v2", "你好").contains(it));
        }
    }

    @Test
    public void shouldSRandMemberNil() {
        Object obj = s().randomMember("kk");
        assertNull(obj);
        //count -5
        List<Object> list = s().randomMembers("kk", 15);
        assertEquals(list.size(), 0);
        //count 5
        list = new ArrayList<>(s().distinctRandomMembers("kk", 5));
        assertEquals(list.size(), 0);
    }

    @Test
    public void shouldRemove() {
        s().add("kk", "v1");
        s().add("kk", "v2");
        s().add("kk", "你好");

        Long eff = s().remove("kk", "v1", "你好", "no-e");
        assertEquals(eff.intValue(), 2);
        assertEquals(s().size("kk").intValue(), 1);
    }

    @Test
    public void shouldRemoveNil() {
        Long eff = s().remove("kk", "v1", "你好", "no-e");
        assertEquals(eff.intValue(), 0);
    }

    @Test
    public void shouldSMembers() {
        s().add("kk", "v1");
        s().add("kk", "v2");
        s().add("kk", "你好");

        Set<Object> objs = s().members("kk");
        assertEquals(objs.size(), 3);
        assertTrue(objs.contains("v1"));
        assertTrue(objs.contains("v2"));
        assertTrue(objs.contains("你好"));

        assertEquals(s().members("k-not-e").size(), 0);
    }

    @Test
    public void shouldSPop() {
        s().add("kk", "v1");
        s().add("kk", "v2");
        s().add("kk", "你好");

        Object kk = s().pop("kk");
        List<String> s = new ArrayList<>();
        s.add("v1");
        s.add("v2");
        s.add("你好");
        assertTrue(s.contains(kk));

        s.remove(kk);
        kk = s().pop("kk");

        assertTrue(s.contains(kk));

        s.remove(kk);
        kk = s().pop("kk");

        assertTrue(s.contains(kk));
        assertEquals(s().size("kk").intValue(), 0);
        //can get as String
        assertNull(v().get("kk"));
    }

    @Test
    public void shouldSPopWithCount() {
        s().add("kk", "v1");
        s().add("kk", "v2");
        s().add("kk", "你好");
        List<String> s = new ArrayList<>();
        s.add("v1");
        s.add("v2");
        s.add("你好");

        List<Object> kk = s().pop("kk", 2);

        assertEquals(kk.size(), 2);
        assertTrue(s.contains(kk.get(0)));
        assertTrue(s.contains(kk.get(1)));

        s.removeAll(kk);
        kk = s().pop("kk", 2);

        assertEquals(kk.size(), 1);
        assertTrue(s.contains(kk.get(0)));

        assertEquals(s().size("kk").intValue(), 0);
        //can get as String
        assertNull(v().get("kk"));
    }

    @Test
    public void shouldSMoveToEmptyAndNormal() {
        s().add("kk", "v1");
        s().add("kk", "v2");
        s().add("kk", "你好");

        Boolean ok = s().move("kk", "你好", "dest");

        assertTrue(ok);
        assertEquals(s().size("dest").intValue(), 1);
        assertEquals(s().size("kk").intValue(), 2);

        ok = s().move("kk", "v1", "dest");
        assertTrue(ok);
        assertEquals(s().size("dest").intValue(), 2);
        assertEquals(s().size("kk").intValue(), 1);

        ok = s().move("kk", "v1-not-e", "dest");
        assertFalse(ok);
        assertEquals(s().size("dest").intValue(), 2);
        assertEquals(s().size("kk").intValue(), 1);
    }

    @Test
    public void shouldSMoveFromEmptyAndNormal() {
        s().add("dest", "v1");
        s().add("dest", "v2");
        s().add("dest", "你好");

        boolean ok = s().move("kk-not-e", "anyv", "dest");
        assertFalse(ok);
        assertEquals(s().size("dest").intValue(), 3);
        assertEquals(s().size("kk").intValue(), 0);
    }

    @Test
    public void shouldSMoveSameAndNormal() {
        s().add("kk", "v1");
        s().add("kk", "v2");

        s().add("dest", "v1");
        s().add("dest", "v2");
        s().add("dest", "你好");

        boolean ok = s().move("kk", "v1", "dest");
        assertTrue(ok);
        assertEquals(s().size("dest").intValue(), 3);
        assertEquals(s().size("kk").intValue(), 1);
    }


    @Test
    public void shouldSDiff() {
        s().add("kk", "v1");
        s().add("kk", "v2");
        s().add("kk", "v3");
        s().add("kk", "v4");
        s().add("kk", "你的");

        s().add("k2", "v1");
        s().add("k2", "v2");
        s().add("k2", "你好");

        s().add("k3", "v1");
        s().add("k3", "v3");
        s().add("k3", "你");

        Set<Object> v = s().difference("kk", Arrays.asList("k2", "k3", "k-not-e"));
        assertEquals(v.size(), 2);
        assertTrue(v.contains("v4"));
        assertTrue(v.contains("你的"));
    }

    @Test
    public void shouldSDiffStore() {
        s().add("kk", "v1");
        s().add("kk", "v2");
        s().add("kk", "v3");
        s().add("kk", "v4");
        s().add("kk", "你的");

        s().add("k2", "v1");
        s().add("k2", "v2");
        s().add("k2", "你好");

        s().add("k3", "v1");
        s().add("k3", "v3");
        s().add("k3", "你");

        Long eff = s().differenceAndStore("kk", Arrays.asList("k2", "k3", "k-not-e"), "dest");
        Set<Object> v = s().members("dest");

        assertEquals(eff.intValue(), 2);
        assertEquals(v.size(), 2);
        assertTrue(v.contains("v4"));
        assertTrue(v.contains("你的"));
    }

    @Test
    public void shouldSDiffStoreToEmpty() {
        v().set("dest", "any");

        s().add("kk", "v1");
        s().add("kk", "v2");
        s().add("kk", "v3");
        s().add("kk", "v4");
        s().add("kk", "你的");

        s().add("k2", "v1");
        s().add("k2", "v2");
        s().add("k2", "你的");

        s().add("k3", "v3");
        s().add("k3", "v4");
        s().add("k3", "你");

        Long eff = s().differenceAndStore("kk", Arrays.asList("k2", "k3", "k-not-e"), "dest");
        Set<Object> v = s().members("dest");

        assertEquals(eff.intValue(), 0);
        assertEquals(v.size(), 0);
        assertNull(v().get("dest"));
    }

    @Test
    public void shouldSInter() {
        s().add("kk", "v1");
        s().add("kk", "v2");
        s().add("kk", "v3");
        s().add("kk", "v4");
        s().add("kk", "你的");

        s().add("k2", "v1");
        s().add("k2", "v2");
        s().add("k2", "你的");

        s().add("k3", "v2");
        s().add("k3", "v4");
        s().add("k3", "你");

        Set<Object> v = s().intersect("kk", Arrays.asList("k2", "k3"));

        assertEquals(v.size(), 1);
        assertTrue(v.contains("v2"));
    }

    @Test
    public void shouldSInterStore() {
        v().set("dest", "any");

        s().add("kk", "v1");
        s().add("kk", "v2");
        s().add("kk", "v3");
        s().add("kk", "v4");
        s().add("kk", "你的");

        s().add("k2", "v1");
        s().add("k2", "v2");
        s().add("k2", "你的");

        s().add("k3", "v2");
        s().add("k3", "v4");
        s().add("k3", "你");

        Long eff = s().intersectAndStore("kk", Arrays.asList("k2", "k3"), "dest");
        Set<Object> v = s().members("dest");

        assertEquals(eff.intValue(), 1);
        assertEquals(v.size(), 1);
        assertTrue(v.contains("v2"));
    }

    @Test
    public void shouldSUnion() {

        s().add("kk", "v1");
        s().add("kk", "v2");
        s().add("kk", "v3");
        s().add("kk", "v4");
        s().add("kk", "你的");

        s().add("k2", "v1");
        s().add("k2", "v2");
        s().add("k2", "你的2");

        s().add("k3", "v2");
        s().add("k3", "v4");
        s().add("k3", "你");

        Set<Object> v = s().union(Arrays.asList("k2", "k3", "kk"));
        assertEquals(v.size(), 7);
        assertTrue(v.contains("v1"));
        assertTrue(v.contains("v2"));
        assertTrue(v.contains("v3"));
        assertTrue(v.contains("v4"));
        assertTrue(v.contains("你"));
        assertTrue(v.contains("你的"));
        assertTrue(v.contains("你的2"));
    }

    @Test
    public void shouldUnionEmpty() {
        Set<Object> v = s().union(Arrays.asList("k1", "k2"));

        assertTrue(v.isEmpty());
    }

    @Test
    public void shouldSUnionStoreToSet() {
         
        s().add("dest", "origin");
        
        s().add("kk", "v1");
        s().add("kk", "v2");
        s().add("kk", "v3");
        s().add("kk", "v4");
        s().add("kk", "你的");

        s().add("k2", "v1");
        s().add("k2", "v2");
        s().add("k2", "你的2");

        s().add("k3", "v2");
        s().add("k3", "v4");
        s().add("k3", "你");

        Long eff = s().unionAndStore(Arrays.asList("k2", "k3", "kk"), "dest");
        Set<Object> v = s().members("dest");

        assertEquals(eff.intValue(), 7);
        assertEquals(v.size(), 7);
        assertTrue(v.contains("v1"));
        assertTrue(v.contains("v2"));
        assertTrue(v.contains("v3"));
        assertTrue(v.contains("v4"));
        assertTrue(v.contains("你"));
        assertTrue(v.contains("你的"));
        assertTrue(v.contains("你的2"));
    }
    
    @Test
    public void shouldSUnionStore() {
        v().set("dest", "any");
        s().add("kk", "v1");
        s().add("kk", "v2");
        s().add("kk", "v3");
        s().add("kk", "v4");
        s().add("kk", "你的");

        s().add("k2", "v1");
        s().add("k2", "v2");
        s().add("k2", "你的2");

        s().add("k3", "v2");
        s().add("k3", "v4");
        s().add("k3", "你");

        Long eff = s().unionAndStore(Arrays.asList("k2", "k3", "kk"), "dest");
        Set<Object> v = s().members("dest");

        assertEquals(eff.intValue(), 7);
        assertEquals(v.size(), 7);
        assertTrue(v.contains("v1"));
        assertTrue(v.contains("v2"));
        assertTrue(v.contains("v3"));
        assertTrue(v.contains("v4"));
        assertTrue(v.contains("你"));
        assertTrue(v.contains("你的"));
        assertTrue(v.contains("你的2"));
    }

    @Test
    public void shouldUnionStoreEmpty() {
        v().set("dest", "any");
        Long eff = s().unionAndStore(Arrays.asList("k1", "k2"), "dest");

        assertEquals(eff.intValue(), 0);
         
        assertNull(v().get("dest"));
        assertTrue(s().members("dest").isEmpty());
    }

    @Test
    public void shouldSInterStoreEmpty() {
        v().set("dest", "any");

        Long eff = s().intersectAndStore("k1", Collections.singletonList("k2"), "dest");

        assertEquals(eff.intValue(), 0);
        assertNull(v().get("dest"));
        assertTrue(s().members("dest").isEmpty());
    }

    @Test
    public void shouldSInterEmpty() {
        s().add("kk", "v1");
        s().add("kk", "v2");
        s().add("kk", "v3");
        s().add("kk", "v4");
        s().add("kk", "你的");

        s().add("k2", "v1");
        s().add("k2", "v2");
        s().add("k2", "你的");

        s().add("k3", "v22");
        s().add("k3", "v4");
        s().add("k3", "你");

        Set<Object> v = s().intersect("kk", Arrays.asList("k2", "k3"));

        assertEquals(v.size(), 0);
    }

    @Test
    public void shouldSInterEmptyKey() {
        s().add("kk", "v1");
        s().add("kk", "v2");
        s().add("kk", "v3");
        s().add("kk", "v4");
        s().add("kk", "你的");

        s().add("k2", "v1");
        s().add("k2", "v2");
        s().add("k2", "你的");

        s().add("k3", "v2");
        s().add("k3", "v4");
        s().add("k3", "你");

        Set<Object> v = s().intersect("kk", Arrays.asList("k2", "k3", "k-not-e"));

        assertEquals(v.size(), 0);
    }

    @Test
    public void shouldSInterOnlyOne() {

        s().add("k2", "v1");
        s().add("k2", "v2");
        s().add("k2", "v2");
        s().add("k2", "你的");

        Set<Object> v = s().intersect("k2", Collections.emptyList());

        assertEquals(v.size(), 3);
    }

    @Test
    public void shouldSDiffStoreEmpty() {
        v().set("dest", "any");

        Long eff = s().differenceAndStore("kk", Arrays.asList("k2", "k3", "k-not-e"), "dest");
        Set<Object> v = s().members("dest");

        assertEquals(eff.intValue(), 0);
        assertEquals(v.size(), 0);
        assertNull(v().get("dest"));
    }

    @Test
    public void shouldSDiff2() {
        s().add("kk", "v1");
        s().add("kk", "v2");
        s().add("kk", "v3");
        s().add("kk", "v4");
        s().add("kk", "你的");

        s().add("k2", "v1");
        s().add("k2", "v2");
        s().add("k2", "你好");

        Set<Object> v = s().difference("kk", "k2");
        assertEquals(v.size(), 3);
        assertTrue(v.contains("v3"));
        assertTrue(v.contains("v4"));
        assertTrue(v.contains("你的"));
    }

    @Test
    public void shouldSDiff3() {
        s().add("kk", "v1");
        s().add("kk", "v2");
        s().add("kk", "v3");
        s().add("kk", "v4");
        s().add("kk", "你的");

        s().add("k2", "v11");
        s().add("k2", "v22");
        s().add("k2", "你好");

        Set<Object> v = s().difference("kk", "k2");
        assertEquals(v.size(), 5);
        assertTrue(v.contains("v1"));
        assertTrue(v.contains("v2"));
        assertTrue(v.contains("v3"));
        assertTrue(v.contains("v4"));
        assertTrue(v.contains("你的"));
    }

    @Test
    public void shouldSDiffNoOther() {
        s().add("kk", "v1");
        s().add("kk", "v2");
        s().add("kk", "v3");
        s().add("kk", "v4");
        s().add("kk", "你的");

        Set<Object> v = s().difference("kk", Collections.emptyList());
        assertEquals(v.size(), 5);
        assertTrue(v.contains("v1"));
        assertTrue(v.contains("v2"));
        assertTrue(v.contains("v3"));
        assertTrue(v.contains("v4"));
        assertTrue(v.contains("你的"));
    }

    @Test
    public void shouldSDiffStoreReplace() {
        v().set("dest", "any");
        s().add("kk", "v1");
        s().add("kk", "v2");
        s().add("kk", "v3");
        s().add("kk", "v4");
        s().add("kk", "你的");

        Long eff = s().differenceAndStore("kk", Collections.emptyList(), "dest");
        Set<Object> v = s().members("dest");

        assertEquals(eff.intValue(), 5);
        assertTrue(v.contains("v1"));
        assertTrue(v.contains("v2"));
        assertTrue(v.contains("v3"));
        assertTrue(v.contains("v4"));
        assertTrue(v.contains("你的"));
    }

    @Test
    public void shouldSScan() {
        if (!isEmbededRedis()) {
            return;
        }
        s().add("kk", "v1");
        s().add("kk", "v2");
        s().add("kk", "v3");
        s().add("kk", "v4");
        s().add("kk", "你的");

        Cursor<Object> c = s().scan("kk", ScanOptions.scanOptions()
                .count(2)
                .build());
        List<Object> list = new ArrayList<>();
        while (c.hasNext()) list.add(c.next());

        assertEquals(list.size(), 5);
        assertEquals(list.get(0), "v1");
        assertEquals(list.get(1), "v2");
        assertEquals(list.get(2), "v3");
        assertEquals(list.get(3), "v4");
        assertEquals(list.get(4), "你的");
    }
}

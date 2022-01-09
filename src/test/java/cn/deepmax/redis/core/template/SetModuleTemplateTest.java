package cn.deepmax.redis.core.template;

import cn.deepmax.redis.base.BasePureTemplateTest;
import org.junit.Test;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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

}

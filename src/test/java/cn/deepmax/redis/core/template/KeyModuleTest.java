package cn.deepmax.redis.core.template;

import cn.deepmax.redis.base.BasePureTemplateTest;
import org.junit.Test;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * @author wudi
 * @date 2021/12/30
 */
public class KeyModuleTest extends BasePureTemplateTest {
    public KeyModuleTest(RedisTemplate<String, Object> redisTemplate) {
        super(redisTemplate);
    }

    @Test
    public void shouldScanNormal() {
        if (!isEmbededRedis()) {
            return;
        }
        v().set("a", "1");
        v().set("b", "2");
        v().set("c", "3");
        l().rightPush("d", "5");
        v().set("e", "4");
        l().rightPush("f", "6");
        t().delete("c");
        v().set("e", "44");

        Cursor<byte[]> c = t().execute((RedisCallback<Cursor<byte[]>>) con -> con.scan(ScanOptions.scanOptions()
                .count(2)
                .build()));

        List<byte[]> vs = new ArrayList<>();
        while (c.hasNext()) {
            vs.add(c.next());
        }
        assertEquals(vs.size(), 5);
        assertArrayEquals(vs.get(0), bytes("a"));
        assertArrayEquals(vs.get(1), bytes("b"));
        assertArrayEquals(vs.get(2), bytes("d"));
        assertArrayEquals(vs.get(3), bytes("f"));
        assertArrayEquals(vs.get(4), bytes("e"));
    }

}
package cn.deepmax.redis.core.support;

import cn.deepmax.redis.core.Key;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

/**
 * @author wudi
 * @date 2021/7/5
 */
public class NavMapTest {

    @Test
    public void shouldAddFirst() {
        NavMap<String> map = new NavMap<>();

        map.set(k("1"), "1");
        map.set(k("2"), "2");
        map.set(k("3"), "3");

        map.delete(k("3"));
        map.set(k("4"), "4");
        map.set(k("5"), "5");
        map.set(k("6"), "6");
        map.delete(k("1"));
        map.delete(k("5"));

        NavMap.Node<String> node = map.get(2);
        while (node != null) {
            System.out.print(node.value+" -> ");
            node = node.next;
        }

        System.out.println(".");
    }

    private Key k(String s) {
        return new Key(s.getBytes(StandardCharsets.UTF_8));
    }
}
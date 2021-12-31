package cn.deepmax.redis.core;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;

/**
 * @author wudi
 * @date 2021/5/10
 */
public class Key implements Comparable<Key> {
    private final byte[] content;

    public Key(byte[] content) {
        this.content = content;
    }

    public String str() {
        return new String(content, StandardCharsets.UTF_8);
    }

    public byte[] getContent() {
        return content;
    }

    @Override
    public int compareTo(Key o) {
        //todo binary byte compare
        return Comparator.comparing(Key::str).compare(this, o);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        Key key = (Key) o;
        return Arrays.equals(content, key.content);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(content);
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Key[");
        sb.append(new String(content, StandardCharsets.UTF_8)).append("]");
        return sb.toString();
    }

}

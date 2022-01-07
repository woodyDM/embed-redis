package cn.deepmax.redis.core;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * @author wudi
 * @date 2021/5/10
 */
public class Key implements Comparable<Key> {
    private final byte[] content;
    public static final Key INF = new Key(new byte[]{});
    public static final Key NEG_INF = new Key(new byte[]{});
    public static final Key DUMMY = new Key(new byte[]{});
    
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
        if (o == this) return 0;
        if (this == INF || o == NEG_INF) {
            return 1;
        }
        if (this == NEG_INF || o == INF) {
            return -1;
        }
        //binary compare
        int len = Math.min(content.length, o.content.length);
        for (int i = 0; i < len; i++) {
            if (content[i] == o.content[i]) {
                continue;
            }
            if (content[i] > o.content[i]) {
                return 1;
            } else {
                return -1;
            }
        }
        if (content.length == o.content.length) {
            return 0;
        } else if (content.length > o.content.length) {
            return 1;
        } else {
            return -1;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        if (this == INF || this == NEG_INF || o == INF || o == NEG_INF) return false;
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

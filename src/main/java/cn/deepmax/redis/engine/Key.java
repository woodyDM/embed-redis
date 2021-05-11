package cn.deepmax.redis.engine;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * @author wudi
 * @date 2021/5/10
 */
public class Key {
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Key key = (Key) o;
        return Arrays.equals(content, key.content);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(content);
    }
}

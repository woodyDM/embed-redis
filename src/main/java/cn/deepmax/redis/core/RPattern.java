package cn.deepmax.redis.core;

import cn.deepmax.redis.utils.RegexUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * @author wudi
 * @date 2021/12/30
 */
public class RPattern {

    private final String p;
    private final String regx;
    private final Pattern pattern;
    private static final Map<String, RPattern> cache = new ConcurrentHashMap<>();

    public static RPattern compile(String pt) {
        cache.computeIfAbsent(pt, k -> new RPattern(pt));
        return cache.get(pt);
    }

    public boolean matches(String str) {
        return pattern.matcher(str).find();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RPattern rPattern = (RPattern) o;

        return p.equals(rPattern.p);
    }

    @Override
    public int hashCode() {
        return p.hashCode();
    }

    private RPattern(String p) {
        if (p == null || p.length() == 0) {
            throw new IllegalArgumentException("invalid pattern");
        }
        this.p = p;
        this.regx = RegexUtils.toRegx(p);
        this.pattern = Pattern.compile(this.regx);
    }

}

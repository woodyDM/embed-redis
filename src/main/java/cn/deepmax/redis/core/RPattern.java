package cn.deepmax.redis.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * @author wudi
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
        this.regx = toRegx(p);
        this.pattern = Pattern.compile(this.regx);
    }

    /**
     * h?llo subscribes to hello, hallo and hxllo
     * h*llo subscribes to hllo and heeeello
     * h[ae]llo subscribes to hello and hallo, but not hillo
     * Use \ to escape special characters if you want to match them verbatim.
     *
     * @param word
     * @return
     */
    public static String toRegx(String word) {
        char[] chars = word.toCharArray();
        StringBuilder sb = new StringBuilder();
        sb.append("^");
        for (int i = 0; i < chars.length; i++) {
            boolean start = i == 0;
            if ('?' == chars[i] && (start || chars[i - 1] != '\\')) {
                sb.append('.').append("{1}");
            } else if ('*' == chars[i] && (start || chars[i - 1] != '\\')) {
                sb.append('.').append('*');
            } else {
                sb.append(chars[i]);
            }
        }
        sb.append("$");
        return sb.toString();
    }

}

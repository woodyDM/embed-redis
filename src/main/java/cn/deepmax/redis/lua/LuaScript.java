package cn.deepmax.redis.lua;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author wudi
 * @date 2021/4/30
 */
public class LuaScript {
    
    public static String make(String source, List<String> keys, List<String> args) {
        StringBuilder sb = new StringBuilder();
        sb.append("local redis = require('redis')\n");
        sb.append("KEYS = ").append(values(keys)).append("\n");
        sb.append("ARGV = ").append(values(args)).append("\n");
        sb.append(source);
        return sb.toString();
    }

    private static String values(List<String> l) {
        if (l == null) {
            l = Collections.emptyList();
        }
        return "{" + l.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(","))
                + "}";
    }
}

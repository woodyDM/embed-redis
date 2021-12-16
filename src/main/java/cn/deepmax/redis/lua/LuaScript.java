package cn.deepmax.redis.lua;

/**
 * @author wudi
 * @date 2021/4/30
 */
public class LuaScript {

    public static String make(String source) {
        StringBuilder sb = new StringBuilder();
        sb.append("local redis = require('redis')\n");
        sb.append(source);
        return sb.toString();
    }

}

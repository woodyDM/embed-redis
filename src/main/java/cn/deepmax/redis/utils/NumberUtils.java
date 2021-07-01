package cn.deepmax.redis.utils;

import cn.deepmax.redis.api.RedisParamException;

/**
 * @author wudi
 * @date 2021/5/8
 */
public class NumberUtils {

    public static Long parse(String s) {
        if (s == null || s.isEmpty()) {
            throw RedisParamException.SYNTAX_ERR;
        }
        try {
            return Long.valueOf(s);
        } catch (NumberFormatException e) {
            throw RedisParamException.SYNTAX_ERR;
        }
    }
}

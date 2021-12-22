package cn.deepmax.redis.utils;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.RedisServerException;

/**
 * @author wudi
 * @date 2021/5/8
 */
public class NumberUtils {

    public static Long parse(String s) {
        if (s == null || s.isEmpty()) {
            throw new RedisServerException(Constants.ERR_SYNTAX_NUMBER);
        }
        try {
            return Long.valueOf(s);
        } catch (NumberFormatException e) {
            throw new RedisServerException(Constants.ERR_SYNTAX_NUMBER);
        }
    }
}

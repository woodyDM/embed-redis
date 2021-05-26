package cn.deepmax.redis;


import cn.deepmax.redis.type.RedisError;
import cn.deepmax.redis.type.RedisString;
import cn.deepmax.redis.type.RedisType;

/**
 * @author wudi
 * @date 2021/4/29
 */
public class Constants {

    public static final String EOL = "\r\n";

    private Constants() {
    }

    static final int TYPE_LENGTH = 1;

    static final int EOL_LENGTH = 2;

    static final int NULL_LENGTH = 2;

    static final int NULL_VALUE = -1;

    static final int REDIS_MESSAGE_MAX_LENGTH = 512 * 1024 * 1024; // 512MB

    // 64KB is max inline length of current Redis server implementation.
    static final int REDIS_INLINE_MESSAGE_MAX_LENGTH = 64 * 1024;

    static final int POSITIVE_LONG_MAX_LENGTH = 19; // length of Long.MAX_VALUE

    static final int LONG_MAX_LENGTH = POSITIVE_LONG_MAX_LENGTH + 1; // +1 is sign

    public static final RedisType NO_AUTH_ERROR = new RedisError("NOAUTH Authentication required.");

}

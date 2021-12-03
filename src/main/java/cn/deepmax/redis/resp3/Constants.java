package cn.deepmax.redis.resp3;



/**
 * @author wudi
 * @date 2021/12/3
 */
final class Constants {


    static final int TYPE_LENGTH = 1;

    static final int EOL_LENGTH = 2;        //\r\n length

    static final int INF_LENGTH = 3;        //inf

    static final int NULL_LENGTH = 2;
    
    static final int BOOLEAN_LENGTH = 1;

    static final int EMPTY_LENGTH_VALUE = 0;

    static final int REDIS_MESSAGE_MAX_LENGTH = 512 * 1024 * 1024; // 512MB

    // 64KB is max inline length of current Redis server implementation.
    static final int REDIS_INLINE_MESSAGE_MAX_LENGTH = 64 * 1024;

    static final int POSITIVE_LONG_MAX_LENGTH = 19; // length of Long.MAX_VALUE

    static final int LONG_MAX_LENGTH = POSITIVE_LONG_MAX_LENGTH + 1; // +1 is sign

    static final short NULL_SHORT =  RedisCodecUtil.makeShort('-', '1');

    static final short EOL_SHORT =  RedisCodecUtil.makeShort('\r', '\n');
}

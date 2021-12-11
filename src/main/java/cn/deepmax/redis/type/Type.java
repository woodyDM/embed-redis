package cn.deepmax.redis.type;

/**
 * @author wudi
 * @date 2021/4/30
 * @deprecated use {@link cn.deepmax.redis.resp3.RedisMessageType} instead.
 */
@Deprecated
public enum Type {
    STRING,
    ERROR,
    INTEGER,
    BULK_STRING,
    ARRAY,
    
    COMPOSITE;
}

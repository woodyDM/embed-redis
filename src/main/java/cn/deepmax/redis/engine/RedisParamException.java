package cn.deepmax.redis.engine;

/**
 * @author wudi
 * @date 2021/5/7
 */
public class RedisParamException extends RuntimeException{
    public RedisParamException(String message) {
        super(message);
    }
}

package cn.deepmax.redis.api;

/**
 * @author wudi
 * @date 2021/5/8
 */
public class RedisEngineHolder {
    private static RedisEngine engine;

    public static void set(RedisEngine engine) {
        RedisEngineHolder.engine = engine;
    }

    public static RedisEngine instance() {
        return engine;
    }

}

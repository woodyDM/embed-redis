package cn.deepmax.redis.api;

import cn.deepmax.redis.core.Key;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.List;

/**
 * @author wudi
 * @date 2021/12/24
 */
public interface TransactionManager {

    void multi(Redis.Client client);

    RedisMessage exec(Redis.Client client);

    RedisMessage queue(Redis.Client client, RedisMessage msg);

    void watch(Redis.Client client, List<Key> keys);

    void unwatch(Redis.Client client);

    boolean inspect(Redis.Client client);

    RedisEngine engine();

}

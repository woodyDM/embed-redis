package cn.deepmax.redis.api;

import cn.deepmax.redis.core.Key;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.List;

/**
 * @author wudi
 * @date 2021/12/24
 */
public interface TransactionManager {
    
    void multi(Client client);

    RedisMessage exec(Client client);

    RedisMessage queue(Client client, RedisMessage msg);

    void watch(Client client, List<Key> keys);

    void unwatch(Client client);

    boolean inspect(Client client);

    RedisEngine engine();

}

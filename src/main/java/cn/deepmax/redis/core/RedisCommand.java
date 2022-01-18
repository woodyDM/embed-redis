package cn.deepmax.redis.core;

import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;

/**
 * @author wudi
 */
public interface RedisCommand {
    RedisMessage OK = new SimpleStringRedisMessage("OK");

    /**
     * command for search
     *
     * @return
     */
    default String name() {
        return this.getClass().getSimpleName().toLowerCase();
    }

    RedisMessage response(RedisMessage type, Client client, RedisEngine engine);

    default ListRedisMessage cast(RedisMessage msg) {
        return (ListRedisMessage) msg;
    }

}

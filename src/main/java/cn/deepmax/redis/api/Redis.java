package cn.deepmax.redis.api;

import io.netty.handler.codec.redis.RedisMessage;

public interface Redis {

    RedisConfiguration configuration();

    void setConfiguration(RedisConfiguration configuration);

    RedisMessage exec(RedisMessage type, Client client);

    /**
     * unique client Identification,client must override equals and hashCode;
     */
    interface Client {
        Object id();

        void send(RedisMessage msg);
    }

}

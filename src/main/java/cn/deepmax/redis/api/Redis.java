package cn.deepmax.redis.api;

import cn.deepmax.redis.type.RedisType;

public interface Redis {

    RedisConfiguration configuration();

    void setConfiguration(RedisConfiguration configuration);

    RedisType exec(RedisType type, Client client);

    /**
     * unique client Identification,client must override equals and hashCode;
     */
    interface Client {
        Object id();

        void send(RedisType msg);
    }

}

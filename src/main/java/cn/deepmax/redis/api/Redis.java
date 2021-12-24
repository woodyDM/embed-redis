package cn.deepmax.redis.api;

import io.netty.channel.Channel;
import io.netty.handler.codec.redis.RedisMessage;

public interface Redis {

    RedisConfiguration configuration();

    void setConfiguration(RedisConfiguration configuration);

    RedisMessage exec(RedisMessage type, Client client);

    /**
     * unique client Identification,client must override equals and hashCode;
     */
    interface Client {

        void setQueue(boolean queue);

        void setScripting(boolean scripting);

        boolean scripting();
        
        boolean queued();

        Protocol resp();

        Object id();

        Channel channel();

        void pub(RedisMessage msg);
    }

    enum Protocol {
        RESP2, RESP3
    }

}

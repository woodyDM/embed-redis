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

        int FLAG_QUEUE = 1;
        int FLAG_QUEUE_EXEC = 1 << 1;
        int FLAG_SCRIPTING = 1 << 2;
        
        void setQueue(boolean queue);

        void setFlag(int f, boolean value);

        boolean queryFlag(int f);
        
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

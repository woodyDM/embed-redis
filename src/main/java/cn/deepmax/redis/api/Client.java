package cn.deepmax.redis.api;

import io.netty.channel.Channel;
import io.netty.handler.codec.redis.RedisMessage;

/**
 * unique client Identification,client must override equals and hashCode;
 */
public interface Client {
    /**
     * multi but not exec 
     */
    int FLAG_QUEUE = 1;
    /**
     * exec
     */
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

    enum Protocol {
        RESP2, RESP3
    }
}
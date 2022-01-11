package cn.deepmax.redis.api;

import io.netty.channel.Channel;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.Optional;

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
    /**
     * script
     */
    int FLAG_SCRIPTING = 1 << 2;

    /**
     * the client request node
     *
     * @return empty if standalone
     */
    Optional<RedisConfiguration.Node> node();

    /**
     * engine
     *
     * @return
     */
    RedisEngine engine();

    /**
     * @param queue
     */
    void setQueue(boolean queue);

    /**
     * @param f
     * @param value
     */
    void setFlag(int f, boolean value);

    /**
     * @param f
     * @return
     */
    boolean queryFlag(int f);

    /**
     * @return
     */
    boolean queued();

    /**
     * @return
     */
    Protocol resp();

    default boolean isV2() {
        return resp() == Protocol.RESP2;
    }

    /**
     * @param p
     */
    void setProtocol(Protocol p);

    /**
     * UniqueId
     *
     * @return
     */
    Object id();

    /**
     * bind channel
     *
     * @return
     */
    Channel channel();

    /**
     * publish message to this client
     *
     * @param msg
     */
    void pub(RedisMessage msg);

    enum Protocol {
        RESP2, RESP3
    }
    
}

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
    long id();

    /**
     * bind channel
     *
     * @return
     */
    Channel channel();

    /**
     * command should be execute when called .
     * when in Script or Transaction , command should not block to wait
     * @return
     */
    boolean commandInstantExec();

    /**
     * publish message to this client
     *
     * @param msg
     */
    void pub(RedisMessage msg);

    /**
     * client getname
     *
     * @return
     */
    byte[] getName();

    /**
     * client setname
     *
     * @param name
     */
    void setName(byte[] name);

    enum Protocol {
        RESP2, RESP3
    }

}

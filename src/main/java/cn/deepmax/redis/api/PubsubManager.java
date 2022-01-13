package cn.deepmax.redis.api;

import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.RPattern;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.List;
import java.util.Map;

/**
 * @author wudi
 * @date 2021/5/10
 */
public interface PubsubManager {

    /**
     * subscribe client nubmers
     *
     * @return
     */
    Map<Key, Integer> numbersub(List<Key> keys);

    /**
     * psubscribe patterns
     *
     * @return
     */
    long numberPattern();

    /**
     * @param pattern nullable
     * @return
     */
    List<Key> channelNumbers(RPattern pattern);

    /**
     * subscribe / unsubscribe
     *
     * @return
     */
    Pubsub direct();

    /**
     * psubscribe / punsubscribe
     *
     * @return
     */
    Pubsub pattern();

    /**
     * publish message to channel
     *
     * @param channel
     * @param message
     * @return
     */
    default int pub(Key channel, byte[] message) {
        List<PubPair> p1 = direct().matches(channel, message);
        List<PubPair> p2 = pattern().matches(channel, message);
        p1.forEach(PubPair::publish);
        p2.forEach(PubPair::publish);

        return p1.size() + p2.size();
    }

    /**
     * total client sub channel.
     *
     * @param client
     * @return
     */
    default long subscribeCount(Client client) {
        return direct().subCount(client) + pattern().subCount(client);
    }

    /**
     * called when client disconnect
     *
     * @param client
     */
    default void quit(Client client) {
        direct().quit(client);
        pattern().quit(client);
    }

    /**
     * low pubsub api
     */
    interface Pubsub {
        /**
         * msg matches client
         *
         * @param channel
         * @param msg
         * @return
         */
        List<PubPair> matches(Key channel, byte[] msg);

        /**
         * subscribe
         *
         * @param client
         * @param channel
         * @return
         */
        List<RedisMessage> sub(Client client, List<Key> channel);

        /**
         * unsubscribe
         *
         * @param client
         * @param channel
         * @return
         */
        List<RedisMessage> unsub(Client client, List<Key> channel);

        /**
         * unsubscribe all
         *
         * @param client
         * @return
         */
        List<RedisMessage> unsubAll(Client client);

        /**
         * query total client sub count
         *
         * @param client
         * @return
         */
        long subCount(Client client);

        /**
         * called when disconnect
         *
         * @param client
         */
        void quit(Client client);
    }

    class PubPair {
        private final Client client;
        private final RedisMessage msg;

        public PubPair(Client client, RedisMessage msg) {
            this.client = client;
            this.msg = msg;
        }

        //publish msg to client
        public void publish() {
            client.pub(msg);
        }

        public Client getClient() {
            return client;
        }

        public RedisMessage getMsg() {
            return msg;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PubPair pubPair = (PubPair) o;
            return client.id() == pubPair.client.id();
        }

        @Override
        public int hashCode() {
            return (client.id() + "").hashCode();
        }
    }

}

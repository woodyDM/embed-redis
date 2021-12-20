package cn.deepmax.redis.api;

import cn.deepmax.redis.core.Key;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.List;

/**
 * @author wudi
 * @date 2021/5/10
 */
public interface PubsubManager {
    /**
     * subscribe / unsubscribe
     * @return
     */
    Pubsub direct();

    /**
     * psubscribe / punsubscribe
     * @return
     */
    Pubsub pattern();

    /**
     * publish message to channel
     * @param channel
     * @param message
     * @return
     */
    default int pub(Key channel, byte[] message) {
        List<PubPair> p1 = direct().matches(channel, message);
        List<PubPair> p2 = pattern().matches(channel, message);

        p1.forEach(p -> direct().pub(p));
        p2.forEach(p -> pattern().pub(p));

        return p1.size() + p2.size();
    }

    /**
     * total client sub channel.
     * @param client
     * @return
     */
    default long subscribeCount(Redis.Client client) {
        return direct().subCount(client) + pattern().subCount(client);
    }

    /**
     * called when client disconnect
     * @param client
     */
    default void quit(Redis.Client client) {
        direct().quit(client);
        pattern().quit(client);
    }

    /**
     * low pubsub api
     */
    interface Pubsub {
        /**
         * msg matches client
         * @param channel
         * @param msg
         * @return
         */
        List<PubPair> matches(Key channel, byte[] msg);

        /**
         * publish message
         * @param pubPair
         */
        void pub(PubPair pubPair);

        /**
         * subscribe
         * @param client
         * @param channel
         * @return
         */
        List<RedisMessage> sub(Redis.Client client, Key... channel);

        /**
         * unsubscribe
         * @param client
         * @param channel
         * @return
         */
        List<RedisMessage> unsub(Redis.Client client, Key... channel);

        /**
         * unsubscribe all
         * @param client
         * @return
         */
        List<RedisMessage> unsubAll(Redis.Client client);

        /**
         * query total client sub count
         * @param client
         * @return
         */
        long subCount(Redis.Client client);

        /**
         * called when disconnect
         * @param client
         */
        void quit(Redis.Client client);
    }

    class PubPair {
        private final Redis.Client client;
        private final RedisMessage msg;

        public PubPair(Redis.Client client, RedisMessage msg) {
            this.client = client;
            this.msg = msg;
        }

        public Redis.Client getClient() {
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
            return client.id().equals(pubPair.client.id());
        }

        @Override
        public int hashCode() {
            return client.id().hashCode();
        }
    }

}

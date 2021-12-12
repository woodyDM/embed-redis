package cn.deepmax.redis.api;

import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.type.RedisType;

import java.util.Set;

/**
 * @author wudi
 * @date 2021/5/10
 */
public interface PubsubManager {

    Pubsub normal();

    Pubsub regex();

    default int pub(Key channel, byte[] message) {
        Set<PubPair> p1 = normal().matches(channel, message);

        Set<PubPair> p2 = regex().matches(channel, message);
        p2.removeAll(p1);

        p1.forEach(p -> normal().pub(p));
        p2.forEach(p -> regex().pub(p));

        return p1.size() + p2.size();
    }

    default void quit(Redis.Client client) {
        normal().quit(client);
        regex().quit(client);
    }

    class PubPair {
        private final Redis.Client client;
        private final RedisType msg;

        public PubPair(Redis.Client client, RedisType msg) {
            this.client = client;
            this.msg = msg;
        }

        public Redis.Client getClient() {
            return client;
        }

        public RedisType getMsg() {
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

    interface Pubsub {

        Set<PubPair> matches(Key channel, byte[] msg);

        void pub(PubPair pubPair);

        void sub(Redis.Client client, Key... channel);

        void unsub(Redis.Client client, Key... channel);

        void quit(Redis.Client client);
    }

}
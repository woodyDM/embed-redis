package cn.deepmax.redis.api;

import cn.deepmax.redis.core.Key;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.List;
import java.util.Set;

/**
 * @author wudi
 * @date 2021/5/10
 */
public interface PubsubManager {

    Pubsub normal();

    Pubsub regex();

    default int pub(Key channel, byte[] message) {
        List<PubPair> p1 = normal().matches(channel, message);
        List<PubPair> p2 = regex().matches(channel, message);
        
        p1.forEach(p -> normal().pub(p));
        p2.forEach(p -> regex().pub(p));

        return p1.size() + p2.size();
    }

    default void quit(Redis.Client client) {
        normal().quit(client);
        regex().quit(client);
    }

    interface Pubsub {

        List<PubPair> matches(Key channel, byte[] msg);

        void pub(PubPair pubPair);

        List<Integer> sub(Redis.Client client, Key... channel);

        void unsub(Redis.Client client, Key... channel);

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

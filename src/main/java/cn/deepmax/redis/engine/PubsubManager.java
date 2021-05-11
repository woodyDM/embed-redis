package cn.deepmax.redis.engine;

import cn.deepmax.redis.type.RedisType;
import io.netty.channel.Channel;

import java.util.Objects;
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

    default void quit(Channel client) {
        normal().quit(client);
        regex().quit(client);
    }

    class PubPair {
        private final Channel channel;
        private final RedisType msg;

        public PubPair(Channel channel, RedisType msg) {
            this.channel = channel;
            this.msg = msg;
        }

        public Channel getChannel() {
            return channel;
        }

        public RedisType getMsg() {
            return msg;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PubPair pubPair = (PubPair) o;
            return channel.equals(pubPair.channel);
        }

        @Override
        public int hashCode() {
            return Objects.hash(channel);
        }
    }

    interface Pubsub {

        Set<PubPair> matches(Key channel, byte[] msg);

        void pub(PubPair pubPair);

        void sub(Channel client, Key... channel);

        void unsub(Channel client, Key... channel);

        void quit(Channel client);
    }

}

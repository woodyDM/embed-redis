package cn.deepmax.redis.type;

import io.netty.handler.codec.redis.RedisMessage;

import java.util.List;

/**
 * @author wudi
 */
public interface CompositeRedisMessage extends RedisMessage {

    List<RedisMessage> children();

    static CompositeRedisMessage of(List<RedisMessage> msg) {
        return new Impl(msg);
    }

    class Impl implements CompositeRedisMessage {
        private final List<RedisMessage> c;

        Impl(List<RedisMessage> c) {
            this.c = c;
        }

        @Override
        public List<RedisMessage> children() {
            return c;
        }
    }
}

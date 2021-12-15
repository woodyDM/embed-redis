package cn.deepmax.redis.resp3;

import io.netty.handler.codec.redis.RedisMessage;

import java.util.List;

public class MapRedisMessage extends AbstractMapRedisMessage {
    public static final MapRedisMessage EMPTY = new MapRedisMessage() {
        @Override
        public MapRedisMessage retain() {
            return this;
        }

        @Override
        public MapRedisMessage retain(int increment) {
            return this;
        }

        @Override
        public MapRedisMessage touch() {
            return this;
        }

        @Override
        public MapRedisMessage touch(Object hint) {
            return this;
        }

        @Override
        public boolean release() {
            return false;
        }

        @Override
        public boolean release(int decrement) {
            return false;
        }

        @Override
        public String toString() {
            return "EmptyMapRedisMessage";
        }
    };

    public MapRedisMessage(List<RedisMessage> list) {
        super(list);
    }

    private MapRedisMessage() {
    }
}

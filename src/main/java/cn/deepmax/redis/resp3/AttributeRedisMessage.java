package cn.deepmax.redis.resp3;

import io.netty.handler.codec.redis.RedisMessage;

import java.util.List;

public class AttributeRedisMessage extends AbstractMapRedisMessage {
    public AttributeRedisMessage(List<RedisMessage> list) {
        super(list);
    }
    private AttributeRedisMessage(){}

    public static final AttributeRedisMessage EMPTY = new AttributeRedisMessage(){
        @Override
        public AttributeRedisMessage retain() {
            return this;
        }

        @Override
        public AttributeRedisMessage retain(int increment) {
            return this;
        }

        @Override
        public AttributeRedisMessage touch() {
            return this;
        }

        @Override
        public AttributeRedisMessage touch(Object hint) {
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
            return "EmptyAttributeRedisMessage";
        }
    };
}

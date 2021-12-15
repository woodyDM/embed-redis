package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.core.support.BaseCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

public class StringModule extends BaseModule {
    public StringModule() {
        super("string");
        register(new Get());
        register(new Set());
    }


    private static class Get extends BaseCommand<RString> {
        @Override
        protected RedisMessage response_(RedisMessage type, Redis.Client client) {
            ListRedisMessage msg = cast(type);
            if (msg.children().size() < 2) {
                return new ErrorRedisMessage("invalid set size");
            }
            byte[] key = msg.getAt(1).bytes();
            RString old = get(key);
            if (old == null) {
                return FullBulkStringRedisMessage.NULL_INSTANCE;
            } else {
                return new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(old.getS()));
            }
        }
    }

    private static class Set extends BaseCommand<RString> {
        @Override
        protected RedisMessage response_(RedisMessage type, Redis.Client client) {
            ListRedisMessage msg = cast(type);
            if (msg.children().size() < 3) {
                return new ErrorRedisMessage("invalid set size");
            }
            byte[] key = msg.getAt(1).bytes();
            byte[] value = msg.getAt(2).bytes();
            set(key, new RString(value));
            return OK;
        }
    }

}

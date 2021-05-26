package cn.deepmax.redis.engine.module;

import cn.deepmax.redis.engine.support.BaseCommand;
import cn.deepmax.redis.engine.support.BaseModule;
import cn.deepmax.redis.type.*;
import io.netty.channel.ChannelHandlerContext;

public class StringModule extends BaseModule {
    public StringModule() {
        super("string");
        register(new Get());
        register(new Set());
    }


    private static class Get extends BaseCommand<RString> {
        @Override
        protected RedisType response_(RedisType type, ChannelHandlerContext ctx) {
            if (type.size() < 2) {
                return new RedisError("invalid set size");
            }
            byte[] key = type.get(1).bytes();
            RString old = get(key);
            if (old == null) {
                return RedisBulkString.NIL;
            } else {
                return RedisBulkString.of(old.getS());
            }
        }

    }

    private static class Set extends BaseCommand<RString> {
        @Override
        protected RedisType response_(RedisType type, ChannelHandlerContext ctx) {
            if (type.size() < 3) {
                return new RedisError("invalid set size");
            }
            byte[] key = type.get(1).bytes();
            byte[] value = type.get(2).bytes();
            set(key, new RString(value));
            return OK;
        }
    }

}

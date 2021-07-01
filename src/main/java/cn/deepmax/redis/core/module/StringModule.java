package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.core.support.BaseCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.type.*;

public class StringModule extends BaseModule {
    public StringModule() {
        super("string");
        register(new Get());
        register(new Set());
    }


    private static class Get extends BaseCommand<RString> {
        @Override
        protected RedisType response_(RedisType type, Redis.Client client) {
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
        protected RedisType response_(RedisType type, Redis.Client client) {
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

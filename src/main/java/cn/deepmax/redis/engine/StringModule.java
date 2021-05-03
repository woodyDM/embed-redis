package cn.deepmax.redis.engine;

import cn.deepmax.redis.type.*;
import io.netty.channel.ChannelHandlerContext;

public class StringModule extends BaseTtlModule<InRedisString>{
    public StringModule(  ) {
        super("string");
        register(new Del());
        register(new Get());
        register(new Set());
    }

  
    private class Del implements RedisCommand{
        @Override
        public RedisType response(RedisType type, ChannelHandlerContext ctx) {

            if (type.children().size() < 2) {
                return new RedisError("ERR wrong number of arguments for 'del' command");
            }
            int c = 0;
            for (int i = 1; i < type.children().size(); i++) {
                InRedisString old = del(type.get(i).bytes());
                if (old!=null && !expired(old)) {
                    c++;
                }
            }
            return new RedisInteger(c);
        }
    }

    private class Get implements RedisCommand {
        @Override
        public RedisType response(RedisType type, ChannelHandlerContext ctx) {

            if (type.size() < 2) {
                return new RedisError("invalid set size");
            }
            byte[] key = type.get(1).bytes();
            InRedisString old = get(key);
            if (old == null) {
                return RedisBulkString.NIL;
            }else{
                return RedisBulkString.of(old.getS());
            }
        }
    }

    private class Set implements RedisCommand {
        @Override
        public RedisType response(RedisType m, ChannelHandlerContext ctx) {
            if (m.size() < 3) {
                return new RedisError("invalid set size");
            }
            byte[] key = m.get(1).bytes();
            byte[] value = m.get(2).bytes();
            StringModule.super.set(key, new InRedisString(value));
            return new RedisString("OK");
        }
    }
 
}

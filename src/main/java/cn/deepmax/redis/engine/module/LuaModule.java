package cn.deepmax.redis.engine.module;

import cn.deepmax.redis.engine.RedisCommand;
import cn.deepmax.redis.engine.RedisEngine;
import cn.deepmax.redis.engine.support.BaseModule;
import cn.deepmax.redis.engine.support.CompositeCommand;
import cn.deepmax.redis.type.*;
import cn.deepmax.redis.utils.SHA1Utils;
import io.netty.channel.ChannelHandlerContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author wudi
 * @date 2021/5/7
 */
public class LuaModule extends BaseModule {
    public LuaModule() {
        super("lua");
        init();
    }

    private void init() {
        CompositeCommand script = new CompositeCommand("script");
        script.add(new Load());
        script.add(new Exists());
        script.add(new Flush());
        
        register(script);
        register(new Eval());
        register(new Evalsha());
    }

    private final Map<String, String> scriptCache = new ConcurrentHashMap<>();

    private class Eval implements RedisCommand {
        @Override
        public RedisType response(RedisType type, ChannelHandlerContext ctx, RedisEngine engine) {
            String lua = type.get(1).str();
            return LuaModule.this.response(lua, type, ctx, engine);
        }
    }

    private class Evalsha implements RedisCommand {
        @Override
        public RedisType response(RedisType type, ChannelHandlerContext ctx, RedisEngine engine) {
            String sha1 = type.get(1).str();
            String lua = scriptCache.get(sha1);
            if (lua != null) {
                return LuaModule.this.response(lua, type, ctx, engine);
            }else{
                return new RedisError("NOSCRIPT No matching script. Please use EVAL.");
            }
        }
    }
    
    private RedisType response(String luaScript, RedisType type, ChannelHandlerContext ctx, RedisEngine engine) {
        return new RedisError("NN");
    }

    private class Load implements RedisCommand {
        @Override
        public RedisType response(RedisType type, ChannelHandlerContext ctx, RedisEngine engine) {
            String script = type.get(2).str();
            String v = SHA1Utils.sha1(script);
            scriptCache.put(v, script);
            return RedisBulkString.of(v);
        }
    }

    private class Exists implements RedisCommand {
        @Override
        public RedisType response(RedisType type, ChannelHandlerContext ctx, RedisEngine engine) {
            RedisArray result = new RedisArray();
            for (int i = 2; i < type.size(); i++) {
                String sha1 = type.get(i).str();
                boolean exist = scriptCache.containsKey(sha1);
                result.add(new RedisInteger(exist ? 1 : 0));
            }
            return result;
        }
    }

    private class Flush implements RedisCommand {
        @Override
        public RedisType response(RedisType type, ChannelHandlerContext ctx, RedisEngine engine) {
            scriptCache.clear();
            return new RedisString("OK");
        }
    }
}


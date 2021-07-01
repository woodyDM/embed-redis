package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.core.RedisCommand;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisParamException;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.core.support.CompositeCommand;
import cn.deepmax.redis.lua.LuaFuncException;
import cn.deepmax.redis.lua.LuaScript;
import cn.deepmax.redis.lua.RedisLuaConverter;
import cn.deepmax.redis.type.*;
import cn.deepmax.redis.utils.NumberUtils;
import cn.deepmax.redis.utils.SHA1;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.jse.JsePlatform;

import java.util.ArrayList;
import java.util.List;
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
        public RedisType response(RedisType type, Redis.Client client, RedisEngine engine) {
            String lua = type.get(1).str();
            return LuaModule.this.response(lua, type, client, engine);
        }
    }

    private class Evalsha implements RedisCommand {
        @Override
        public RedisType response(RedisType type, Redis.Client client, RedisEngine engine) {
            String sha1 = type.get(1).str();
            String lua = scriptCache.get(sha1);
            if (lua != null) {
                return LuaModule.this.response(lua, type, client, engine);
            }else{
                return new RedisError("NOSCRIPT No matching script. Please use EVAL.");
            }
        }
    }
    
    private RedisType response(String luaScript, RedisType type, Redis.Client client, RedisEngine engine) {
        try {
            int keyNum = NumberUtils.parse(type.get(2).str()).intValue();
            List<String> key = new ArrayList<>();
            List<String> arg = new ArrayList<>();
            for (int i = 3; i < 3 + keyNum; i++) {
                key.add(type.get(i).str());
            }
            for (int i = 3 + keyNum; i < type.size(); i++) {
                arg.add(type.get(i).str());
            }
            String fullLua = LuaScript.make(luaScript, key, arg);
            Globals globals = JsePlatform.standardGlobals();
            LuaValue lua = globals.load(fullLua);
            LuaValue callResult = lua.call();
            return RedisLuaConverter.toRedis(callResult);
        }  catch (LuaError error) {
            if (error.getCause() instanceof LuaFuncException) {
                LuaFuncException c = (LuaFuncException) error.getCause();
                return new RedisError(c.getMessage());
            }
            return new RedisError("ERR " + error.getMessage());
        }
    }

    private class Load implements RedisCommand {
        @Override
        public RedisType response(RedisType type, Redis.Client client, RedisEngine engine) {
            String script = type.get(2).str();
            if (script == null || script.length() == 0) {
                throw new RedisParamException("invalid lua script");
            }
            String v = SHA1.encode(script);
            scriptCache.put(v, script);
            return RedisBulkString.of(v);
        }
    }

    private class Exists implements RedisCommand {
        @Override
        public RedisType response(RedisType type, Redis.Client client, RedisEngine engine) {
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
        public RedisType response(RedisType type, Redis.Client client, RedisEngine engine) {
            scriptCache.clear();
            return OK;
        }
    }
}


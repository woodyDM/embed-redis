package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisParamException;
import cn.deepmax.redis.core.RedisCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.core.support.CompositeCommand;
import cn.deepmax.redis.lua.LuaFuncException;
import cn.deepmax.redis.lua.LuaScript;
import cn.deepmax.redis.lua.RedisLuaConverter;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.utils.NumberUtils;
import cn.deepmax.redis.utils.SHA1;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaTable;
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
    private final Map<String, String> scriptCache = new ConcurrentHashMap<>();

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

    private RedisMessage response(String luaScript, RedisMessage type, Redis.Client client, RedisEngine engine) {
        try {
            ListRedisMessage msg = (ListRedisMessage) type;
            int keyNum = NumberUtils.parse(msg.getAt(2).str()).intValue();
            List<FullBulkValueRedisMessage> key = new ArrayList<>();
            List<FullBulkValueRedisMessage> arg = new ArrayList<>();
            for (int i = 3; i < 3 + keyNum; i++) {
                key.add(msg.getAt(i));
            }
            for (int i = 3 + keyNum; i < msg.children().size(); i++) {
                arg.add(msg.getAt(i));
            }

            String fullLua = LuaScript.make(luaScript);
            
            Globals globals = JsePlatform.standardGlobals();
            globals.set("KEYS", make(key));
            globals.set("ARGV", make(arg));
            
            LuaValue lua = globals.load(fullLua);
            LuaValue callResult = lua.call();
            return RedisLuaConverter.toRedis(callResult);
        } catch (LuaError error) {
            if (error.getCause() instanceof LuaFuncException) {
                LuaFuncException c = (LuaFuncException) error.getCause();
                return new ErrorRedisMessage(c.getMessage());
            }
            return new ErrorRedisMessage("ERR " + error.getMessage());
        }
    }

    private LuaTable make(List<FullBulkValueRedisMessage> list) {
        LuaTable table = LuaTable.tableOf();
        for (int i = 0; i < list.size(); i++) {
            FullBulkValueRedisMessage msg = list.get(i);
            table.set(1 + i, LuaValue.valueOf(msg.bytes()));
        }
        return table;
    }

    private class Eval implements RedisCommand {
        @Override
        public RedisMessage response(RedisMessage type, Redis.Client client, RedisEngine engine) {
            String lua = cast(type).getAt(1).str();
            return LuaModule.this.response(lua, type, client, engine);
        }
    }

    private class Evalsha implements RedisCommand {
        @Override
        public RedisMessage response(RedisMessage type, Redis.Client client, RedisEngine engine) {
            String sha1 = cast(type).getAt(1).str();
            String lua = scriptCache.get(sha1);
            if (lua != null) {
                return LuaModule.this.response(lua, type, client, engine);
            } else {
                return new ErrorRedisMessage("NOSCRIPT No matching script. Please use EVAL.");
            }
        }
    }

    private class Load implements RedisCommand {
        @Override
        public RedisMessage response(RedisMessage type, Redis.Client client, RedisEngine engine) {
            String script = cast(type).getAt(2).str();
            if (script == null || script.length() == 0) {
                throw new RedisParamException("invalid lua script");
            }
            String v = SHA1.encode(script);
            scriptCache.put(v, script);
            return FullBulkValueRedisMessage.ofString(v);
        }
    }

    private class Exists implements RedisCommand {
        @Override
        public RedisMessage response(RedisMessage type, Redis.Client client, RedisEngine engine) {
            ListRedisMessage msg = cast(type);
            List<RedisMessage> result = new ArrayList<>();
            for (int i = 2; i < msg.children().size(); i++) {
                String sha1 = msg.getAt(i).str();
                boolean exist = scriptCache.containsKey(sha1);
                result.add(new IntegerRedisMessage(exist ? 1 : 0));
            }
            return new ListRedisMessage(result);
        }
    }

    private class Flush implements RedisCommand {
        @Override
        public RedisMessage response(RedisMessage type, Redis.Client client, RedisEngine engine) {
            scriptCache.clear();
            return OK;
        }
    }
}


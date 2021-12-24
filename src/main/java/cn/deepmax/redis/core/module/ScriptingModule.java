package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisServerException;
import cn.deepmax.redis.core.support.ArgsCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.lua.LuaFuncException;
import cn.deepmax.redis.lua.LuaScript;
import cn.deepmax.redis.lua.RedisLuaConverter;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
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
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author wudi
 * @date 2021/5/7
 */
public class ScriptingModule extends BaseModule {
    private final Map<String, String> scriptCache = new ConcurrentHashMap<>();
    private final Load cmdLoad = new Load();
    private final Exists cmdExists = new Exists();
    private final Flush cmdFlush = new Flush();

    public ScriptingModule() {
        super("scripting");

        register(new Script());
        register(new Eval());
        register(new Evalsha());
    }

    public void flush() {
        scriptCache.clear();
    }

    public class Script extends ArgsCommand.Two {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Redis.Client client, RedisEngine engine) {
            String subCommand = msg.getAt(1).str().toLowerCase();
            switch (subCommand) {
                case "flush":
                    return cmdFlush.response(msg, client, engine);
                case "load":
                    return cmdLoad.response(msg, client, engine);
                case "exists":
                    return cmdExists.response(msg, client, engine);
                default:
                    throw new IllegalStateException("in valid subCommand");
            }
        }

        @Override
        public Optional<ErrorRedisMessage> preCheckLength(RedisMessage type) {
            ListRedisMessage msg = cast(type);
            int len = msg.children().size();
            String subCommand = msg.getAt(1).str().toLowerCase();
            String errMsg = String.format("ERR Unknown subcommand or wrong number of arguments for '%s'. Try SCRIPT HELP.", subCommand);
            switch (subCommand) {
                case "exists":
                case "load":
                    if (len != 3) return Optional.of(new ErrorRedisMessage(errMsg));
                    break;
                case "flush":
                    if (len != 2) return Optional.of(new ErrorRedisMessage(errMsg));
                    break;
                default:
                    return Optional.of(new ErrorRedisMessage(errMsg));
            }
            return super.preCheckLength(type);
        }
    }

    private RedisMessage response(String luaScript, RedisMessage type, Redis.Client client, RedisEngine engine) {
        try {
            ListRedisMessage msg = (ListRedisMessage) type;
            int keyNum = msg.getAt(2).val().intValue();
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

    private class Eval extends ArgsCommand.Two {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Redis.Client client, RedisEngine engine) {
            String lua = msg.getAt(1).str();
            return ScriptingModule.this.response(lua, msg, client, engine);
        }
    }

    //sub command: script evalsha xxx
    private class Evalsha extends ArgsCommand.Two {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Redis.Client client, RedisEngine engine) {
            String sha1 = msg.getAt(1).str();
            String lua = scriptCache.get(sha1.toLowerCase());
            if (lua != null) {
                return ScriptingModule.this.response(lua, msg, client, engine);
            } else {
                return new ErrorRedisMessage("NOSCRIPT No matching script. Please use EVAL.");
            }
        }
    }

    private class Load extends ArgsCommand.ThreeEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Redis.Client client, RedisEngine engine) {
            String script = msg.getAt(2).str();
            if (script == null || script.length() == 0) {
                throw new RedisServerException("invalid lua script");
            }
            String v = SHA1.encode(script);
            scriptCache.put(v, script);
            return FullBulkValueRedisMessage.ofString(v);
        }

    }

    private class Exists extends ArgsCommand.ThreeEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Redis.Client client, RedisEngine engine) {
            List<RedisMessage> result = new ArrayList<>();
            for (int i = 2; i < msg.children().size(); i++) {
                String sha1 = msg.getAt(i).str();
                boolean exist = scriptCache.containsKey(sha1);
                result.add(new IntegerRedisMessage(exist ? 1 : 0));
            }
            return new ListRedisMessage(result);
        }

    }

    private class Flush extends ArgsCommand.TwoEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Redis.Client client, RedisEngine engine) {
            scriptCache.clear();
            return OK;
        }

    }
}


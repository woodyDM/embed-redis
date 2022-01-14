package cn.deepmax.redis.core.module;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.Flushable;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.support.ArgsCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.core.support.CompositeCommand;
import cn.deepmax.redis.lua.LuaFuncException;
import cn.deepmax.redis.lua.RedisLib;
import cn.deepmax.redis.lua.RedisLuaConverter;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.utils.SHA1;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import lombok.extern.slf4j.Slf4j;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.jse.JsePlatform;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author wudi
 * @date 2021/5/7
 */
@Slf4j
public class ScriptingModule extends BaseModule implements Flushable {
    private final Map<String, String> scriptCache = new ConcurrentHashMap<>();

    public ScriptingModule() {
        super("scripting");
        register(new Eval());
        register(new Evalsha());
        register(new CompositeCommand("script")
                .with(new ScriptLoad())
                .with(new ScriptFlush())
                .with(new ScriptExists())
        );
    }

    @Override
    public void flush() {
        log.debug("flush all script");
        scriptCache.clear();
    }

    //EVAL script numkeys [key [key ...]] [arg [arg ...]]
    public class Eval extends ArgsCommand.Two {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            String lua = msg.getAt(1).str();
            return ScriptingModule.this.response(lua, msg, client, engine);
        }
    }

    //EVALSHA sha1 numkeys [key [key ...]] [arg [arg ...]]
    public class Evalsha extends ArgsCommand.Two {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            String sha1 = msg.getAt(1).str();
            String lua = scriptCache.get(sha1.toLowerCase());
            if (lua != null) {
                return ScriptingModule.this.response(lua, msg, client, engine);
            } else {
                return new ErrorRedisMessage("NOSCRIPT No matching script. Please use EVAL.");
            }
        }
    }

    private RedisMessage response(String luaScript, RedisMessage type, Client client, RedisEngine engine) {
        try {
            ListRedisMessage msg = (ListRedisMessage) type;
            int keyNum = msg.getAt(2).val().intValue();
            List<FullBulkValueRedisMessage> key = new ArrayList<>();
            for (int i = 3; i < 3 + keyNum; i++) {
                key.add(msg.getAt(i));
            }
            List<FullBulkValueRedisMessage> arg = new ArrayList<>();
            for (int i = 3 + keyNum; i < msg.children().size(); i++) {
                arg.add(msg.getAt(i));
            }

            Globals globals = JsePlatform.standardGlobals();
            //load redis lib and set envs
            globals.load(new RedisLib(engine, client));
            globals.set("KEYS", makeArgs(key));
            globals.set("ARGV", makeArgs(arg));

            LuaValue lua = globals.load(luaScript);
            client.setFlag(Client.FLAG_SCRIPTING, true);
            LuaValue callResult = lua.call();
            //now flag scripting is off. events fired in RedisExecutor
            return RedisLuaConverter.toRedis(callResult);
        } catch (LuaError error) {
            if (error.getCause() instanceof LuaFuncException) {
                LuaFuncException c = (LuaFuncException) error.getCause();
                return new ErrorRedisMessage(c.getMessage());
            }
            return new ErrorRedisMessage("ERR " + error.getMessage());
        } finally {
            client.setFlag(Client.FLAG_SCRIPTING, false);
        }
    }

    private LuaTable makeArgs(List<FullBulkValueRedisMessage> list) {
        LuaTable table = LuaTable.tableOf();
        for (int i = 0; i < list.size(); i++) {
            FullBulkValueRedisMessage msg = list.get(i);
            table.set(1 + i, LuaValue.valueOf(msg.bytes()));
        }
        return table;
    }


    public class ScriptLoad extends ArgsCommand.ThreeEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            String script = msg.getAt(2).str();
            if (script.length() == 0) {
                return new ErrorRedisMessage("invalid lua script");
            }
            String v = SHA1.encode(script);
            scriptCache.put(v, script);
            return FullBulkValueRedisMessage.ofString(v);
        }
    }

    public class ScriptExists extends ArgsCommand.Three {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            List<Key> shaValues = genKeys(msg.children(), 2);

            List<RedisMessage> result = shaValues.stream().map(s -> {
                boolean exist = scriptCache.containsKey(s.str().toLowerCase());
                return exist ? Constants.INT_ONE : Constants.INT_ZERO;
            }).collect(Collectors.toList());
            return new ListRedisMessage(result);
        }
    }

    public class ScriptFlush extends ArgsCommand.TwoEx {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            scriptCache.clear();
            return OK;
        }
    }
}


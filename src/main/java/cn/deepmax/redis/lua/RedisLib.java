package cn.deepmax.redis.lua;

import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.resp3.RedisMessageType;
import cn.deepmax.redis.type.RedisMessages;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import lombok.extern.slf4j.Slf4j;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.TwoArgFunction;
import org.luaj.vm2.lib.VarArgFunction;

/**
 * lua bridge for redis
 *
 * @see <a href='https://redis.io/commands/eval'></a>
 * @see <a href='http://www.luaj.org/luaj/3.0/examples/jse/hyperbolic.java'></a>
 * @see <a href='http://www.luaj.org/luaj/3.0/examples/lua/hyperbolicapp.lua'></a>
 */
@Slf4j
public class RedisLib extends TwoArgFunction {

    private final RedisEngine engine;
    private final Client client;

    public RedisLib(RedisEngine engine, Client client) {
        this.engine = engine;
        this.client = client;
    }

    /**
     * package load entry
     * add redis methods.
     *
     * @param modname
     * @param env
     * @return
     * @see <a href='http://www.luaj.org/luaj/3.0/examples/jse/hyperbolic.java'></a>
     * @see <a href='http://www.luaj.org/luaj/3.0/examples/lua/hyperbolicapp.lua'></a>
     */
    @Override
    public LuaValue call(LuaValue modname, LuaValue env) {
        LuaValue library = tableOf();
        library.set("call", new Call());
        library.set("pcall", new PCall());
        library.set("error_reply", new ErrorReply());
        library.set("status_reply", new StatusReply());
        library.set("setresp", new SetResp());

        env.set("redis", library);
        return library;
    }

    /**
     * @see <a href='https://redis.io/commands/eval'></a>
     */
    final class Call extends TheCall {
        @Override
        protected RedisMessage onError(RedisMessage resp) {
            if (resp instanceof ErrorRedisMessage) {
                throw new LuaFuncException(((ErrorRedisMessage) resp).content());
            } else if (resp instanceof FullBulkValueRedisMessage && ((FullBulkValueRedisMessage) resp).type() == RedisMessageType.BLOG_ERROR) {
                throw new LuaFuncException(((FullBulkValueRedisMessage) resp).str());
            } else {
                throw new IllegalStateException("unknown error type " + resp.getClass().getName());
            }
        }
    }

    final class PCall extends TheCall {
        @Override
        protected RedisMessage onError(RedisMessage resp) {
            //the error will convert to LuaTable
            return resp;
        }
    }

    abstract class TheCall extends VarArgFunction {
        @Override
        public Varargs invoke(Varargs args) {
            RedisMessage msg = RedisLuaConverter.toRedis(args, client.resp());
            RedisMessage resp;
            try {
                // a Redis command call will result in an error, redis.call() will 
                // raise a Lua error that in turn will force EVAL to return an error to the command caller,
                // redis.pcall will trap the error and return a Lua table representing the error.
                resp = engine.execute(msg, client);
                if (RedisMessages.isError(resp)) {
                    resp = onError(resp);
                }
            } catch (LuaFuncException e) {
                throw e;
            } catch (Exception e) {
                resp = ListRedisMessage.newBuilder().append("ERR internal " + e.getMessage()).build();
            }
            return RedisLuaConverter.toLua(resp, client.resp());
        }

        protected abstract RedisMessage onError(RedisMessage resp);
    }

    /**
     * This function simply returns a single field table with the err field set to the specified string for you.
     */
    final static class ErrorReply extends Reply {
        @Override
        public Varargs invoke(Varargs args) {
            return super.invoke(args, "err");
        }
    }

    /**
     * This function simply returns a single field table with the ok field set to the specified string for you.
     */
    final static class StatusReply extends Reply {
        @Override
        public Varargs invoke(Varargs args) {
            return super.invoke(args, "ok");
        }
    }

    class SetResp extends VarArgFunction {

        @Override
        public Varargs invoke(Varargs arg) {
            int len = arg.narg();
            if (len == 1) {
                LuaValue value = arg.arg1();
                if (value.isnumber()) {
                    int r = value.toint();
                    if (r == 2) {
                        client.setProtocol(Client.Protocol.RESP2);
                        return LuaValue.NIL;
                    } else if (r == 3) {
                        client.setProtocol(Client.Protocol.RESP3);
                        return LuaValue.NIL;
                    }
                }
                throw new LuaFuncException("RESP version must be 2 or 3.");
            } else {
                throw new LuaFuncException("redis.setresp() requires one argument.");
            }
        }
    }

    static class Reply extends VarArgFunction {
        protected Varargs invoke(Varargs args, String key) {
            LuaValue value = args.arg(1);
            String status = value.strvalue().toString();
            LuaTable table = LuaTable.tableOf();
            table.set(key, status);
            return table;
        }
    }
}


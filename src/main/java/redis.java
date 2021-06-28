import cn.deepmax.redis.engine.NettyClient;
import cn.deepmax.redis.engine.RedisEngine;
import cn.deepmax.redis.engine.RedisEngineHolder;
import cn.deepmax.redis.lua.LuaChannelContext;
import cn.deepmax.redis.lua.LuaFuncException;
import cn.deepmax.redis.lua.RedisLuaConverter;
import cn.deepmax.redis.type.RedisArray;
import cn.deepmax.redis.type.RedisBulkString;
import cn.deepmax.redis.type.RedisType;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.TwoArgFunction;
import org.luaj.vm2.lib.VarArgFunction;

import java.util.Objects;

/**
 * lua bridge for redis
 * require should be same as package name of this Class
 * usage: local redis = require("redis")
 *
 * @author wudi
 * @date 2021/4/30
 */
@Slf4j
public class redis extends TwoArgFunction {

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

        env.set("redis", library);
        return library;
    }

    private RedisEngine engine() {
        return RedisEngineHolder.instance();
    }

    /**
     * https://redis.io/commands/eval
     */
    final class Call extends TheCall {
        @Override
        protected RedisType onError(RedisType resp) {
            throw new LuaFuncException(resp.str());
        }
    }

    final class PCall extends TheCall {
        @Override
        protected RedisType onError(RedisType resp) {
            //the error will convert to LuaTable
            return resp;
        }
    }

    abstract class TheCall extends VarArgFunction {
        @Override
        public Varargs invoke(Varargs args) {
            RedisType msg = RedisLuaConverter.toRedis(args);
            RedisType resp;
            try {
                // a Redis command call will result in an error, redis.call() will 
                // raise a Lua error that in turn will force EVAL to return an error to the command caller,
                // redis.pcall will trap the error and return a Lua table representing the error.
                ChannelHandlerContext ctx = LuaChannelContext.get();
                Objects.requireNonNull(ctx);
                resp = engine().execute(msg, new NettyClient(ctx));
                if (resp.isError()) {
                    resp = onError(resp);
                }
            } catch (LuaFuncException e) {
                throw e;
            } catch (Exception e) {
                resp = new RedisArray();
                resp.add(RedisBulkString.of("ERR " + e.getMessage()));
            }
            return RedisLuaConverter.toLua(resp);
        }

        protected abstract RedisType onError(RedisType resp);
    }

    final static class ErrorReply extends Reply {
        @Override
        public Varargs invoke(Varargs args) {
            return super.invoke(args, "err");
        }
    }

    final static class StatusReply extends Reply {
        @Override
        public Varargs invoke(Varargs args) {
            return super.invoke(args, "ok");
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


import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisEngineHolder;
import cn.deepmax.redis.core.NettyClient;
import cn.deepmax.redis.lua.LuaChannelContext;
import cn.deepmax.redis.lua.LuaFuncException;
import cn.deepmax.redis.lua.RedisLuaConverter;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import lombok.extern.slf4j.Slf4j;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.TwoArgFunction;
import org.luaj.vm2.lib.VarArgFunction;

import java.util.ArrayList;
import java.util.List;
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
        protected RedisMessage onError(RedisMessage resp) {
            if (resp instanceof ErrorRedisMessage) {
                throw new LuaFuncException(((ErrorRedisMessage) resp).content());
            } else {
                //todo
                throw new LuaFuncException("Invalid redis type");
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
            RedisMessage msg = RedisLuaConverter.toRedis(args);;
            RedisMessage resp;
            try {
                // a Redis command call will result in an error, redis.call() will 
                // raise a Lua error that in turn will force EVAL to return an error to the command caller,
                // redis.pcall will trap the error and return a Lua table representing the error.
                ChannelHandlerContext ctx = LuaChannelContext.get();
                Objects.requireNonNull(ctx);
                RedisEngine engine = engine();
                resp = engine.execute(msg, new NettyClient(ctx.channel()));
                //todo blog error
                if (resp instanceof ErrorRedisMessage) {
                    resp = onError(resp);
                }
            } catch (LuaFuncException e) {
                throw e;
            } catch (Exception e) {
                List<RedisMessage> list = new ArrayList<>();
                list.add(FullBulkValueRedisMessage.ofString("ERR " + e.getMessage()));
                resp = new ListRedisMessage(list);
            }
            return RedisLuaConverter.toLua(resp);
        }

        protected abstract RedisMessage onError(RedisMessage resp);
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


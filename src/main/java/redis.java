import cn.deepmax.redis.engine.RedisEngine;
import lombok.extern.slf4j.Slf4j;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.TwoArgFunction;
import org.luaj.vm2.lib.VarArgFunction;

import java.util.UUID;

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
    private final RedisEngine engine = RedisEngine.getInstance();

    /**
     * package load entry
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
        env.set("redis", library);
        return library;
    }

    final class Call extends VarArgFunction {


        public Varargs invoke(Varargs args) {
            log.info("invoke:[{}]", args);
            return action(args.arg(1), args.arg(2));

        }


        public LuaValue action(LuaValue a, LuaValue b) {

            String s = a.strvalue().toString();
            String sb = b.strvalue().toString();
            log.info("[{}][{}]", s, sb);
            engine.set(s.getBytes(), UUID.randomUUID().toString().getBytes());
            String v = "1";
            return LuaValue.valueOf(v);
        }
    }

    //todo
    final class PCall extends VarArgFunction {
        
    }


}

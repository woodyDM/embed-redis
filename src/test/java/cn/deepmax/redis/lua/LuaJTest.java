package cn.deepmax.redis.lua;

import org.junit.Test;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.jse.JsePlatform;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wudi
 * @date 2021/4/30
 */
public class LuaJTest {
    
    String m2 =    "local r = (redis.call(KEYS[1],KEYS[2],1,2,3,4,5,6,7))\n"+
            "print(r)\n" +
            "return r .. '->'.. math.random()  ";
//            "return r .. math.random() .. KEYS[1] .. ARGV[1]  .. KEYS[1] .. ARGV[2] .. ARGV[2]";

    @Test
    public void shouldPrint() {

        Globals globals = JsePlatform.standardGlobals();
        List<String> key = new ArrayList<>();
        key.add("k1");
        key.add("k2");

        List<String> a = new ArrayList<>();
        a.add("bar");
        a.add("foo");
        String script = LuaScript.make(m2, key, a);
        LuaValue lua = globals.load(script);
        LuaValue call= null;
        try {

            call = lua.call();
        } catch (Throwable e) {
            System.out.println("Lua error");
            e.printStackTrace();
        }
        if (call != null) {
            System.out.println("Java: "+call.strvalue().toString());
        }

    }
}

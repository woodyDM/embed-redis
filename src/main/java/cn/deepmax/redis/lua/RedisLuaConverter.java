package cn.deepmax.redis.lua;

import cn.deepmax.redis.type.*;
import org.luaj.vm2.*;

/**
 * @author wudi
 * @date 2021/5/8
 */
public class RedisLuaConverter {

    public static LuaValue toLua(RedisType type) {
        if (type.isNil()) {
            return LuaBoolean.valueOf(false);
        } else if (type.isInteger()) {
            return LuaValue.valueOf(type.value());
        } else if (type.isString()) {
            return LuaString.valueOf(type.str());
        } else if (type.isError()) {
            LuaTable table = LuaTable.tableOf();
            table.set("err", type.str());
            return table;
        } else if (type.isArray()) {
            LuaTable table = LuaTable.tableOf();
            for (int i = 0; i < type.size(); i++) {
                table.set(1 + i, toLua(type.get(i)));
            }
            return table;
        } else {
            throw new LuaError("Unable to convert type " + type.type());
        }
    }

    public static RedisType toRedis(Varargs value) {
        int i = value.narg();
        if (i == 1) {
            LuaValue v = value.arg1();
            switch (v.type()) {
                case LuaValue.TBOOLEAN:
                    boolean flag = v.toboolean();
                    return flag ? new RedisInteger(1) : RedisBulkString.NIL;
                case LuaValue.TNUMBER:
                case LuaValue.TINT:
                    int ivalue = (int) v.todouble();
                    return new RedisInteger(ivalue);
                case LuaValue.TSTRING:
                    return RedisBulkString.of(v.toString());
                case LuaValue.TTABLE:
                    LuaTable table = v.checktable();
                    int len = table.keyCount();
                    if (len == 1) {
                        LuaValue okValue = table.get("ok");
                        if (!okValue.isnil()) {
                            return new RedisString(okValue.toString());
                        }
                        LuaValue errValue = table.get("err");
                        if (!errValue.isnil()) {
                            return new RedisError(errValue.toString());
                        }
                    }
                    //other
                    RedisArray array = new RedisArray();
                    for (LuaValue key : table.keys()) {
                        LuaValue argj = table.get(key.toString());
                        //truncated to the first nil inside the Lua array if any
                        if (argj.isnil()) {
                            break;
                        }
                        array.add(toRedis(argj));
                    }
                    return array;
                default:
                    throw new LuaError("unsupported lua type " + v.typename());
            }
        } else {
            RedisArray ar = new RedisArray();
            for (int j = 0; j < i; j++) {
                LuaValue arg = value.arg(j + 1);
                //truncated to the first nil inside the Lua array if any
                if (arg.type() == LuaValue.TNIL) {
                    break;
                }
                ar.add(toRedis(arg));
            }
            return ar;
        }

    }
}

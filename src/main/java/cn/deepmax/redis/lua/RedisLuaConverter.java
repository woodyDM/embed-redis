package cn.deepmax.redis.lua;

import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.resp3.NullRedisMessage;
import cn.deepmax.redis.type.RedisMessages;
import io.netty.handler.codec.redis.*;
import org.luaj.vm2.*;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wudi
 * @date 2021/5/8
 */
public class RedisLuaConverter {

    //??? where to use
    public static LuaValue toLua(RedisMessage type) {
        if (type instanceof NullRedisMessage || type == FullBulkStringRedisMessage.NULL_INSTANCE
                || type == ArrayRedisMessage.NULL_INSTANCE) {
            return LuaBoolean.valueOf(false);
        } else if (type instanceof IntegerRedisMessage) {
            return LuaValue.valueOf(((IntegerRedisMessage) type).value());
        } else if (type instanceof FullBulkValueRedisMessage) {
            return LuaValue.valueOf(((FullBulkValueRedisMessage) type).bytes());
        } else if (RedisMessages.isStr(type)) {
            return LuaString.valueOf(RedisMessages.getStr(type));
        } else if (RedisMessages.isError(type)) {
            LuaTable table = LuaTable.tableOf();
            table.set("err", RedisMessages.getStr(type));
            return table;
        } else if (type instanceof ArrayRedisMessage) {
            LuaTable table = LuaTable.tableOf();
            ArrayRedisMessage m = (ArrayRedisMessage) type;
            for (int i = 0; i < m.children().size(); i++) {
                table.set(1 + i, toLua(m.children().get(i)));
            }
            return table;
        } else {
            throw new LuaError("Unable to convert type " + type.getClass().getName());
        }
    }

    public static RedisMessage toRedis(Varargs value) {
        int i = value.narg();
        if (i == 1) {
            LuaValue v = value.arg1();
            switch (v.type()) {
                case LuaValue.TBOOLEAN:
                    boolean flag = v.toboolean();
                    return flag ? new IntegerRedisMessage(1) : FullBulkValueRedisMessage.NULL_INSTANCE;
                case LuaValue.TNUMBER:
                case LuaValue.TINT:
                    int ivalue = (int) v.todouble();
                    return new IntegerRedisMessage(ivalue);
                case LuaValue.TSTRING:
                    return FullBulkValueRedisMessage.ofString(v.strvalue().m_bytes);
                case LuaValue.TTABLE:
                    LuaTable table = v.checktable();
                    int len = table.keyCount();
                    if (len == 1) {
                        LuaValue okValue = table.get("ok");
                        if (!okValue.isnil()) {
                            return new SimpleStringRedisMessage(okValue.toString());
                        }
                        LuaValue errValue = table.get("err");
                        if (!errValue.isnil()) {
                            return new ErrorRedisMessage(errValue.toString());
                        }
                    }
                    //other
                    List<RedisMessage> r = new ArrayList<>();
                    for (LuaValue key : table.keys()) {
                        LuaValue argj = table.get(key);
                        //truncated to the first nil inside the Lua array if any
                        if (argj.isnil()) {
                            break;
                        }
                        r.add(toRedis(argj));
                    }
                    return new ListRedisMessage(r);
                default:
                    throw new LuaError("unsupported lua type " + v.typename());
            }
        } else {
            List<RedisMessage> r = new ArrayList<>();

            for (int j = 0; j < i; j++) {
                LuaValue arg = value.arg(j + 1);
                //truncated to the first nil inside the Lua array if any
                if (arg.type() == LuaValue.TNIL) {
                    break;
                }
                r.add(toRedis(arg));
            }
            return new ListRedisMessage(r);
        }
    }
}

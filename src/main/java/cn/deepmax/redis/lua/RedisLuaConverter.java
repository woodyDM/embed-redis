package cn.deepmax.redis.lua;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.Network;
import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.resp3.RedisMessageType;
import cn.deepmax.redis.resp3.*;
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
    /**
     * convert redis types to Lua types
     *
     * @param type
     * @param resp
     * @return
     */
    public static LuaValue toLua(RedisMessage type, Client.Protocol resp) {
        if (type instanceof NullRedisMessage) {
            return LuaValue.NIL;
        } else if (type == FullBulkStringRedisMessage.NULL_INSTANCE || type == ArrayRedisMessage.NULL_INSTANCE) {
            return LuaBoolean.valueOf(false);
        } else if (type instanceof BooleanRedisMessage) {
            return LuaBoolean.valueOf(((BooleanRedisMessage) type).value());
        } else if (type instanceof DoubleRedisMessage) {
            return table("double", ((DoubleRedisMessage) type).getValue());
        } else if (type instanceof BigNumberRedisMessage) {
            return table("big_number", ((BigNumberRedisMessage) type).content());
        } else if (type instanceof SimpleStringRedisMessage) {
            return table("ok", ((SimpleStringRedisMessage) type).content());
        } else if (RedisMessages.isError(type)) {
            return table("err", RedisMessages.getStr(type));
        } else if (type instanceof IntegerRedisMessage) {
            return LuaValue.valueOf((int) ((IntegerRedisMessage) type).value());
        } else if (type instanceof FullBulkValueRedisMessage) {
            return LuaValue.valueOf(((FullBulkValueRedisMessage) type).bytes());
        } else if (type instanceof FullBulkStringRedisMessage) {
            return LuaValue.valueOf(FullBulkValueRedisMessage.bytesOf(((FullBulkStringRedisMessage) type)));
        } else if (type instanceof ArrayRedisMessage) {
            if (type instanceof ListRedisMessage) {
                return ofAgg(RedisMessageType.AGG_ARRAY, ((ListRedisMessage) type).children(), resp);
            } else if (type instanceof SetRedisMessage) {
                return ofAgg(RedisMessageType.AGG_SET, ((SetRedisMessage) type).children(), resp);
            } else {
                return ofAgg(RedisMessageType.AGG_ARRAY, ((ArrayRedisMessage) type).children(), resp);
            }
        } else if (type instanceof MapRedisMessage) {
            return ofAgg(RedisMessageType.AGG_MAP, ((MapRedisMessage) type).children(), resp);
        } else {
            throw new LuaError("Unable to convert type " + type.getClass().getName());
        }
    }

    private static LuaValue ofAgg(RedisMessageType type, List<RedisMessage> msg, Client.Protocol resp) {
        if (resp == Client.Protocol.RESP2 || type == RedisMessageType.AGG_ARRAY) {
            LuaTable table = LuaTable.tableOf();
            for (int i = 0; i < msg.size(); i++) {
                //start at index 1
                table.set(1 + i, toLua(msg.get(i), resp));
            }
            return table;
        } else {
            LuaTable outTable = LuaTable.tableOf();
            LuaTable innerTable = LuaTable.tableOf();
            if (type == RedisMessageType.AGG_MAP) {
                for (int i = 0; i < msg.size() / 2; i += 2) {
                    LuaValue key = toLua(msg.get(i), resp);
                    LuaValue value = toLua(msg.get(i + 1), resp);
                    innerTable.set(key, value);
                }
                outTable.set("map", innerTable);
            } else if (type == RedisMessageType.AGG_SET) {
                for (RedisMessage message : msg) {
                    LuaValue key = toLua(message, resp);
                    innerTable.set(key, LuaBoolean.valueOf(true));
                }
                outTable.set("set", innerTable);
            } else {
                throw new LuaError("lua not support  redis type " + type.name());
            }
            return outTable;
        }
    }

    public static LuaTable table(String key, double value) {
        LuaTable table = LuaTable.tableOf();
        table.set(key, value);
        return table;
    }

    public static LuaTable table(String key, String value) {
        LuaTable table = LuaTable.tableOf();
        table.set(key, value);
        return table;
    }

    /**
     * from Lua to Redis types
     *
     * @param value
     * @return
     */
    public static RedisMessage toRedis(Varargs value, Client.Protocol resp) {
        int argLen = value.narg();
        if (argLen == 1) {
            LuaValue v = value.arg1();
            switch (v.type()) {
                case LuaValue.TNIL:
                    return NullRedisMessage.INSTANCE;
                case LuaValue.TSTRING:
                    return FullBulkValueRedisMessage.ofString(v.strvalue().m_bytes);
                case LuaValue.TBOOLEAN:
                    boolean flag = v.toboolean();
                    if (resp == Client.Protocol.RESP2) {
                        return flag ? Constants.INT_ONE : Network.nullValue(resp);
                    } else {
                        return flag ? BooleanRedisMessage.TRUE : BooleanRedisMessage.FALSE;
                    }
                case LuaValue.TNUMBER:
                case LuaValue.TINT:
                    int ivalue = (int) v.todouble();
                    return new IntegerRedisMessage(ivalue);
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
                        LuaValue doubleValue = table.get("double");
                        if (!doubleValue.isnil()) {
                            return DoubleRedisMessage.ofDouble(doubleValue.todouble());
                        }
                        LuaValue mapValue = table.get("map");
                        if (mapValue.type() == LuaValue.TTABLE) {
                            LuaTable mapTable = mapValue.checktable();
                            List<RedisMessage> msgs = new ArrayList<>();
                            for (LuaValue key : mapTable.keys()) {
                                msgs.add(toRedis(key, resp));
                                msgs.add(toRedis(mapTable.get(key), resp));
                            }
                            return new MapRedisMessage(msgs);
                        }
                        LuaValue setValue = table.get("set");
                        if (setValue.type() == LuaValue.TTABLE) {
                            LuaTable mapTable = setValue.checktable();
                            List<RedisMessage> msgs = new ArrayList<>();
                            for (LuaValue key : mapTable.keys()) {
                                msgs.add(toRedis(key, resp));
                            }
                            return new SetRedisMessage(msgs);
                        }
                    }
                    //normal array
                    return redisOf(table, resp);
                default:
                    throw new LuaError("unsupported lua type " + v.typename());
            }
        } else {
            List<RedisMessage> r = new ArrayList<>();
            for (int j = 0; j < argLen; j++) {
                LuaValue arg = value.arg(j + 1);
                //truncated to the first nil inside the Lua array if any
                if (arg.type() == LuaValue.TNIL) {
                    break;
                }
                r.add(toRedis(arg, resp));
            }
            return new ListRedisMessage(r);
        }
    }

    private static RedisMessage redisOf(LuaTable table, Client.Protocol resp) {
        List<RedisMessage> msgs = new ArrayList<>();
        for (LuaValue key : table.keys()) {
            LuaValue value = table.get(key);
            //truncated to the first nil inside the Lua array if any
            if (value.isnil()) {
                break;
            }
            msgs.add(toRedis(value, resp));
        }
        return new ListRedisMessage(msgs);
    }
}

package cn.deepmax.redis.core.module;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.*;
import cn.deepmax.redis.core.support.ArgsCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.utils.NumberUtils;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author wudi
 * @date 2021/12/23
 */
public class BitMapModule extends BaseModule {
    public BitMapModule() {
        super("bitmap");
        register(new SetBit());
        register(new GetBit());
        register(new BitCount());
        register(new BitOp());
        register(new BitPos());
        register(new BitField());
    }

    public static class SetBit extends ArgsCommand.FourExWith<RString> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            Long offset = msg.getAt(2).val();
            int value = msg.getAt(3).val().intValue();
            RString obj = get(key);
            if (obj == null) {
                obj = new RString(engine.timeProvider());
                engine.getDb(client).set(client, key, obj);
            }
            int i = obj.setBit(offset, value);
            engine.fireChangeEvent(client, key, DbManager.EventType.UPDATE);
            return i == 1 ? Constants.INT_ONE : Constants.INT_ZERO;
        }
    }

    public static class GetBit extends ArgsCommand.ThreeExWith<RString> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            Long offset = msg.getAt(2).val();
            RString obj = get(key);
            if (obj == null) {
                return Constants.INT_ZERO;
            }
            int i = obj.getBit(offset);
            return i == 1 ? Constants.INT_ONE : Constants.INT_ZERO;
        }
    }

    public static class BitCount extends ArgsCommand.TwoWith<RString> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            long start;
            long end;
            if (msg.children().size() == 4) {
                start = msg.getAt(2).val();
                end = msg.getAt(3).val();
            } else if (msg.children().size() == 2) {
                start = 0;
                end = -1;
            } else {
                return Constants.ERR_SYNTAX;
            }
            RString obj = get(key);
            if (obj == null) {
                return Constants.INT_ZERO;
            }
            long c = obj.bitCount((int) start, (int) end);
            return new IntegerRedisMessage(c);
        }
    }

    public static class BitOp extends ArgsCommand.FourWith<RString> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            String op = msg.getAt(1).str().toLowerCase();
            byte[] dest = msg.getAt(2).bytes();
            List<RString> dbStrings = msg.children().stream().skip(3)
                    .map(it -> (FullBulkValueRedisMessage) it)
                    .map(m -> get(m.bytes()))
                    .collect(Collectors.toList());
            RString r;
            switch (op) {
                case "and":
                    r = RString.bitOpAnd(dbStrings);
                    break;
                case "or":
                    r = RString.bitOpOr(dbStrings);
                    break;
                case "xor":
                    r = RString.bitOpXor(dbStrings);
                    break;
                case "not":
                    if (dbStrings.size() != 1) {
                        throw new RedisServerException("ERR BITOP NOT must be called with a single source key.");
                    }
                    r = RString.bitOpNot(dbStrings.get(0));
                    break;
                default:
                    throw new RedisServerException(Constants.ERR_SYNTAX);
            }
            if (r == null) {
                engine.getDb(client).del(client, dest);
                return Constants.INT_ZERO;
            } else {
                engine.getDb(client).set(client, dest, r);
                int length = r.length();
                engine.fireChangeEvent(client, dest, DbManager.EventType.NEW_OR_REPLACE);
                return new IntegerRedisMessage(length);
            }
        }
    }

    public static class BitPos extends ArgsCommand.ThreeWith<RString> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            String bit = msg.getAt(2).str();
            if (!"0".equals(bit) && !"1".equals(bit)) {
                return new ErrorRedisMessage("The bit argument must be 1 or 0.");
            }
            RString obj = get(key);
            if (obj == null) {
                return "0".equals(bit) ? Constants.INT_ZERO : Constants.INT_ONE_NEG;
            }
            int start = msg.children().size() > 3 ? NumberUtils.parse(msg.getAt(3).str()).intValue() : 0;
            int end = msg.children().size() > 4 ? NumberUtils.parse(msg.getAt(4).str()).intValue() : -1;
            boolean endGiven = msg.children().size() == 5;
            long v = obj.bitPos(start, end, Integer.parseInt(bit), endGiven);
            return new IntegerRedisMessage(v);
        }
    }

    //todo
    public static class BitField extends ArgsCommand.ThreeWith<RString> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {

            return new ErrorRedisMessage("not not support");
        }
    }


}

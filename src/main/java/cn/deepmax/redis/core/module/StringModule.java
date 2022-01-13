package cn.deepmax.redis.core.module;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.Network;
import cn.deepmax.redis.api.*;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.support.ArgsCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.utils.NumberUtils;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StringModule extends BaseModule {
    static final int FLAG_EMPTY = 0;
    static final int FLAG_NX = 1;
    static final int FLAG_XX = 1 << 1;
    static final int FLAG_KEEPTTL = 1 << 4;

    public StringModule() {
        super("string");
        register(new Get());
        register(new GetDel());
        register(new GetEx());
        register(new Set());
        register(new SetNx());
        register(new SetEx());
        register(new PSetEx());
        register(new Append());
        register(new Incr());
        register(new IncrBy());
        register(new Decr());
        register(new DecrBy());
        register(new Strlen());
        register(new GetRange("getrange"));
        register(new GetRange("substr"));
        register(new SetRange());
        register(new GetSet());
        register(new MGet());
        register(new MSet());
        register(new MSetNx());
        register(new IncrByFloat());
    }

    static void multiSet(ListRedisMessage msg, Client client, RedisEngine engine) {
        int len = msg.children().size();
        List<Key> keys = new ArrayList<>();
        List<RedisObject> obj = new ArrayList<>();
        for (int i = 1; i < len; i += 2) {
            byte[] key = msg.getAt(i).bytes();
            keys.add(new Key(key));
            byte[] value = msg.getAt(i + 1).bytes();
            RString newValue = new RString(engine.timeProvider(), value);
            obj.add(newValue);
        }
        engine.getDb(client).multiSet(client, keys, obj);
    }

    static RedisMessage genericSet(RedisEngine engine, Client client,
                                   byte[] key, byte[] value, Optional<Long> px, int flag,
                                   Supplier<RedisMessage> successReply, Supplier<RedisMessage> emptyReply) {
        //checks
        if (((flag & FLAG_XX) != 0) && ((flag & FLAG_NX) != 0)) {
            throw Constants.EX_SYNTAX;
        }
        boolean keep = (flag & FLAG_KEEPTTL) != 0;
        if (px.isPresent() && keep) {
            throw Constants.EX_SYNTAX;
        }
        //set
        RedisObject r = engine.getDb(client).get(client, key);
        if (r != null && !(r instanceof RString)) {
            throw new RedisServerException(Constants.ERR_TYPE);
        }
        RString old = (RString) r;
        if (old == null && ((flag & FLAG_XX) != 0) || old != null && ((flag & FLAG_NX) != 0)) {
            return emptyReply.get();
        }
        RString newV = new RString(engine.timeProvider(), value);
        if (keep && old != null && old.expireTime() != null) {
            newV.expireAt(old.expireTime());
        }
        px.ifPresent(newV::pexpire);
        engine.getDb(client).set(client, key, newV);
        return successReply.get();
    }

    static RedisMessage genericIncre(RedisEngine engine, Client client,
                                     byte[] key, long number) {
        RedisObject r = engine.getDb(client).get(client, key);
        if (r != null && !(r instanceof RString)) {
            throw new RedisServerException(Constants.ERR_TYPE);
        }
        if (r == null) {
            RString old = new RString(engine.timeProvider(), Long.valueOf(number).toString().getBytes(StandardCharsets.UTF_8));
            engine.getDb(client).set(client, key, old);
            return new IntegerRedisMessage(number);
        } else {
            RString old = (RString) r;
            Optional<Long> oldV = NumberUtils.parseO(old.str());
            if (!oldV.isPresent()) {
                throw new RedisServerException(Constants.ERR_SYNTAX_NUMBER);
            }
            long v = oldV.get() + number;
            old.setS(Long.valueOf(v).toString().getBytes(StandardCharsets.UTF_8));
            engine.fireChangeEvent(client, key, DbManager.EventType.UPDATE);
            return new IntegerRedisMessage(v);
        }
    }

    public static class Get extends ArgsCommand.TwoExWith<RString> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            RString s = get(key);
            if (s == null) {
                return Network.nullValue(client);
            }
            return FullBulkValueRedisMessage.ofString(s.getS());
        }
    }

    public static class GetDel extends ArgsCommand.TwoExWith<RString> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            RString s = get(key);
            if (s == null) {
                return Network.nullValue(client);
            }
            engine.getDb(client).del(client, key);
            return FullBulkValueRedisMessage.ofString(s.getS());
        }
    }

    //GETEX key [EX seconds|PX milliseconds|EXAT unix-time|PXAT unix-time|PERSIST]
    public static class GetEx extends ArgsCommand.TwoWith<RString> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            Optional<Long> ex = ArgParser.parseLongArg(msg, "ex", 2);
            Optional<Long> px = ArgParser.parseLongArg(msg, "px", 2);
            Optional<Long> exat = ArgParser.parseLongArg(msg, "exat", 2);
            Optional<Long> pxat = ArgParser.parseLongArg(msg, "pxat", 2);
            boolean persist = ArgParser.parseFlag(msg, "persist", 2);

            if (persist && (ex.isPresent() || px.isPresent() || exat.isPresent() || pxat.isPresent())) {
                return Constants.ERR_SYNTAX;
            }
            long c = Stream.of(ex, px, exat, pxat).filter(Optional::isPresent).count();
            if (c > 1) return Constants.ERR_SYNTAX;
            boolean neg = Stream.of(ex, px, exat, pxat).filter(Optional::isPresent).anyMatch(i -> i.get() <= 0);
            if (neg) return new ErrorRedisMessage("invalid expire time in getex");

            RString s = get(key);
            if (s == null) {
                return Network.nullValue(client);
            }
            //no expire and no persist
            if (persist && s.expireTime() != null) {
                s.expireAt(null);
                engine.fireChangeEvent(client, key, DbManager.EventType.UPDATE);
            } else if (c == 1) {
                LocalDateTime now = engine.timeProvider().now();
                LocalDateTime unix = engine.timeProvider().now();
                if (ex.isPresent()) unix = unix.plusSeconds(ex.get());
                if (px.isPresent()) unix = unix.plusNanos(px.get() * 1000_000);
                if (exat.isPresent()) unix = new Timestamp(exat.get() * 1000).toLocalDateTime();
                if (pxat.isPresent()) unix = new Timestamp(pxat.get()).toLocalDateTime();
                boolean expire = unix.isBefore(now);
                if (expire) {
                    engine.getDb(client).del(client, key);
                } else {
                    s.expireAt(unix);
                    engine.fireChangeEvent(client, key, DbManager.EventType.UPDATE);
                }
            }
            return FullBulkValueRedisMessage.ofString(s.getS());
        }
    }

    /**
     * SET key value [NX] [XX] [KEEPTTL] [EX <seconds>] [PX <milliseconds>]
     */
    public static class Set extends ArgsCommand.ThreeWith<RString> {

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            byte[] value = msg.getAt(2).bytes();
            Optional<Long> exp = parseExpire(msg);
            int flag = FLAG_EMPTY;
            if (parseFlag(msg, "nx")) flag |= FLAG_NX;
            if (parseFlag(msg, "xx")) flag |= FLAG_XX;
            if (parseFlag(msg, "KEEPTTL")) flag |= FLAG_KEEPTTL;
            return genericSet(engine, client, key, value, exp, flag, () -> OK, () -> Network.nullValue(client));
        }

        public Optional<Long> parseExpire(ListRedisMessage msg) {
            Optional<Long> ex = parseExpire(msg, "ex");
            Optional<Long> px = parseExpire(msg, "px");
            if (ex.isPresent() && px.isPresent()) {
                throw Constants.EX_SYNTAX;
            }
            return ex.map(v -> Optional.of(v * 1000L)).orElse(px);
        }

        public Optional<Long> parseExpire(ListRedisMessage msg, String ex) {
            return ArgParser.parseLongArg(msg, ex);
        }

        public boolean parseFlag(ListRedisMessage msg, String ex) {
            return ArgParser.parseFlag(msg, ex);
        }
    }

    public static class SetNx extends ArgsCommand.ThreeExWith<RString> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            byte[] value = msg.getAt(2).bytes();
            return genericSet(engine, client, key, value, Optional.empty(), FLAG_NX, () -> Constants.INT_ONE, () ->
                    Constants.INT_ZERO);
        }
    }

    public static class SetEx extends ArgsCommand.FourWith<RString> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            Long seconds = msg.getAt(2).val();
            byte[] value = msg.getAt(3).bytes();
            return genericSet(engine, client, key, value, Optional.of(seconds * 1000L), FLAG_EMPTY, () -> OK, () ->
                    Network.nullValue(client));
        }
    }

    public static class PSetEx extends ArgsCommand.FourWith<RString> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            Long seconds = msg.getAt(2).val();
            byte[] value = msg.getAt(3).bytes();
            return genericSet(engine, client, key, value, Optional.of(seconds), FLAG_EMPTY, () -> OK, () ->
                    Network.nullValue(client));
        }
    }

    public static class Append extends ArgsCommand.ThreeExWith<RString> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            byte[] value = msg.getAt(2).bytes();
            RString old = get(key);
            if (old == null) {
                old = new RString(engine.timeProvider(), value);
                engine.getDb(client).set(client, key, old);
            } else {
                old.append(value);
                engine.fireChangeEvent(client, key, DbManager.EventType.UPDATE);
            }
            return new IntegerRedisMessage(old.length());
        }
    }

    public static class Incr extends ArgsCommand.TwoExWith<RString> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            return genericIncre(engine, client, key, 1L);
        }
    }

    public static class IncrBy extends ArgsCommand.ThreeExWith<RString> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            Long num = msg.getAt(2).val();
            return genericIncre(engine, client, key, num);
        }
    }

    public static class Decr extends ArgsCommand.TwoExWith<RString> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            return genericIncre(engine, client, key, -1L);
        }
    }

    public static class DecrBy extends ArgsCommand.ThreeExWith<RString> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            Long num = msg.getAt(2).val();
            return genericIncre(engine, client, key, -num);
        }
    }


    public static class Strlen extends ArgsCommand.TwoExWith<RString> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            RString obj = get(key);
            if (obj == null) {
                return Constants.INT_ZERO;
            } else {
                return new IntegerRedisMessage(obj.length());
            }
        }
    }

    public static class GetRange extends ArgsCommand.FourExWith<RString> {
        private final String name;

        public GetRange(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            RString obj = get(key);
            if (obj == null) {
                return FullBulkStringRedisMessage.EMPTY_INSTANCE;
            }
            long start = msg.getAt(2).val();
            long end = msg.getAt(3).val();
            byte[] range = obj.getRange((int) start, (int) end);
            return FullBulkValueRedisMessage.ofString(range);
        }
    }

    public static class SetRange extends ArgsCommand.FourExWith<RString> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            RString obj = get(key);
            long offset = msg.getAt(2).val();
            byte[] value = msg.getAt(3).bytes();
            if (obj == null) {
                RString s = RString.of(engine.timeProvider(), value, (int) offset);
                engine.getDb(client).set(client, key, s);
                return new IntegerRedisMessage(s.length());
            } else {
                obj.setRange(value, (int) offset);
                engine.fireChangeEvent(client, key, DbManager.EventType.UPDATE);
                return new IntegerRedisMessage(obj.length());
            }
        }
    }

    public static class GetSet extends ArgsCommand.ThreeExWith<RString> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            byte[] value = msg.getAt(2).bytes();
            RString obj = get(key);
            engine.getDb(client).set(client, key, new RString(engine.timeProvider(), value));
            if (obj == null) {
                return Network.nullValue(client);
            } else {
                return FullBulkValueRedisMessage.ofString(obj.getS());
            }
        }
    }

    public static class MGet extends ArgsCommand.Two {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            List<RedisMessage> result = msg.children().stream().skip(1)
                    .map(k -> (FullBulkValueRedisMessage) k)
                    .map(k -> engine.getDb(client).get(client, k.bytes()))
                    .map(v -> {
                        if (v instanceof RString) {
                            return FullBulkValueRedisMessage.ofString(((RString) v).getS());
                        } else {
                            return Network.nullValue(client);
                        }
                    }).collect(Collectors.toList());
            return new ListRedisMessage(result);
        }
    }

    public static class MSet extends ArgsCommand.Three {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            int len = msg.children().size();
            if (len % 2 != 1) {
                throw new RedisServerException("ERR wrong number of arguments for MSET");
            }
            multiSet(msg, client, engine);
            return OK;
        }
    }

    public static class MSetNx extends ArgsCommand.Three {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            int len = msg.children().size();
            if (len % 2 != 1) {
                throw new RedisServerException("ERR wrong number of arguments for MSET");
            }
            for (int i = 1; i < len; i += 2) {
                byte[] key = msg.getAt(i).bytes();
                RedisObject exist = engine.getDb(client).get(client, key);
                if (exist != null) {
                    return Constants.INT_ZERO;
                }
            }
            multiSet(msg, client, engine);
            return Constants.INT_ONE;
        }
    }

    public static class IncrByFloat extends ArgsCommand.ThreeExWith<RString> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            String value = msg.getAt(2).str();
            Double d = NumberUtils.parseDouble(value);
            if (d.isInfinite() || d.isNaN()) {
                throw new RedisServerException("increment would produce NaN or Infinity");
            }
            RString exist = get(key);
            if (exist == null) {
                String s = NumberUtils.formatDouble(d);
                RString obj = new RString(engine.timeProvider(), s.getBytes(StandardCharsets.UTF_8));
                engine.getDb(client).set(client, key, obj);
                return FullBulkValueRedisMessage.ofString(s);
            } else {
                Double newD = NumberUtils.parseDouble(exist.str()) + d;
                if (newD.isInfinite() || newD.isNaN()) {
                    throw new RedisServerException("increment would produce NaN or Infinity");
                }
                String s = NumberUtils.formatDouble(newD);
                exist.setS(s.getBytes(StandardCharsets.UTF_8));
                engine.fireChangeEvent(client, key, DbManager.EventType.UPDATE);
                return FullBulkValueRedisMessage.ofString(s);
            }
        }
    }

}

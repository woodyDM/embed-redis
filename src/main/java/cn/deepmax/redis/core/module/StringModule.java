package cn.deepmax.redis.core.module;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisObject;
import cn.deepmax.redis.api.RedisServerException;
import cn.deepmax.redis.core.support.ArgsCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.utils.NumberUtils;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.Optional;
import java.util.function.Supplier;

public class StringModule extends BaseModule {
    public StringModule() {
        super("string");
        register(new Get());
        register(new Set());
        register(new SetNx());
        register(new SetEx());
        register(new PSetEx());
        register(new Append());
    }

    public static class Get extends ArgsCommand.TwoWith<RString> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Redis.Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            RedisObject obj = engine.getDb(client).get(key);
            if (obj == null) {
                return FullBulkStringRedisMessage.NULL_INSTANCE;
            }
            RString s = get(key);
            return new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(s.getS()));
        }
    }

    /**
     * SET key value [NX] [XX] [KEEPTTL] [EX <seconds>] [PX <milliseconds>]
     */
    public static class Set extends ArgsCommand.ThreeWith<RString> {

        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Redis.Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            byte[] value = msg.getAt(2).bytes();
            Optional<Long> exp = parseExpire(msg);
            int flag = FLAG_EMPTY;
            if (parseFlag(msg, "nx")) flag |= FLAG_NX;
            if (parseFlag(msg, "xx")) flag |= FLAG_XX;
            if (parseFlag(msg, "KEEPTTL")) flag |= FLAG_KEEPTTL;
            return genericSet(engine, client, key, value, exp, flag, () -> OK, () -> FullBulkStringRedisMessage.NULL_INSTANCE);
        }

        Optional<Long> parseExpire(ListRedisMessage msg) {
            Optional<Long> ex = parseExpire(msg, "ex");
            Optional<Long> px = parseExpire(msg, "px");
            if (ex.isPresent() && px.isPresent()) {
                throw Constants.EX_SYNTAX;
            }
            return ex.map(v -> Optional.of(v * 1000L)).orElse(px);
        }

        Optional<Long> parseExpire(ListRedisMessage msg, String ex) {
            int len = msg.children().size();
            for (int i = 3; i < len; i++) {
                String key = msg.getAt(i).str();
                if (ex.toLowerCase().equals(key.toLowerCase())) {
                    if (i + 1 < len) {
                        Long v = NumberUtils.parse(msg.getAt(i + 1).str());
                        return Optional.of(v);
                    } else {
                        throw new RedisServerException(Constants.ERR_SYNTAX);
                    }
                }
            }
            return Optional.empty();
        }

        boolean parseFlag(ListRedisMessage msg, String ex) {
            int len = msg.children().size();
            for (int i = 3; i < len; i++) {
                String key = msg.getAt(i).str();
                if (ex.toLowerCase().equals(key.toLowerCase())) {
                    return true;
                }
            }
            return false;
        }
    }

    public static class SetNx extends ArgsCommand.ThreeWith<RString> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Redis.Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            byte[] value = msg.getAt(2).bytes();
            return genericSet(engine, client, key, value, Optional.empty(), FLAG_NX, () -> Constants.INT_ONE, () ->
                    Constants.INT_ZERO);
        }
    }

    public static class SetEx extends ArgsCommand.FourWith<RString> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Redis.Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            Long seconds = NumberUtils.parse(msg.getAt(2).str());
            byte[] value = msg.getAt(3).bytes();
            return genericSet(engine, client, key, value, Optional.of(seconds * 1000L), FLAG_EMPTY, () -> OK, () ->
                    FullBulkStringRedisMessage.NULL_INSTANCE);
        }
    }

    public static class PSetEx extends ArgsCommand.FourWith<RString> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Redis.Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            Long seconds = NumberUtils.parse(msg.getAt(2).str());
            byte[] value = msg.getAt(3).bytes();
            return genericSet(engine, client, key, value, Optional.of(seconds), FLAG_EMPTY, () -> OK, () ->
                    FullBulkStringRedisMessage.NULL_INSTANCE);
        }
    }

    public static class Append extends ArgsCommand.ThreeWith<RString> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Redis.Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            byte[] value = msg.getAt(2).bytes();
            RString old = get(key);
            if (old == null) {
                old = new RString(engine.timeProvider(), value);
            } else {
                old = old.append(value);
            }
            engine.getDb(client).set(key, old);
            return new IntegerRedisMessage(old.getS().length);
        }
    }

    static final int FLAG_EMPTY = 0;
    static final int FLAG_NX = 1 << 0;
    static final int FLAG_XX = 1 << 1;
    static final int FLAG_KEEPTTL = 1 << 4;

    static RedisMessage genericSet(RedisEngine engine, Redis.Client client,
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
        RedisObject r = engine.getDb(client).get(key);
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
        engine.getDb(client).set(key, newV);
        return successReply.get();
    }

}

package cn.deepmax.redis.netty;

import cn.deepmax.redis.type.*;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.redis.*;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.List;

@ChannelHandler.Sharable
@Slf4j
public class RedisTypeDecoder extends MessageToMessageDecoder<RedisMessage> {

    @Override
    protected void decode(ChannelHandlerContext ctx, RedisMessage msg, List<Object> out) throws Exception {
        RedisType result = decode(msg);
        if (result != null) {
            out.add(result);
        }
    }

    private RedisType decode(RedisMessage msg) {
        if (msg instanceof ArrayRedisMessage) {
            return decodeArray((ArrayRedisMessage) msg);
        } else if (msg instanceof SimpleStringRedisMessage) {
            return decodeString((SimpleStringRedisMessage) msg);
        } else if (msg instanceof FullBulkStringRedisMessage) {
            return decodeBulkString((FullBulkStringRedisMessage) msg);
        } else if (msg instanceof ErrorRedisMessage) {
            return decodeError((ErrorRedisMessage) msg);
        } else if (msg instanceof IntegerRedisMessage) {
            return decodeInteger((IntegerRedisMessage) msg);
        } else {
            log.error("Unsupported msg type : {}", msg.getClass().getSimpleName());
            return null;
        }
    }

    private RedisBulkString decodeBulkString(FullBulkStringRedisMessage msg) {
        String value = msg.content().toString(StandardCharsets.UTF_8);
        return RedisBulkString.valueOf(value);
    }

    private RedisError decodeError(ErrorRedisMessage msg) {
        return new RedisError(msg.content());
    }

    private RedisInteger decodeInteger(IntegerRedisMessage msg) {
        return new RedisInteger(msg.value());
    }

    private RedisString decodeString(SimpleStringRedisMessage msg) {
        return new RedisString(msg.content());
    }

    private RedisArray decodeArray(ArrayRedisMessage m) {
        if (m.isNull()) {
            return RedisArray.NIL;
        } else {
            RedisArray array = new RedisArray();
            for (RedisMessage child : m.children()) {
                RedisType type = decode(child);
                if (type != null) {
                    array.add(type);
                }
            }
            return array;
        }
    }
}

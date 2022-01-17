package cn.deepmax.redis.type;

import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.RedisMessageType;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;

import java.nio.charset.StandardCharsets;

/**
 * @author wudi
 * @date 2021/12/15
 */
public class RedisMessages {
    /**
     * error has two types
     *
     * @param msg
     * @return
     */
    public static boolean isError(RedisMessage msg) {
        boolean ok = msg instanceof ErrorRedisMessage;
        if (ok) {
            return true;
        }
        if (msg instanceof FullBulkValueRedisMessage) {
            return ((FullBulkValueRedisMessage) msg).type() == RedisMessageType.BLOG_ERROR;
        }
        return false;
    }

    public static boolean isStr(RedisMessage msg) {
        if (msg instanceof SimpleStringRedisMessage) {
            return true;
        }
        if (msg instanceof FullBulkValueRedisMessage) {
            return ((FullBulkValueRedisMessage) msg).type() == RedisMessageType.BLOG_STRING;
        }
        return msg instanceof FullBulkStringRedisMessage;
    }

    public static String getStr(RedisMessage msg) {
        if (msg instanceof SimpleStringRedisMessage) {
            return ((SimpleStringRedisMessage) msg).content();
        } else if (msg instanceof ErrorRedisMessage) {
            return ((ErrorRedisMessage) msg).content();
        } else if (msg instanceof FullBulkValueRedisMessage) {
            return ((FullBulkValueRedisMessage) msg).content().toString(StandardCharsets.UTF_8);
        } else if (msg instanceof FullBulkStringRedisMessage) {
            return ((FullBulkStringRedisMessage) msg).content().toString(StandardCharsets.UTF_8);
        } else {
            throw new UnsupportedOperationException("Printer not suppport " + msg.getClass().getName());
        }
    }

    public static boolean isNumber(RedisMessage message) {
        return false;
    }
}

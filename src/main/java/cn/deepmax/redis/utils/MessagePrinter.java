package cn.deepmax.redis.utils;

import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.NullRedisMessage;
import cn.deepmax.redis.type.CallbackRedisMessage;
import cn.deepmax.redis.type.CompositeRedisMessage;
import cn.deepmax.redis.type.RedisMessages;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wudi
 * @date 2021/12/28
 */
@Slf4j
public class MessagePrinter {
    
    private final static AtomicLong requestCounter = new AtomicLong();
    private final static AtomicLong responseCounter = new AtomicLong();

    public static void requestStart() {
        if (log.isDebugEnabled()) {
            log.debug("[{}]Request", requestCounter.getAndIncrement());
        }
    }

    public static void responseStart() {
        if (log.isDebugEnabled()) {
            log.debug("[{}]Response", responseCounter.getAndIncrement());
        }
    }

    public static void printMessage(RedisMessage msg, boolean queued) {
        if (log.isDebugEnabled()) {
            doPrint(msg, 0, false, queued);
        }
    }

    private static void doPrint(RedisMessage msg, int depth, boolean isLast, boolean queued) {
        if (msg == null) {
            return;
        }
        if (msg instanceof CallbackRedisMessage) {
            msg = ((CallbackRedisMessage) msg).unwrap();
        }
        String word = "";
        String space = String.join("", Collections.nCopies(depth, " "));
        if (RedisMessages.isError(msg)) {
            word = RedisMessages.getStr(msg);
        } else if (msg == FullBulkValueRedisMessage.NULL_INSTANCE || msg == NullRedisMessage.INSTANCE) {
            word = "Null Message Instance";
        } else if (RedisMessages.isStr(msg)) {
            if (msg instanceof FullBulkValueRedisMessage) {
                word = msg.toString();
            } else {
                word = RedisMessages.getStr(msg);
            }
        } else if (msg instanceof IntegerRedisMessage) {
            word = "" + ((IntegerRedisMessage) msg).value();
        } else if (msg instanceof ArrayRedisMessage) {
            log.debug("{}-[{}] ", space,
                    msg.getClass().getSimpleName());
            List<RedisMessage> children = ((ArrayRedisMessage) msg).children();
            for (int i = 0; i < children.size(); i++) {
                doPrint(children.get(i), depth + 1, i == children.size() - 1, queued);
            }
            return;
        } else if (msg instanceof CompositeRedisMessage) {
            log.debug("{}-[{}] ", space,
                    msg.getClass().getSimpleName());
            List<RedisMessage> children = ((CompositeRedisMessage) msg).children();
            for (int i = 0; i < children.size(); i++) {
                doPrint(children.get(i), depth + 1, i == children.size() - 1, queued);
            }
            return;
        } else {
            throw new CodecException("unknown message type: " + msg);
        }
        String corner = (isLast ? "└" : "├");
        String q = queued ? "[Q]" : "";
        log.debug("{}{}-[{}{}]{}", corner, space, q,
                msg.getClass().getSimpleName(), word);
    }
}
package cn.deepmax.redis.utils;

import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.resp3.AbstractMapRedisMessage;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.NullRedisMessage;
import cn.deepmax.redis.type.CallbackRedisMessage;
import cn.deepmax.redis.type.CompositeRedisMessage;
import cn.deepmax.redis.type.RedisMessages;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author wudi
 * @date 2021/12/28
 */
@Slf4j
public class MessagePrinter {
    
    public static void requestStart(Client client, long seq) {
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}]Request", seq, clientInfo(client));
        }
    }

    public static void responseStart(Client client, long seq) {
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}]Response", seq, clientInfo(client));
        }
    }

    private static String clientInfo(Client client) {
        if (client.resp() == Client.Protocol.RESP2) {
            return "V2";
        } else {
            return "V3";
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
        } else if (msg instanceof AbstractMapRedisMessage) {
            log.debug("{}-[{}] ", space,
                    msg.getClass().getSimpleName());
            Map<RedisMessage, RedisMessage> children = ((AbstractMapRedisMessage) msg).data();
            children.forEach((k, v) -> {
                doPrint(k, depth + 1, false, queued);
                doPrint(v, depth + 1, false, queued);
            });
            return;
        } else {
            throw new IllegalStateException("can't print msg " + msg.getClass().getName());
        }
        String corner = (isLast ? "└" : "├");
        String q = queued ? "[Q]" : "";
        log.debug("{}{}-[{}{}]{}", corner, space, q,
                msg.getClass().getSimpleName(), word);
    }
}

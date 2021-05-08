package cn.deepmax.redis.engine;

import cn.deepmax.redis.type.RedisError;
import cn.deepmax.redis.type.RedisType;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CodecException;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wudi
 * @date 2021/5/8
 */
@Slf4j
public class RedisExecutor {
    private final AtomicLong requestCounter = new AtomicLong();
    private final AtomicLong responseCounter = new AtomicLong();

    public RedisType execute(RedisType type, RedisEngine engine) {
        return execute(type, engine, null);
    }

    public RedisType execute(RedisType type, RedisEngine engine, ChannelHandlerContext ctx) {
        boolean net = ctx != null;
        log.info("[{}][{}]Request", requestCounter.getAndIncrement(), net);
        printRedisMessage(type);
        RedisCommand command = engine.getCommand(type);
        RedisType response;
        try {
            response = command.response(type, ctx, engine);
        } catch (RedisParamException e) {
            response = new RedisError(e.getMessage());
        } catch (Exception e) {
            response = new RedisError("ERR internal server error");
            log.error("Embed server error, may be bug! ", e);
        }
        log.info("[{}][{}]Response", responseCounter.getAndIncrement(), net);
        printRedisMessage(response);
        return response;
    }
    
    private void printRedisMessage(RedisType msg) {
        doPrint(msg, 0);
    }

    private void doPrint(RedisType msg, int depth) {
        String word = "";

        String space = String.join("", Collections.nCopies(depth, " "));
        if (msg.isString()) {
            word = msg.str();
        } else if (msg.isError()) {
            word = msg.str();
        } else if (msg.isInteger()) {
            word = "" + msg.value();
        } else if (msg.isArray()) {
            log.info("{}-[{}] ", space,
                    msg.getClass().getSimpleName());
            for (RedisType child : msg.children()) {
                doPrint(child, depth + 1);
            }
            return;
        } else {
            throw new CodecException("unknown message type: " + msg);
        }
        log.info("{}├-[{}]{}", space,
                msg.getClass().getSimpleName(), word);

    }
}


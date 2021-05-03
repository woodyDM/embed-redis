package cn.deepmax.redis.netty;

import cn.deepmax.redis.engine.RedisCommand;
import cn.deepmax.redis.engine.RedisEngine;
import cn.deepmax.redis.type.RedisType;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.CodecException;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An example Redis client handler. This handler read input from STDIN and write output to STDOUT.
 */
@Slf4j
public class RedisServerHandler extends ChannelInboundHandlerAdapter {
    private final AtomicLong c = new AtomicLong();
    private final AtomicLong r = new AtomicLong();

    private RedisEngine engine;

    public RedisServerHandler(RedisEngine engine) {
        this.engine = engine;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        RedisType type = (RedisType) msg;
        log.info("[{}]Request", c.getAndIncrement());
        printRedisMessage(type);
        RedisCommand command = engine.getCommand(type);
        RedisType response = command.response(type, ctx);
        log.info("[{}]Response", r.getAndIncrement());
        printRedisMessage(response);
        ctx.writeAndFlush(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        System.err.print("exceptionCaught: ");
        cause.printStackTrace(System.err);
        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("Exit channel ");
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
            log.info("{}-- [{}] ", space,
                    msg.getClass().getSimpleName());
            for (RedisType child : msg.children()) {
                doPrint(child, depth + 1);
            }
            return;
        } else {
            throw new CodecException("unknown message type: " + msg);
        }

        log.info("{}|- [{}]{}", space,
                msg.getClass().getSimpleName(), word);

    }

}

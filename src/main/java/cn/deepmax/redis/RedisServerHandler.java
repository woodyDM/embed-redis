package cn.deepmax.redis;

import cn.deepmax.redis.command.RedisCommand;
import cn.deepmax.redis.engine.RedisEngine;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.redis.*;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
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
        RedisMessage redisMessage = (RedisMessage) msg;
        log.info("[{}]Request", c.getAndIncrement());
        printRedisMessage(redisMessage);
        RedisCommand command = RedisCommandFactory.command(redisMessage);
        RedisMessage response = command.response(this.engine, redisMessage, ctx);
        log.info("[{}]Response", r.getAndIncrement());
        printRedisMessage(response);
        ctx.writeAndFlush(response);
        ReferenceCountUtil.release(redisMessage);
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

    private void printRedisMessage(RedisMessage msg) {

        doPrint(msg, 0);
    }

    private void doPrint(RedisMessage msg, int depth) {
        String word = "";

        String space = String.join("", Collections.nCopies(depth, " "));
        if (msg instanceof SimpleStringRedisMessage) {
            word = ((SimpleStringRedisMessage) msg).content();
        } else if (msg instanceof ErrorRedisMessage) {
            word = (((ErrorRedisMessage) msg).content());
        } else if (msg instanceof IntegerRedisMessage) {
            word = "" + (((IntegerRedisMessage) msg).value());
        } else if (msg instanceof FullBulkStringRedisMessage) {
            word = "" + (getString((FullBulkStringRedisMessage) msg));
        } else if (msg instanceof ArrayRedisMessage) {
            log.info("{}-- [{}] ", space,
                    msg.getClass().getSimpleName());
            for (RedisMessage child : ((ArrayRedisMessage) msg).children()) {
                doPrint(child, depth + 1);
            }
            return;
        } else {
            throw new CodecException("unknown message type: " + msg);
        }

        log.info("{}|- [{}]{}", space,
                msg.getClass().getSimpleName(), word);

    }

    private static String getString(FullBulkStringRedisMessage msg) {
        if (msg.isNull()) {
            return "(null)";
        }
        return msg.content().toString(CharsetUtil.UTF_8);
    }
}

package cn.deepmax.redis.netty;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.engine.*;
import cn.deepmax.redis.engine.module.AuthModule;
import cn.deepmax.redis.type.RedisType;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

/**
 */
@Slf4j
public class RedisServerHandler extends ChannelInboundHandlerAdapter {

    private final RedisEngine engine;

    public RedisServerHandler(RedisEngine engine) {
        this.engine = engine;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        RedisType type = (RedisType) msg;
        AuthManager auth = engine.authManager();
        RedisExecutor exec = engine.executor();
        RedisCommand command = exec.get(type, engine, ctx);
        if (auth.needAuth() && !auth.alreadyAuth(ctx.channel())) {
            command = wrapAuth(command);
        }
        RedisType response = exec.execute(command, type, engine, ctx);
        ctx.writeAndFlush(response);
    }

    /**
     * wrap for auth
     * @param command
     * @return
     */
    private RedisCommand wrapAuth(RedisCommand command) {
        return ((type, ctx, en) -> {
            if (command instanceof AuthModule.Auth ||
                    command == CommandManager.UNKNOWN_COMMAND ||
                    en.authManager().alreadyAuth(ctx.channel())) {
                return command.response(type, ctx, en);
            } else {
                return Constants.NO_AUTH_ERROR;
            }
        });
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
        engine.pubsub().quit(ctx.channel());
    }
    
}

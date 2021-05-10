package cn.deepmax.redis.engine.module;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.engine.AuthManager;
import cn.deepmax.redis.engine.RedisCommand;
import cn.deepmax.redis.engine.RedisEngine;
import cn.deepmax.redis.engine.support.BaseModule;
import cn.deepmax.redis.type.RedisError;
import cn.deepmax.redis.type.RedisType;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author wudi
 * @date 2021/5/10
 */
public class AuthModule extends BaseModule {
    public AuthModule( ) {
        super("auth");
        register(new Auth());
    }

    public static class Auth implements RedisCommand {
        @Override
        public RedisType response(RedisType type, ChannelHandlerContext ctx, RedisEngine engine) {
            //todo ACL?
            if (type.size() > 2) {
                return new RedisError("Redis6 ACL is not supported");
            }
            AuthManager auth = engine.authManager();
            String userAuth = type.get(1).str();
            if (!auth.needAuth() || auth.tryAuth(userAuth, ctx.channel())) {
                return Constants.OK;
            }else{
                return new RedisError("WRONGPASS invalid username-password pair");
            }
        }
    }
    
}

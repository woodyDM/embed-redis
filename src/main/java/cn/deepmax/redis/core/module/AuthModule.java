package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.AuthManager;
import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.RedisCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

/**
 * @author wudi
 * @date 2021/5/10
 */
public class AuthModule extends BaseModule {
    public AuthModule() {
        super("auth");
        register(new Auth());
    }

    public static class Auth implements RedisCommand {
        @Override
        public RedisMessage response(RedisMessage type, Redis.Client client, RedisEngine engine) {
            ListRedisMessage msg = cast(type);

            if (msg.children().size() > 2) {
                return new ErrorRedisMessage("Redis6 ACL is not supported");
            }
            AuthManager auth = engine.authManager();
            String userAuth = msg.getAt(1).str();
            if (!auth.needAuth() || auth.tryAuth(userAuth, client)) {
                return OK;
            } else {
                return new ErrorRedisMessage("WRONGPASS invalid username-password pair");
            }
        }
    }

}

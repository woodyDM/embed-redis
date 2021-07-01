package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.AuthManager;
import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.core.RedisCommand;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.type.RedisError;
import cn.deepmax.redis.type.RedisType;

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
        public RedisType response(RedisType type, Redis.Client client, RedisEngine engine) {
            //todo ACL?
            if (type.size() > 2) {
                return new RedisError("Redis6 ACL is not supported");
            }
            AuthManager auth = engine.authManager();
            String userAuth = type.get(1).str();
            if (!auth.needAuth() || auth.tryAuth(userAuth, client)) {
                return OK;
            }else{
                return new RedisError("WRONGPASS invalid username-password pair");
            }
        }
    }
    
}

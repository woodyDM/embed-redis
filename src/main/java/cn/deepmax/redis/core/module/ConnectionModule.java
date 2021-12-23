package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.AuthManager;
import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.RedisCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.type.CallbackRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import lombok.extern.slf4j.Slf4j;

/**
 * @author wudi
 * @date 2021/5/10
 */
@Slf4j
public class ConnectionModule extends BaseModule {
    public ConnectionModule() {
        super("connection");
        register(new Auth());
        register(new Hello());
        register(new Ping());
        register(new Quit());
        register(new Select());
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


    private static class Hello implements RedisCommand {
        @Override
        public RedisMessage response(RedisMessage type, Redis.Client client, RedisEngine engine) {
//            if (type.size() == 1) {
//                RedisArray array = new RedisArray();
//                array.add(RedisBulkString.of("server"));
//                array.add(RedisBulkString.of("redis"));
//                array.add(RedisBulkString.of("proto"));
//                array.add(new RedisInteger(2));
//                return array;
//            } else {
//                RedisType v = type.get(1);
//                return new RedisError("NOPROTO unsupported protocol version");
//            }
            return OK;
        }

    }

    private static class Ping implements RedisCommand {
        @Override
        public RedisMessage response(RedisMessage type, Redis.Client client, RedisEngine engine) {
            return new SimpleStringRedisMessage("PONG");
        }
    }

    private static class Quit implements RedisCommand {
        @Override
        public RedisMessage response(RedisMessage type, Redis.Client client, RedisEngine engine) {
            CallbackRedisMessage msg = CallbackRedisMessage.of(OK);
            msg.addHook(c ->
                    client.channel().close()
                            .addListener(e -> log.debug("Client quit! {}", client.channel().remoteAddress())));
            return msg;
        }
    }

    private static class Select implements RedisCommand {
        @Override
        public RedisMessage response(RedisMessage type, Redis.Client client, RedisEngine engine) {
            String idx = cast(type).getAt(1).str();
            int i = Integer.parseInt(idx);
            engine.getDbManager().switchTo(client, i);
            return OK;
        }
    }

}

package cn.deepmax.redis;

import cn.deepmax.redis.command.*;
import cn.deepmax.redis.message.MessageWrapper;
import cn.deepmax.redis.type.RedisType;
import io.netty.handler.codec.redis.ArrayRedisMessage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SET key value [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp|KEEPTTL] [NX|XX] [GET]
 * @author wudi
 * @date 2021/4/29
 */
public class RedisCommandFactory {

    private static final Map<String, RedisCommand> c = new ConcurrentHashMap<>();
    static {
        add("ping", new Ping());
        add("set", new Set());
        add("get", new Get());
        add("del", new Del());
        add("hello", new Hello());
    }

    private static void add(String key, RedisCommand command) {
        c.put(key, command);
    }

    public  static RedisCommand command(RedisType type) {
        if (type.isArray()) {
            RedisType cmd = type.get(0);
            if (cmd.isString()) {
                String strCmd = cmd.str().toLowerCase();
                RedisCommand redisCommand = c.get(strCmd);
                if (redisCommand != null) {
                    return redisCommand;
                }
            }
        }

        return new UnsupportedErrorCommand();
    }
 
}

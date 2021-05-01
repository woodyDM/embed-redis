package cn.deepmax.redis;

import cn.deepmax.redis.command.*;
import cn.deepmax.redis.message.MessageWrapper;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

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
        add("ping", new PingCommand());
        add("set", new Set());
        add("get", new Get());
        add("del", new Del());
    }

    private static void add(String key, RedisCommand command) {
        c.put(key, command);
    }

    public  static RedisCommand command(RedisMessage redisMessage) {
        if (redisMessage instanceof ArrayRedisMessage) {
            MessageWrapper m = new MessageWrapper((ArrayRedisMessage) redisMessage);
            String content = m.getAt(0).toLowerCase();
            RedisCommand command = c.get(content);
            if (command != null) {
                return command;
            }
        }
        return new UnsupportedErrorCommand();
    }
 
}

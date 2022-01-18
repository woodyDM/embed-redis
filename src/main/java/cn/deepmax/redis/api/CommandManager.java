package cn.deepmax.redis.api;

import cn.deepmax.redis.core.RedisCommand;
import io.netty.handler.codec.redis.RedisMessage;

/**
 * @author wudi
 */
public interface CommandManager {

    RedisCommand getCommand(RedisMessage msg);

}

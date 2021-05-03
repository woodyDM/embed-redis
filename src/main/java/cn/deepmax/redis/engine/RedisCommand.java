package cn.deepmax.redis.engine;

import cn.deepmax.redis.type.RedisType;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author wudi
 * @date 2021/4/29
 */
public interface RedisCommand {

    /**
     * command for search
     * @return
     */
    default String name(){
        return this.getClass().getSimpleName().toLowerCase();
    }

    RedisType response(RedisType type, ChannelHandlerContext ctx);
    
}

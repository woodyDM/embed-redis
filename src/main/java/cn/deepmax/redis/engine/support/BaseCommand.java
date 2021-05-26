package cn.deepmax.redis.engine.support;

import cn.deepmax.redis.engine.RedisCommand;
import cn.deepmax.redis.engine.RedisEngine;
import cn.deepmax.redis.engine.RedisObject;
import cn.deepmax.redis.type.RedisType;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

public abstract class BaseCommand<E extends RedisObject> implements RedisCommand {
    protected RedisEngine engine;
    protected Channel channel;
    
    @Override
    public RedisType response(RedisType type, ChannelHandlerContext ctx, RedisEngine engine) {
        this.engine = engine;
        this.channel = ctx.channel();
        return response_(type, ctx);
    }

    protected abstract RedisType response_(RedisType type, ChannelHandlerContext ctx);

    protected E get(byte[] key){
        RedisObject obj = engine.getDbManager().get(channel).get(key);
        return cast(obj);
    }

    protected E set(byte[] key, RedisObject object) {
        return cast(engine.getDbManager().get(channel).set( key, object));
    }

    protected E del(byte[] key) {
        return cast(engine.getDbManager().get(channel).del(key));
    }
    
    @SuppressWarnings("unchecked")
    private E cast(RedisObject obj){
        return (E) obj;
    }
}

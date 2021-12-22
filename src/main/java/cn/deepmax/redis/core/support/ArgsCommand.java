package cn.deepmax.redis.core.support;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisObject;
import cn.deepmax.redis.api.RedisServerException;
import cn.deepmax.redis.core.RedisCommand;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * @author wudi
 * @date 2021/12/21
 */
public abstract class ArgsCommand<T extends RedisObject> implements RedisCommand {

    protected int limit;
    protected RedisEngine engine;
    protected Redis.Client client;

    public ArgsCommand(int limit) {
        this.limit = limit;
    }

    @Override
    public RedisMessage response(RedisMessage type, Redis.Client client, RedisEngine engine) {
        //this is thread unsafe. but redis will only use one thread.
        this.engine = engine;
        this.client = client;
        ListRedisMessage msg = cast(type);
        if (msg.children().size() < limit) {
            return new ErrorRedisMessage("ERR wrong number of arguments for '"
                    + this.getClass().getSimpleName().toLowerCase() + "' command");
        }
        return doResponse(msg, client, engine);
    }

    /**
     * helper method for types
     * @param key
     * @return
     */
    @SuppressWarnings("unchecked")
    protected T get(byte[] key) {
        RedisObject obj = engine.getDb(client).get(key);
        if (obj == null) {
            return null;
        }
        //check type
        Type t = this.getClass().getGenericSuperclass();
        ParameterizedType p = (ParameterizedType) t;
        Class<?> clazz = (Class<?>) p.getActualTypeArguments()[0];
        if (clazz.isInstance(obj)) {
            return (T) obj;
        } else {
            throw new RedisServerException(Constants.ERR_TYPE);
        }
    }

    abstract protected RedisMessage doResponse(ListRedisMessage msg, Redis.Client client, RedisEngine engine);
    
    public abstract static class TwoWith<T extends RedisObject> extends ArgsCommand<T> {
        public TwoWith() {
            super(2);
        }
    }

    public abstract static class ThreeWith<T extends RedisObject> extends ArgsCommand<T> {
        public ThreeWith() {
            super(3);
        }
    }

    public abstract static class FourWith<T extends RedisObject> extends ArgsCommand<T> {
        public FourWith() {
            super(4);
        }
    }

    public abstract static class Two extends ArgsCommand<RVoid> {
        public Two() {
            super(2);
        }
    }
    
    public abstract static class Three extends ArgsCommand<RVoid> {
        public Three() {
            super(3);
        }
    }

    static class RVoid extends AbstractRedisObject {
        public RVoid() {
            super(null);
        }
    }
} 
    

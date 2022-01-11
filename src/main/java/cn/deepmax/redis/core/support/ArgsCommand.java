package cn.deepmax.redis.core.support;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisObject;
import cn.deepmax.redis.api.RedisServerException;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.RedisCommand;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author wudi
 * @date 2021/12/21
 */
public abstract class ArgsCommand<T extends RedisObject> implements RedisCommand {
    protected int[] validArgs;
    protected RedisEngine engine;
    protected Client client;
    protected final int limit;
    private final boolean exact;

    public ArgsCommand(int limit) {
        this(limit, false);
    }

    public ArgsCommand(int... valid) {
        this.limit = -1;
        this.validArgs = valid;
        this.exact = false;
    }

    public ArgsCommand(int limit, boolean exact) {
        this.limit = limit;
        this.exact = exact;
    }

    @Override
    public RedisMessage response(RedisMessage type, Client client, RedisEngine engine) {
        //this is thread unsafe. but redis will only use one thread.
        this.engine = engine;
        this.client = client;
        ListRedisMessage msg = cast(type);
        Optional<ErrorRedisMessage> err = preCheckLength(msg);
        if (err.isPresent()) {
            return err.get();
        }
        return doResponse(msg, client, engine);
    }

    protected Optional<ErrorRedisMessage> exactCheckLength(RedisMessage type, int size) {
        if (cast(type).children().size() != size) {
            return createErrorRedisMessage();
        } else {
            return Optional.empty();
        }
    }

    private Optional<ErrorRedisMessage> createErrorRedisMessage() {
        ErrorRedisMessage value = new ErrorRedisMessage("ERR wrong number of arguments for '"
                + this.getClass().getSimpleName().toLowerCase() + "' command");
        return Optional.of(value);
    }

    public Optional<ErrorRedisMessage> preCheckLength(RedisMessage type) {
        ListRedisMessage msg = cast(type);
        int len = cast(type).children().size();
        if (limit == -1) {
            for (int i : validArgs) {
                if (i == len) return Optional.empty();
            }
            return createErrorRedisMessage();
        } else {
            if (msg.children().size() < limit || (exact && len != limit)) {
                return createErrorRedisMessage();
            }
            return Optional.empty();
        }
    }

    /**
     * helper method for types
     *
     * @param key
     * @return
     */
    @SuppressWarnings("unchecked")
    protected T get(byte[] key) {
        RedisObject obj = engine.getDb(client).get(client, key);
        if (obj == null) {
            return null;
        }
        Class<?> clazz = getGenericClass();
        if (clazz.isInstance(obj)) {
            return (T) obj;
        } else {
            throw new RedisServerException(Constants.ERR_TYPE);
        }
    }

    private Class<?> getGenericClass() {
        Class<?> cur = this.getClass();
        while (cur != null) {
            Type t = cur.getGenericSuperclass();
            if (t instanceof ParameterizedType) {
                ParameterizedType p = (ParameterizedType) t;
                return (Class<?>) p.getActualTypeArguments()[0];
            } else {
                cur = cur.getSuperclass();
            }
        }
        throw new IllegalStateException("can't predicate real type of " + this.getClass().getName());
    }

    /**
     * keys helper
     *
     * @param children
     * @return
     */
    protected List<Key> genKeys(List<RedisMessage> children, int start) {
        return genKeys(children, start, children.size());
    }

    /**
     * keys helper
     *
     * @param children
     * @return
     */
    protected List<Key> genKeys(List<RedisMessage> children, int start, int endEx) {
        List<Key> channels = new ArrayList<>(children.size() - 1);
        for (int i = start; i < endEx; i++) {
            channels.add(new Key(((FullBulkValueRedisMessage) children.get(i)).bytes()));
        }
        return channels;
    }

    abstract protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine);

    /*  Helper classes */
    public abstract static class TwoExWith<T extends RedisObject> extends ArgsCommand<T> {
        public TwoExWith() {
            super(2, true);
        }
    }

    public abstract static class TwoWith<T extends RedisObject> extends ArgsCommand<T> {
        public TwoWith() {
            super(2);
        }
    }

    public abstract static class ThreeExWith<T extends RedisObject> extends ArgsCommand<T> {
        public ThreeExWith() {
            super(3, true);
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

    public abstract static class FourExWith<T extends RedisObject> extends ArgsCommand<T> {
        public FourExWith() {
            super(4, true);
        }
    }

    public abstract static class TwoEx extends ArgsCommand<RVoid> {
        public TwoEx() {
            super(2, true);
        }
    }

    public abstract static class OneEx extends ArgsCommand<RVoid> {
        public OneEx() {
            super(1,true);
        }
    }
    
    public abstract static class Two extends ArgsCommand<RVoid> {
        public Two() {
            super(2);
        }
    }

    public abstract static class One extends ArgsCommand<RVoid> {
        public One() {
            super(1);
        }
    }

    public abstract static class ThreeEx extends ArgsCommand<RVoid> {
        public ThreeEx() {
            super(3, true);
        }
    }

    public abstract static class Three extends ArgsCommand<RVoid> {
        public Three() {
            super(3);
        }
    }

    public static class RVoid extends AbstractRedisObject {
        public RVoid() {
            super(null);
        }
    }
} 
    

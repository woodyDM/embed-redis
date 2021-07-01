package cn.deepmax.redis.core.support;

import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.api.RedisObject;
import cn.deepmax.redis.core.RedisCommand;
import cn.deepmax.redis.type.RedisType;

public abstract class BaseCommand<E extends RedisObject> implements RedisCommand {
    protected RedisEngine engine;
    protected Redis.Client client;

    @Override
    public RedisType response(RedisType type, Redis.Client client, RedisEngine engine) {
        this.engine = engine;
        this.client = client;
        return response_(type, client);
    }

    protected abstract RedisType response_(RedisType type, Redis.Client client);

    protected E get(byte[] key) {
        RedisObject obj = engine.getDbManager().get(client).get(key);
        return cast(obj);
    }

    protected E set(byte[] key, RedisObject object) {
        return cast(engine.getDbManager().get(client).set(key, object));
    }

    protected E del(byte[] key) {
        return cast(engine.getDbManager().get(client).del(key));
    }

    @SuppressWarnings("unchecked")
    private E cast(RedisObject obj) {
        return (E) obj;
    }
}

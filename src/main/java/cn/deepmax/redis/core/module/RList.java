package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.TimeProvider;
import cn.deepmax.redis.core.support.AbstractRedisObject;

/**
 *
 */
public class RList extends AbstractRedisObject {
    public RList(TimeProvider timeProvider) {
        super(timeProvider);
    }
}

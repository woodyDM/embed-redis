package cn.deepmax.redis.engine;

import cn.deepmax.redis.infra.TimeProvider;

import java.util.List;

public interface Module {

    List<RedisCommand> commands();

    String moduleName();

    default void setTimeProvider(TimeProvider timeProvider) {
    }

}

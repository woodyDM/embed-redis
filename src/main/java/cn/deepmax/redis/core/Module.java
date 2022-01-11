package cn.deepmax.redis.core;

import java.util.Map;

public interface Module {

    Map<String, RedisCommand> commands();

    String moduleName();

}

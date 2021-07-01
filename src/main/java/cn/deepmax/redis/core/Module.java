package cn.deepmax.redis.core;

import java.util.List;

public interface Module {

    List<RedisCommand> commands();

    String moduleName();

}

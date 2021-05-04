package cn.deepmax.redis.engine;

import java.util.List;

public interface Module {

    List<RedisCommand> commands();

    String moduleName();

}

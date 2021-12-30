package cn.deepmax.redis.core.module;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.RedisServerException;
import cn.deepmax.redis.resp3.ListRedisMessage;

import java.util.Optional;

/**
 * @author wudi
 * @date 2021/12/29
 */
public class ArgParser {
    
    static Optional<Long> parseLongArg(ListRedisMessage msg, String name) {
        int len = msg.children().size();
        for (int i = 3; i < len; i++) {
            String key = msg.getAt(i).str();
            if (name.toLowerCase().equals(key.toLowerCase())) {
                if (i + 1 < len) {
                    Long v = msg.getAt(i + 1).val();
                    return Optional.of(v);
                } else {
                    throw new RedisServerException(Constants.ERR_SYNTAX);
                }
            }
        }
        return Optional.empty();
    }
}

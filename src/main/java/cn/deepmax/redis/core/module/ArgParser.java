package cn.deepmax.redis.core.module;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.RedisServerException;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.utils.NumberUtils;

import java.util.Optional;

/**
 * @author wudi
 * @date 2021/12/29
 */
public class ArgParser {

    static Optional<String> parseArg(ListRedisMessage msg, int startIndex, String name) {
        int len = msg.children().size();
        for (int i = startIndex; i < len; i++) {
            String key = msg.getAt(i).str();
            if (name.toLowerCase().equals(key.toLowerCase())) {
                if (i + 1 < len) {
                    String v = msg.getAt(i + 1).str();
                    return Optional.of(v);
                } else {
                    throw new RedisServerException(Constants.ERR_SYNTAX);
                }
            }
        }
        return Optional.empty();
    }

    static Optional<Long> parseLongArg(ListRedisMessage msg, String name) {
        return parseArg(msg, 3, name).map(NumberUtils::parse);
    }
}

package cn.deepmax.redis.core.module;

import cn.deepmax.redis.api.RedisObject;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.RPattern;
import cn.deepmax.redis.resp3.FullBulkValueRedisMessage;
import cn.deepmax.redis.resp3.ListRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * @author wudi
 * @date 2022/1/12
 */
public class ScanMaps {
    /**
     * @param map
     * @param extraMapper 不为空时额外加数据
     * @param cursor
     * @param count
     * @param pattern
     * @param type        globalMap=true时有效，hscan zscan sscan 固定传empty
     * @return
     */
    public static RedisMessage genericScan(ScanMap<Key, ?> map, Function<Key, byte[]> extraMapper,
                                           Long cursor, Long count, Optional<String> pattern, Optional<String> type) {
        Optional<RPattern> p = pattern.map(RPattern::compile);
        Function<Key, Boolean> mapper = k -> !p.isPresent() || p.get().matches(k.str());
        ScanMap.ScanResult<Key> result = map.scan(cursor, count, mapper);
        List<RedisMessage> keys = new ArrayList<>();
        for (Key key : result.getKeyNames()) {
            if (type.isPresent() && !type.get().equalsIgnoreCase(((RedisObject) map.get(key)).type().name())) {
                continue;
            }
            keys.add(FullBulkValueRedisMessage.ofString(key.getContent()));
            if (extraMapper != null) {
                keys.add(FullBulkValueRedisMessage.ofString(extraMapper.apply(key)));
            }
        }
        return ListRedisMessage.newBuilder()
                .append(FullBulkValueRedisMessage.ofString(result.getNextCursor().toString()))
                .append(new ListRedisMessage(keys))
                .build();
    }

}

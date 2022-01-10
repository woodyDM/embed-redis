package cn.deepmax.redis.core.module;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.RedisServerException;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.utils.NumberUtils;
import cn.deepmax.redis.utils.Tuple;

import java.util.Optional;

/**
 * @author wudi
 * @date 2021/12/29
 */
public class ArgParser {
    /**
     * 查找ex 是否范围里存在
     *
     * @param msg
     * @param flag
     * @param start
     * @param end
     * @return
     */
    public static boolean parseFlag(ListRedisMessage msg, String flag, int start, int end) {
        for (int i = start; i < end; i++) {
            String key = msg.getAt(i).str();
            if (flag.equalsIgnoreCase(key.toLowerCase())) {
                return true;
            }
        }
        return false;
    }

    public static boolean parseFlag(ListRedisMessage msg, String ex, int start) {
        return parseFlag(msg, ex, start, msg.children().size());
    }

    public static boolean parseFlag(ListRedisMessage msg, String ex) {
        return parseFlag(msg, ex, 3, msg.children().size());
    }

    public static Optional<Tuple<String, String>> parseArgTwo(ListRedisMessage msg, String flag, int start, int end) {
        for (int i = start; i < end; i++) {
            String key = msg.getAt(i).str();
            if (flag.toLowerCase().equals(key.toLowerCase())) {
                if (i + 2 < end) {
                    String v1 = msg.getAt(i + 1).str();
                    String v2 = msg.getAt(i + 2).str();
                    return Optional.of(new Tuple<>(v1, v2));
                } else {
                    throw new RedisServerException(Constants.ERR_SYNTAX);
                }
            }
        }
        return Optional.empty();
    }

    public static Optional<Tuple<Long, Long>> parseLongArgTwo(ListRedisMessage msg, String flag, int start, int end) {
        Optional<Tuple<String, String>> v = parseArgTwo(msg, flag, start, end);
        return v.map(t -> {
            Long l1 = NumberUtils.parse(t.a);
            Long l2 = NumberUtils.parse(t.b);
            return new Tuple<>(l1, l2);
        });
    }

    /**
     * 解析name后面一个的参数
     *
     * @param msg
     * @param startIndex
     * @param name
     * @return
     */
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

    /**
     * 解析name后面一个的Long参数
     *
     * @param msg
     * @param name
     * @return
     */
    static Optional<Long> parseLongArg(ListRedisMessage msg, String name) {
        return parseArg(msg, 3, name).map(NumberUtils::parse);
    }

    /**
     * 解析count
     * @param msg
     * @param lastPos
     * @return
     */
    public static Optional<CountArg> parseCount(ListRedisMessage msg, int lastPos) {
        CountArg arg = new CountArg();
        if (msg.children().size() == lastPos+1) {
            arg.count = msg.getAt(lastPos).val();
            arg.withCount = true;
        }
        if (arg.count == 0) {
            return Optional.empty();
        }
        return Optional.of(arg);
    }
    
    public static class CountArg {
        public boolean withCount = false;
        public long count = 1;
    }
}

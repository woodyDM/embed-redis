package cn.deepmax.redis.utils;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.Redis;
import cn.deepmax.redis.api.RedisServerException;

import java.text.NumberFormat;
import java.util.Optional;

/**
 * @author wudi
 * @date 2021/5/8
 */
public class NumberUtils {

    public static Long parse(String s) {
        return parseO(s).orElseThrow(() -> new RedisServerException(Constants.ERR_SYNTAX_NUMBER));
    }

    public static Double parseDouble(String s) {
        return parseDoubleO(s).orElseThrow(() -> new RedisServerException("ERR value is not a valid float"));
    }

    public static String formatDouble(Double d) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        // INCRBYFLOAT 的计算结果也最多只能表示小数点的后十七位。
        nf.setMaximumFractionDigits(17);
        return nf.format(d);
    }

    public static Optional<Double> parseDoubleO(String s) {
        if (s == null || s.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(Double.valueOf(s));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public static Optional<Long> parseO(String s) {
        if (s == null || s.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(Long.valueOf(s));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }
}

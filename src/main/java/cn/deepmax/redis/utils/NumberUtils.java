package cn.deepmax.redis.utils;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.RedisServerException;
import cn.deepmax.redis.core.Key;

import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Optional;

/**
 * @author wudi
 * @date 2021/5/8
 */
public class NumberUtils {

    private final static byte[] PLUS = new byte[]{'+'};
    private final static byte[] MINUS = new byte[]{'-'};

    public static Long parse(String s) {
        return parseO(s).orElseThrow(() -> new RedisServerException(Constants.ERR_SYNTAX_NUMBER));
    }

    /**
     * @param s
     * @return mills
     */
    public static long parseTimeout(String s) {
        if ("0".equals(s)) {
            return 0;
        }
        Double t = parseDoubleO(s).orElseThrow(() -> new RedisServerException("ERR timeout is not a float or out of range"));
        if (t < 0) {
            throw new RedisServerException("ERR timeout is negative");
        }
        return (long) (t * 1000);
    }

    public static Long parse(String s, String errorMessage) {
        return parseO(s).orElseThrow(() -> new RedisServerException(errorMessage));
    }

    public static Double parseDouble(String s) {
        return parseDoubleO(s).orElseThrow(() -> new RedisServerException("ERR value is not a valid float"));
    }

    public static Range<Key> parseLexRange(byte[] startB, byte[] endB) {
        Range<Key> r = new Range<>();
        Tuple<Key, Boolean> start = parseKey(startB);
        Tuple<Key, Boolean> end = parseKey(endB);
        r.start = start.a;
        r.startOpen = start.b;
        r.end = end.a;
        r.endOpen = end.b;
        return r;
    }

    public static Range<Double> parseScoreRange(String min, String max) {
        min = min.toLowerCase();
        max = max.toLowerCase();
        Range<Double> r = new Range<>();
        if (min.startsWith("(")) {
            r.startOpen = true;
            min = min.substring(1);
        }
        if (max.startsWith("(")) {
            r.endOpen = true;
            max = max.substring(1);
        }
        r.start = parseDoubleWithInf(min);
        r.end = parseDoubleWithInf(max);
        return r;
    }

    /**
     * @param b
     * @return true if open
     */
    public static Tuple<Key, Boolean> parseKey(byte[] b) {
        if (b == null || b.length == 0) {
            throw new RedisServerException(Constants.ERR_SYNTAX);
        }

        if (Arrays.equals(b, PLUS)) {
            return new Tuple<>(Key.INF, true);
        } else if (Arrays.equals(b, MINUS)) {
            return new Tuple<>(Key.NEG_INF, true);
        } else if (b.length >= 2) {
            //at least 2 bytes
            byte[] c = new byte[b.length - 1];
            System.arraycopy(b, 1, c, 0, c.length);
            if (b[0] == '(') {
                return new Tuple<>(new Key(c), true);
            } else if (b[0] == '[') {
                return new Tuple<>(new Key(c), false);
            } else {
                throw new RedisServerException(Constants.ERR_SYNTAX);
            }
        } else {
            throw new RedisServerException(Constants.ERR_SYNTAX);
        }
    }

    private static Double parseDoubleWithInf(String s) {
        if (s.length() == 0) {
            throw new RedisServerException(Constants.ERR_SYNTAX);
        }
        if (s.equals("inf") || s.equals("+inf")) {
            return Double.POSITIVE_INFINITY;
        } else if (s.equals("-inf")) {
            return Double.NEGATIVE_INFINITY;
        } else {
            return parseDoubleO(s)
                    .filter(d -> !d.isNaN())
                    .orElseThrow(() -> new RedisServerException("min or max is not a float"));
        }
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

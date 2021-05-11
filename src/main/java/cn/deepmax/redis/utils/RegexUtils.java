package cn.deepmax.redis.utils;

/**
 * @author wudi
 * @date 2021/5/10
 */
public class RegexUtils {
    /**
     * h?llo subscribes to hello, hallo and hxllo
     * h*llo subscribes to hllo and heeeello
     * h[ae]llo subscribes to hello and hallo, but not hillo
     * Use \ to escape special characters if you want to match them verbatim.
     *
     * @param word
     * @return
     */
    public static String toRegx(String word) {
        char[] chars = word.toCharArray();
        StringBuilder sb = new StringBuilder();
        sb.append("^");
        for (int i = 0; i < chars.length; i++) {
            boolean start = i == 0;
            if ('?' == chars[i] && (start || chars[i - 1] != '\\')) {
                sb.append('.').append("{1}");
            } else if ('*' == chars[i] && (start || chars[i - 1] != '\\')) {
                sb.append('.').append('*');
            } else {
                sb.append(chars[i]);
            }
        }
        sb.append("$");
        return sb.toString();
    }
}

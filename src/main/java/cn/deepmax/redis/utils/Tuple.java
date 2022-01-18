package cn.deepmax.redis.utils;

/**
 * @author wudi
 */
public class Tuple<A,B> {
    public final A a;
    public final B b;

    public Tuple(A a, B b) {
        this.a = a;
        this.b = b;
    }
}

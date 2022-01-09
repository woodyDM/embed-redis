package cn.deepmax.redis.utils;

/**
 * @author wudi
 * @date 2021/12/22
 */
public class Triple<A,B,C> {
    public final A a;
    public final B b;
    public final C c;


    public Triple(A a, B b, C c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }
}
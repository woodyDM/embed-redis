package cn.deepmax.redis.api;

/**
 * @author wudi
 */
public interface Statistic {

    long messageRev();

    long messageSend();

    long incrSend();

}

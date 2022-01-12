package cn.deepmax.redis.api;

/**
 * @author wudi
 * @date 2022/1/12
 */
public interface Statistic {

    long messageRev();

    long messageSend();

    long incrSend();
}

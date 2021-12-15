package cn.deepmax.redis.api;

import java.time.LocalDateTime;

/**
 * @author wudi
 * @date 2021/4/30
 */
public interface TimeProvider {

    LocalDateTime now();

}

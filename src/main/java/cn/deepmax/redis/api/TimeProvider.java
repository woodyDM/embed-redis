package cn.deepmax.redis.api;

import java.time.LocalDateTime;

/**
 * @author wudi
 */
public interface TimeProvider {

    LocalDateTime now();

}

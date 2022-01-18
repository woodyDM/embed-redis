package cn.deepmax.redis.core;

import cn.deepmax.redis.api.TimeProvider;

import java.time.LocalDateTime;

/**
 * @author wudi
 */
public class DefaultTimeProvider implements TimeProvider {
    @Override
    public LocalDateTime now() {
        return LocalDateTime.now();
    }

}

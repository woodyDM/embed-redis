package cn.deepmax.redis.utils;

import cn.deepmax.redis.api.TimeProvider;

import java.time.LocalDateTime;

/**
 * @author wudi
 * @date 2021/12/21
 */
public class MockTimeProvider implements TimeProvider {
    public LocalDateTime time;
    
    @Override
    public LocalDateTime now() {
        return time;
    }
}

package cn.deepmax.redis.support;

import cn.deepmax.redis.api.TimeProvider;

import java.time.LocalDateTime;

/**
 * @author wudi
 */
public class MockTimeProvider implements TimeProvider {
    public LocalDateTime time;
    public static LocalDateTime BASE = LocalDateTime.of(2021, 9, 5, 12, 8, 0);

    public MockTimeProvider() {
        this.time = BASE;
    }

    public void reset() {
        this.time = BASE;
    }

    @Override
    public LocalDateTime now() {
        return time;
    }
}

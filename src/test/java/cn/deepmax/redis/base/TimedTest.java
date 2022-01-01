package cn.deepmax.redis.base;

import cn.deepmax.redis.support.MockTimeProvider;

import java.time.LocalDateTime;
import java.util.Objects;

public interface TimedTest {

    MockTimeProvider TIME_PROVIDER = new MockTimeProvider();

    default void mockTime(LocalDateTime time) {
        TIME_PROVIDER.time = Objects.requireNonNull(time);
    }

}

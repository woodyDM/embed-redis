package cn.deepmax.redis.infra;

import java.time.LocalDateTime;

/**
 * @author wudi
 * @date 2021/4/30
 */
public class DefaultTimeProvider implements TimeProvider{
    @Override
    public LocalDateTime now() {
        return LocalDateTime.now();
    }
    
}

package cn.deepmax.redis.core;

import cn.deepmax.redis.api.RedisObject;

/**
 * @author wudi
 * @date 2022/1/12
 */
public class RedisDataType implements RedisObject.Type {

    private final String name;
    private final String encoding;

    public RedisDataType(String name, String encoding) {
        this.name = name;
        this.encoding = encoding;
    }

    @Override
    public String encoding() {
        return encoding;
    }

    @Override
    public String name() {
        return name;
    }
}

package cn.deepmax.redis.base;

import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;

import java.nio.charset.StandardCharsets;

public interface ByteHelper {
    JdkSerializationRedisSerializer jdkSerializer = new JdkSerializationRedisSerializer();

    default byte[] bytes(String k) {
        return k.getBytes(StandardCharsets.UTF_8);
    }

    default byte[] serialize(String s) {
        return jdkSerializer.serialize(s);
    }
}

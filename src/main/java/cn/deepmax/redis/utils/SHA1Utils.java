package cn.deepmax.redis.utils;

import lombok.NonNull;

import java.util.UUID;

/**
 * @author wudi
 * @date 2021/5/7
 */
public class SHA1Utils {


    //todo
    public static String sha1(@NonNull String script) {
        return UUID.randomUUID().toString().replace("-","").toUpperCase();
        
    }
    
}

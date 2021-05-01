package cn.deepmax.redis.type;

import java.util.Collections;
import java.util.List;

/**
 * @author wudi
 * @date 2021/4/30
 */
public interface RedisType {

    default boolean isNil() {
        return false;
    }
    
    default boolean isString() {
        return false;
    }

    default boolean isError() {
        return false;
    }

    default boolean isInteger() {
        return false;
    }
    
    default boolean isArray() {
        return false;
    }
    
    default void add(RedisType type){
        throw new UnsupportedOperationException();
    }

    default List<RedisType> children() {
        return Collections.emptyList();
    }
    
    default RedisType get(int i) {
        throw new UnsupportedOperationException();
    }
    
    default String str(){
        throw new UnsupportedOperationException();
    }

    default long value(){
        throw new UnsupportedOperationException();
    }

    default byte[] bytes() {
        throw new UnsupportedOperationException();
    }
     
    default int size(){
        return children().size();
    }

    Type type();

    String respContent();

}

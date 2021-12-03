package cn.deepmax.redis.resp3;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.RedisCodecException;

/**
 * @author wudi
 * @date 2021/12/3
 */
public enum RedisMessageType {

    BLOG_STRING('$', false),    //$<length>\r\n<bytes>\r\n
    SIMPLE_STRING('+', true),  //+<string>\r\n
    SIMPLE_ERROR('-', true),   //-<string>\r\n    ex:-ERR this is the error description\r\n
    NUMBER(':', true),         //:<number>\r\n
    NULL('_', true),           //_\r\n
    DOUBLE(',', true),         //,<floating-point-number>\r\n          ",inf\r\n"   ",-inf\r\n"
    BOOLEAN('#', true),        //#t\r\n        #f\r\n
    BLOG_ERROR('!', false),     //!<length>\r\n<bytes>\r\n
    VERBATIM_STRING('=', false),//=<length>\r\n<bytes>\r\n
    BIG_NUMBER('(', true),     //(<big number>\r\n

    //Aggregate data types 
    //The format for every aggregate type in RESP3 is always the same:
    //<aggregate-type-char><numelements><CR><LF> ... numelements other types ...
    AGG_ARRAY('*', false),      //*
    AGG_MAP('%', false),        //%       Moreover the number of following elements must be even
    AGG_SET('~', false),        //~
    AGG_ATTRIBUTE('|', false),   //|

    PUGH_TYPE('>', false);      //>


//    STREAM(' ', false);    // ignored not SUPPORTED

    public final byte value;
    public final boolean inline;

    RedisMessageType(char value, boolean inline) {
        this.value = (byte) value;
        this.inline = inline;
    }

    /**
     */
    public static RedisMessageType readFrom(ByteBuf in ) {
        byte b = in.readByte();
        return of(b);
    }
    
    public static RedisMessageType of(byte b){
        for (RedisMessageType v : values()) {
            if (b == v.value) {
                return v;
            }
        }
        throw new RedisCodecException("invalid type of byte (" + b + ")");
    }


}   

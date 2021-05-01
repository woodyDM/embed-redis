package cn.deepmax.redis.command;

import cn.deepmax.redis.BulkString;
import cn.deepmax.redis.engine.RedisEngine;
import cn.deepmax.redis.type.RedisBulkString;
import cn.deepmax.redis.type.RedisType;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wudi
 * @date 2021/4/29
 */
public class Info implements RedisCommand {

    @Override
    public RedisType response(RedisEngine engine, RedisType type, ChannelHandlerContext ctx) {
        List<String> list = new ArrayList<>();
        list.add("# Server");
        list.add("redis_version:6.2.2");
        list.add("redis_git_sha1:00000000");
        list.add("redis_git_dirty:0");
        list.add("redis_build_id:ef30456deb15adb3");
        list.add("redis_mode:standalone");
        list.add("os:Linux 5.4.0-1030-aws x86_64");
        list.add("arch_bits:64");
        list.add("multiplexing_api:epoll");
        list.add("atomicvar_api:c11-builtin");
        list.add("gcc_version:9.3.0");
        list.add("process_id:68867");
        list.add("process_supervised:no");
        list.add("run_id:cb62122647041b0769d548a64fc66bad0a24f2d9");
        list.add("tcp_port:6379");
        list.add("server_time_usec:1619689544880924");
        list.add("uptime_in_seconds:793659");
        list.add("uptime_in_days:9");
        list.add("hz:10");
        list.add("configured_hz:10");
        ByteBuf buf = ctx.alloc().buffer();
        BulkString bulkString = new BulkString(list);
        bulkString.writeTo(buf);
        return RedisBulkString.of("2");
    }


}

/*
 * Copyright 2016 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package cn.deepmax.redis.resp3;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.MessageAggregator;
import io.netty.handler.codec.redis.*;
import io.netty.util.internal.UnstableApi;

/**
 * A {@link ChannelHandler} that aggregates an {@link BulkValueHeaderRedisMessage}
 * and its following {@link BulkStringRedisContent}s into a single {@link FullBulkValueRedisMessage}
 * with no following {@link BulkStringRedisContent}s.  It is useful when you don't want to take
 * care of {@link RedisMessage}s whose transfer encoding is 'chunked'.  Insert this
 * handler after {@link RedisDecoder} in the {@link ChannelPipeline}:
 * <pre>
 * {@link ChannelPipeline} p = ...;
 * ...
 * p.addLast("encoder", new {@link RedisEncoder}());
 * p.addLast("decoder", new {@link RedisDecoder}());
 * p.addLast("aggregator", <b>new {@link RedisBulkValueAggregator}()</b>);
 * ...
 * p.addLast("handler", new HttpRequestHandler());
 * </pre>
 * Be aware that you need to have the {@link RedisEncoder} before the {@link RedisBulkValueAggregator}
 * in the {@link ChannelPipeline}.
 */
@UnstableApi
public final class RedisBulkValueAggregator extends MessageAggregator<RedisMessage, BulkValueHeaderRedisMessage,
        BulkStringRedisContent, FullBulkValueRedisMessage> {

    /**
     * Creates a new instance.
     */
    public RedisBulkValueAggregator() {
        super(Constants.REDIS_MESSAGE_MAX_LENGTH);
    }

    @Override
    protected boolean isStartMessage(RedisMessage msg) throws Exception {
        return msg instanceof BulkValueHeaderRedisMessage && !isAggregated(msg);
    }

    @Override
    protected boolean isContentMessage(RedisMessage msg) throws Exception {
        return msg instanceof BulkStringRedisContent;
    }

    @Override
    protected boolean isLastContentMessage(BulkStringRedisContent msg) throws Exception {
        return msg instanceof LastBulkStringRedisContent;
    }

    @Override
    protected boolean isAggregated(RedisMessage msg) throws Exception {
        return msg instanceof FullBulkValueRedisMessage;
    }

    @Override
    protected boolean isContentLengthInvalid(BulkValueHeaderRedisMessage start, int maxContentLength)
            throws Exception {
        return start.getLength() > maxContentLength;
    }

    @Override
    protected Object newContinueResponse(BulkValueHeaderRedisMessage start, int maxContentLength,
                                         ChannelPipeline pipeline) throws Exception {
        return null;
    }

    @Override
    protected boolean closeAfterContinueResponse(Object msg) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean ignoreContentAfterContinueResponse(Object msg) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected FullBulkValueRedisMessage beginAggregation(BulkValueHeaderRedisMessage start, ByteBuf content)
            throws Exception {
        return new FullBulkValueRedisMessage(content, start.getType());
    }
}

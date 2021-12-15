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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.RedisCodecException;
import io.netty.handler.codec.redis.RedisDecoder;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.util.ReferenceCountUtil;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Aggregates {@link RedisMessage} parts into {@link ArrayRedisMessage}. This decoder
 * should be used together with {@link RedisDecoder}.
 */
public class RedisAggTypesAggregator extends MessageToMessageDecoder<RedisMessage> {

    private final Deque<AggregateState> depths = new ArrayDeque<AggregateState>(4);

    @Override
    protected void decode(ChannelHandlerContext ctx, RedisMessage msg, List<Object> out) throws Exception {
        if (msg instanceof AggRedisTypeHeaderMessage) {
            msg = decodeRedisArrayHeader((AggRedisTypeHeaderMessage) msg);
            if (msg == null) {
                return;
            }
        } else {
            ReferenceCountUtil.retain(msg);
        }

        while (!depths.isEmpty()) {
            AggregateState current = depths.peek();
            List<RedisMessage> children = current.children;
            children.add(msg);

            // if current aggregation completed, go to parent aggregation.
            if (children.size() == current.length) {
                switch (current.type) {
                    case AGG_MAP:
                        msg = new MapRedisMessage(children);
                        break;
                    case AGG_ARRAY:
                        msg = new ListRedisMessage(children);
                        break;
                    case AGG_SET:
                        msg = new SetRedisMessage(children);
                        break;
                    case AGG_ATTRIBUTE:
                        msg = new AttributeRedisMessage(children);
                        break;
                    default:
                        throw new RedisCodecException("type invalid " + current.type);
                }
                depths.pop();
            } else {
                // not aggregated yet. try next time.
                return;
            }
        }

        out.add(msg);
    }

    private RedisMessage decodeRedisArrayHeader(AggRedisTypeHeaderMessage header) {
        if (header.length() == 0L) {
            switch (header.getType()) {
                case AGG_MAP:
                    return MapRedisMessage.EMPTY;
                case AGG_ARRAY:
                    return ListRedisMessage.EMPTY_INSTANCE;
                case AGG_SET:
                    return SetRedisMessage.EMPTY_INSTANCE;
                case AGG_ATTRIBUTE:
                    return AttributeRedisMessage.EMPTY;
                default:
                    throw new RedisCodecException("type invalid " + header.getType());
            }
        } else if (header.length() > 0L) {
            // Currently, this codec doesn't support `long` length for arrays because Java's List.size() is int.
            if (header.length() > Integer.MAX_VALUE) {
                throw new CodecException("this codec doesn't support longer length than " + Integer.MAX_VALUE);
            }

            // start aggregating array
            depths.push(new AggregateState((int) header.length(), header.getType()));
            return null;
        } else {
            throw new CodecException("bad length: " + header.length());
        }
    }

    private static final class AggregateState {
        private final int length;
        private final List<RedisMessage> children;
        private final RedisMessageType type;

        AggregateState(int length, RedisMessageType type) {
            this.length = length;
            this.children = new ArrayList<>(length);
            this.type = type;
        }
    }
}

package cn.deepmax.redis.core.module;

import cn.deepmax.redis.Constants;
import cn.deepmax.redis.api.Client;
import cn.deepmax.redis.api.RedisEngine;
import cn.deepmax.redis.core.Key;
import cn.deepmax.redis.core.support.ArgsCommand;
import cn.deepmax.redis.core.support.BaseModule;
import cn.deepmax.redis.resp3.ListRedisMessage;
import cn.deepmax.redis.utils.NumberUtils;
import cn.deepmax.redis.utils.Tuple;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author wudi
 * @date 2021/12/30
 */
public class SortedSetModule extends BaseModule {
    public SortedSetModule() {
        super("sortedSet");
    }

    /**
     * ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
     */
    public static class ZAdd extends ArgsCommand.FourWith<SortedSet> {
        @Override
        protected RedisMessage doResponse(ListRedisMessage msg, Client client, RedisEngine engine) {
            byte[] key = msg.getAt(1).bytes();
            //scan from tail
            int idx = msg.children().size() - 2;
            List<Tuple<Double, Key>> values = new ArrayList<>();
            Optional<Double> v;
            while (idx >= 2 && (v = NumberUtils.parseDoubleO(msg.getAt(idx).str())).isPresent()) {
                values.add(new Tuple<>(v.get(), new Key(msg.getAt(idx + 1).bytes())));
                idx -= 2;
            }
            if (values.isEmpty()) {
                return Constants.ERR_SYNTAX;
            }
            //todo check from [2,idx+1] flags  
            SortedSet set = get(key);
            if (set == null) {
                set = new SortedSet(engine.timeProvider());
                engine.getDb(client).set(client, key, set);
            }
            //todo return values
            set.add(values);
            return Constants.INT_ONE;
        }

    }

}

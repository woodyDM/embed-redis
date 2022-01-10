## Debug  
-Dio.netty.leakDetectionLevel=PARANOID

## Commands
### List
|name|supported|description|
|-----|-----|-----|
|LPOP|Y||
|RPOP|Y||
|LPUSH|Y||
|RPUSH|Y||
|LPUSHX|Y||
|RPUSHX|Y||
|LLEN|Y||
|LPOS|Y||
|LTRIM|Y||
|LINDEX|Y||
|LRANGE|Y||
|RPOPLPUSH|Y||
|LMOVE|Y||
|LMPOP|N||
|LINSERT|Y||
|LREM|Y||
|LSET|Y||
|BLPOP|Y||
|BRPOP|Y||
|BLMOVE|N|redis 7.x|
|BRPOPLPUSH|Y||
|BLMPOP|N|redis 7.x|

### Sorted Set
|name|supported|description|
|-----|-----|-----|
|BZMPOP|N|7.x|
|BZPOPMAX|Y||
|BZPOPMIN|Y||
|ZADD|Y||
|ZCARD|Y||
|ZCOUNT|Y||
|ZDIFF|Y||
|ZDIFFSTORE|Y||
|ZINCRBY|Y||
|ZINTER|Y||
|ZINTERCARD|N|7.x|
|ZINTERSTORE|Y||
|ZLEXCOUNT|Y||
|ZMPOP|N|7.x|
|ZMSCORE|Y||
|ZPOPMAX|Y||
|ZPOPMIN|Y||
|ZRANDMEMBER|Y||
|ZRANGE|Y||
|ZRANGEBYLEX|Y||
|ZRANGEBYSCORE|Y||
|ZRANGESTORE|Y||
|ZRANK|Y||
|ZREM|Y||
|ZREMRANGEBYLEX|Y||
|ZREMRANGEBYRANK|Y||
|ZREMRANGEBYSCORE|Y||
|ZREVRANGE|Y||
|ZREVRANGEBYLEX|Y||
|ZREVRANGEBYSCORE|Y||
|ZREVRANK|Y||
|ZSCAN| |later|
|ZSCORE|Y||
|ZUNION|Y||
|ZUNIONSTORE|Y||

### Hash  
|name|supported|description|
|-----|-----|-----|
|HDEL| Y| |
|HEXISTS|Y | |
|HGET| Y| |
|HGETALL|Y | |
|HINCRBY| Y| |
|HINCRBYFLOAT|Y | |
|HKEYS| Y| |
|HLEN|Y | |
|HMGET|Y | |
|HMSET|Y | |
|HRANDFIELD| Y| |
|HSCAN| | later|
|HSET|Y | |
|HSETNX|Y | |
|HSTRLEN|Y | |
|HVALS|Y | |

### Set 
|name|supported|description|
|-----|-----|-----|
|SADD| Y| |
|SCARD|Y | |
|SDIFF|Y | |
|SDIFFSTORE| Y| |
|SINTER|Y | |
|SINTERCARD| | 7.x|
|SINTERSTORE| Y| |
|SISMEMBER|Y | |
|SMEMBERS|Y | |
|SMISMEMBER| Y| |
|SMOVE|Y | |
|SPOP|Y | |
|SRANDMEMBER| Y| |
|SREM|Y | |
|SSCAN| |later |
|SUNION|Y | |
|SUNIONSTORE| Y| |

## TODO
-[ ] Lua types.  
-[ ] Script error handle.  
-[ ] redis 7.x support.  
-[ ] cluster support.  
-[ ] scan commands.( all / sortedset / hash / set)
-[ ] Block command in Transaction and scripting
-[ ] Events and `SizedOperation`

  



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
|BLMOVE|Y|redis 7.x|
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
|ZINTERCARD|||
|ZINTERSTORE|Y||
|ZLEXCOUNT|||
|ZMPOP|N|7.x|
|ZMSCORE|Y||
|ZPOPMAX|Y||
|ZPOPMIN|Y||
|ZRANDMEMBER|||
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

## TODO
-[ ] Lua types.  
-[ ] Script error handle.  
-[ ] redis 7.x support.  
-[ ] cluster support.  
-[ ] scan commands.(all / sortedset / hash)     
-[ ] Upgrade redisson 3.16.7.   
-[X] Fix RedissonConnection lpos 
-[ ] Block command in Transaction and scripting

  



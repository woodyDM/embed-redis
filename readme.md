
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
|BZMPOP|||
|BZPOPMAX|||
|BZPOPMIN|||
|ZADD|||
|ZCARD|||
|ZCOUNT|||
|ZDIFF|||
|ZDIFFSTORE|||
|ZINCRBY|||
|ZINTER|||
|ZINTERCARD|||
|ZINTERSTORE|||
|ZLEXCOUNT|||
|ZMPOP|||
|ZMSCORE|Y||
|ZPOPMAX|||
|ZPOPMIN|||
|ZRANDMEMBER|||
|ZRANGE|||
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
|ZSCAN|||
|ZSCORE|Y||
|ZUNION|||
|ZUNIONSTORE|||

## TODO
-[ ] Lua types.  
-[ ] Script error handle.  
-[ ] redis 7.x support.  
-[ ] cluster support.  
-[ ] scans.     
-[ ] Upgrade redisson 3.16.7.   
-[X] Fix RedissonConnection lpos 

  



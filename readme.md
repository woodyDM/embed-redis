## Debug  
-Dio.netty.leakDetectionLevel=PARANOID

## Commands
### Bitmap 
|name|supported|description|
|-----|-----|-----|
|BITCOUNT| Y| |
|BITFIELD| |later |
|BITFIELD_RO| | later|
|BITOP|Y | |
|BITPOS|Y | |
|GETBIT| Y| |
|SETBIT|Y | |

### Cluster (later)
|name|supported|description|
|-----|-----|-----|
|ASKING| | |
|CLUSTER ADDSLOTS| | |
|CLUSTER ADDSLOTSRANGE| | |
|CLUSTER BUMPEPOCH| | |
|CLUSTER COUNT-FAILURE-REPORTS| | |
|CLUSTER COUNTKEYSINSLOT| | |
|CLUSTER DELSLOTS| | |
|CLUSTER DELSLOTSRANGE| | |
|CLUSTER FAILOVER| | |
|CLUSTER FLUSHSLOTS| | |
|CLUSTER FORGET| | |
|CLUSTER GETKEYSINSLOT| | |
|CLUSTER INFO| | |
|CLUSTER KEYSLOT| | |
|CLUSTER LINKS| | |
|CLUSTER MEET| | |
|CLUSTER MYID| | |
|CLUSTER NODES| | |
|CLUSTER REPLICAS| | |
|CLUSTER REPLICATE| | |
|CLUSTER RESET| | |
|CLUSTER SAVECONFIG| | |
|CLUSTER SET-CONFIG-EPOCH| | |
|CLUSTER SETSLOT| | |
|CLUSTER SLAVES| | |
|CLUSTER SLOTS| | |
|READONLY| | |
|READWRITE| | |

### Connection (later)
|name|supported|description|
|-----|-----|-----|
|AUTH|Y | |
|CLIENT CACHING|N |track not support |
|CLIENT GETNAME| Y| |
|CLIENT GETREDIR| N|track not support |
|CLIENT ID| Y| |
|CLIENT INFO|N |utils not support |
|CLIENT KILL| N|utils not support |
|CLIENT LIST|N | utils not support|
|CLIENT NO-EVICT| N| 7.x|
|CLIENT PAUSE| N| utils not support|
|CLIENT REPLY|N |utils not support |
|CLIENT SETNAME| Y| |
|CLIENT TRACKING|N |track not support |
|CLIENT TRACKINGINFO|N | track not support|
|CLIENT UNBLOCK|N | |
|CLIENT UNPAUSE| N| |
|ECHO|Y | |
|HELLO|Y | |
|PING|Y | |
|QUIT| Y| |
|RESET|Y | |
|SELECT| Y| |

### Keys
|name|supported|description|
|-----|-----|-----|  
|COPY| | |
|DEL| | |
|DUMP| | |
|EXISTS| | |
|EXPIRE| | |
|EXPIREAT| | |
|EXPIRETIME| | |
|KEYS| | |
|MIGRATE| | |
|MOVE| | |
|OBJECT ENCODING| | |
|OBJECT FREQ| | |
|OBJECT IDLETIME| | |
|OBJECT REFCOUNT| | |
|PERSIST| | |
|PEXPIRE| | |
|PEXPIREAT| | |
|PEXPIRETIME| | |
|PTTL| | |
|RANDOMKEY| | |
|RENAME| | |
|RENAMENX| | |
|RESTORE| | |
|SCAN| | |
|SORT| | |
|SORT_RO| | |
|TOUCH| | |
|TTL| | |
|TYPE| | |
|UNLINK| | |
|WAIT| | |

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

### Pub/sub  
|name|supported|description|
|-----|-----|-----|
|PSUBSCRIBE| | |
|PUBLISH| | |
|PUBSUB CHANNELS| | |
|PUBSUB NUMPAT| | |
|PUBSUB NUMSUB| | |
|PUNSUBSCRIBE| | |
|SUBSCRIBE| | |
|UNSUBSCRIBE| | |
 
### Scripting 
|name|supported|description|
|-----|-----|-----|
|EVAL| | |
|EVALSHA| | |
|EVALSHA_RO| | |
|EVAL_RO| | |
|FCALL| | |
|FCALL_RO| | |
|FUNCTION CREATE| | |
|FUNCTION DELETE| | |
|FUNCTION DUMP| | |
|FUNCTION FLUSH| | |
|FUNCTION INFO| | |
|FUNCTION KILL| | |
|FUNCTION LIST| | |
|FUNCTION RESTORE| | |
|FUNCTION STATS| | |
|SCRIPT DEBUG| | |
|SCRIPT EXISTS| | |
|SCRIPT FLUSH| | |
|SCRIPT KILL| | |
|SCRIPT LOAD| | |
 
### Strings 
|name|supported|description|
|-----|-----|-----|
|APPEND| Y| |
|DECR| Y| |
|DECRBY| Y| |
|GET|Y | |
|GETDEL| | |
|GETEX| | |
|GETRANGE| Y| |
|GETSET| Y| |
|INCR|Y | |
|INCRBY|Y | |
|INCRBYFLOAT|Y | |
|LCS| N| 7.x|
|MGET|Y | |
|MSET| Y| |
|MSETNX| Y| |
|PSETEX|Y | |
|SET|Y | |
|SETEX|Y | |
|SETNX|Y | |
|SETRANGE|Y | |
|STRLEN|Y | |
|SUBSTR| | later|

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
|SINTERCARD| N| 7.x|
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

### Transaction
|name|supported|description|
|-----|-----|-----| 
|DISCARD| Y| |
|EXEC|Y | |
|MULTI| Y| |
|UNWATCH| Y| |
|WATCH| Y| |

## TODO - list 
- [ ] Geo support.    
- [ ] HyperLogLog support.  
- [ ] Lua types.  
- [ ] Script error handle.    
- [ ] redis 7.x support.  
- [ ] Resp3.    
- [ ] cluster support.  
- [ ] scan commands.( all / sortedset / hash / set)     
- [ ] Block command in Transaction and scripting    
- [ ] Events and `SizedOperation`   
- [ ] Use different AuthManager for cluster and standalone mode.  

  



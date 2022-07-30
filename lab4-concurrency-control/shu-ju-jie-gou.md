# 数据结构

#### LockRequest

记录了事务id,请求的锁类型，请求是否发放。

#### LockRequestQueue

一个LockRequest的队列，一个条件变量，一个记录是否有请求升级事务。

#### Lock\_table\_

哈希表，记录了每一个rid对应的request队列。


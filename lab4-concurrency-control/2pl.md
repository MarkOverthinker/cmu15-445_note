# 2PL

即分两个阶段对事务上锁解锁。

GROWING阶段：只能对该事务上锁

SHRINKING阶段：只能对该事务解锁

则一个record只能在GROWING阶段上锁一次，最后再解锁一次。

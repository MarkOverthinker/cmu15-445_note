# 总

lab1: lru, 哈希存储链表中的迭代器，buffer pool

lab2: Extendible Hashing原理，double check、race

lab3:火山模型（关系代数分成多个operator,如project, predicate, scan）、优缺点、两种join、pipeline breaker

{% embed url="https://zhuanlan.zhihu.com/p/219516250" %}

lab4:2pl,增强版2pl避免级联终止，隔离级别RR,RU,RC。would-wait,wait-die,哲学家也可以用。每个RID有一个request队列，保存所有事务请求的锁，每个队列里有一个条件变量用来实现锁等待。RU:只上写锁，不用读锁。几个信号量时机。锁晋升：从锁队列里去掉自己的读锁（必须是已颁发，否则throw异常），去掉这个读锁，然后将自己的写锁请求放到未颁发的所有锁前面。would-wait:上读锁时，高优先级事务abort掉低优先级的写锁；上写锁时，abort掉所有的更低优先级的锁。条件变量(std::condition\_variable)：获取不到锁时wait,可能获取到锁时再被唤醒。唤醒时不一定能获取，可能事务状态检查有问题，也可能没轮到。然后唤醒要用cv\_.notify\_all();

# TASK1\&TASK2

### 思路

对于每一个锁请求，先通过事务状态和隔离性级别判断锁请求是否合理，不合理则直接abort并返回false。查看前面的请求与该请求是否冲突，若冲突则调用条件变量挂起线程，等待能上锁的时候。否则颁发锁并返回true。

在锁队列中，越靠前的锁代表这个锁请求越先到达锁队列。锁是按队列里的顺序颁发的。

#### wound-wait死锁预防策略

时间戳更早的事务请求锁时，若存在时间戳更晚的事务拿到了该锁，则abort该事务，抢到该锁。(wound)

时间戳更晚的事务请求锁时， 若存在时间戳更早的事务拿到了该锁，则阻塞等待该锁释放。（wait)

此策略优先牺牲新事务以确保老事务一定能拿到任何想要的锁。通过老事务主动abort新事务的方式解开死锁。

实验采取该策略

#### wait-die死锁预防策略

时间戳更早的事务请求锁时，若存在时间戳更晚的事务拿到了该锁。则进入等待。（wait)

时间戳更晚的事务请求锁时， 若存在时间戳更早的事务拿到了该锁。则时间戳更晚的这个事务直接abort。(die)

此策略则是只让老事务等待新事务，而不让新事务等待老事务，即新事务不会主动争抢老事务已有的锁，而是直接abort自己以防自己身上还有其他该老事务的锁造成死锁。

### LockShared

判断事务是否abort

判断事务隔离级别是否为read\_uncommited，这个隔离级别不需要读锁。

判断事务状态是否为GROWING,事务只在这个阶段能上锁。

查看该rid对应的锁请求队列，只需关心有无写锁，遍历找到所有的写锁申请，若写锁申请中有更新的事务，则直接abort(wound),将该请求去除，若有更老的事务，则插入本读锁请求后通过条件变量挂起等待老事务解锁。无更老事务的写锁则可直接颁发读锁。

### LockExclusive

与LockShared相似，区别是写锁只能独占锁请求，所以对于队列中的读锁和写锁都要检查时间戳进行死锁预防，且只能在本请求位于请求队列首的时候颁发锁。

### LockUpdate

先检查是否已有该读锁，若有则去掉该读锁和队列中的该读锁请求，之后与LockShared类似，获取一个写锁请求即可。
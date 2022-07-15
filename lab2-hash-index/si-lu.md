# 思路

## DIRECTORY

### SetBucketPageId

更改bucket\_page\_idx数组即可。GetBucketPageId同理。

### GetGlobalDepthMask

获得一个掩码，若取最低几位为1来生成掩码，则直接将1左移GlobalDepth\_位再减一即可。

GetLocalDpethMask同理。

### IncrGlobalDepth

GlobalDepth加一，并且将local\_depths数组和bucket\_page\_ids数组新增的部分设置为其兄弟的值。

### GetSplitImageIndex

得到一个页面的兄弟页面的idx,此函数用于分裂插入，应该返回分裂后兄弟页面的索引，如idx111的bucket分裂后idx变为0111，其兄弟页面则为1111。



## BUCKET

### SetReadable

将readable_数组中元素对应为设为1即可，如SetReadable(17)标识将readable_\_\[2]的第一位设为1.(假设idx从1开始,17/8=2,17%8=1)

SetOccupied,isReadable,isOccupied同理。

### Insert

将(key, value)插入到array\_中，需要通过readable\_数组判断有无空位以及是否已有相同的kv,key的比较需要通过KeyComparator进行。同时设置SetOccupied,SetReadable。

### RemoveAt

将readable\_对应位置0即可。

### IsFull

检查readable\_的每一位是否为1，全为1则bukcet已满。需注意容量不一定为8的倍数，也就是说readable\_最后一个元素有效位数未必是8，可能只有几位用来记录bukcet是否已满，需单独处理最后一个元素。IsEmpty和NumReadable同理,遍历readable\_即可。

### GetValue

遍历readable\_查找array\_中的有效数据，若Key符合要求则加入返回值，遍历完成后返回。



## ExtendibleHashTable

### FetchDirectoryPage

已有directory\_page\_id\_,则向bpm取来Directory。若无，可以创建一个初始的directory和bucket并保存。在处理并发时，此处出现了一个大坑，我最初的实现是先if语句判断有无directory，判断为无时则先上一个写锁，再进行一个"double-check"，再此检查是否无directory,然后创建directory和bucket,为了减少代码，我先创建一个Page \*page=directory，之后再创建bucket,还是使用page = bucket，最后再解锁，完成创建之后再取出所需的页面返回。这样做的隐患在于我先创建了dirctory后创建了bucket，同时我的锁是写在if中的。在并发时，首先一个fetch操作A检查到无directory，上锁并进入创建，这时如果有另一个fetch操作B也执行，按理想的执行方式，若此时另一个A已全部完成，则毫无问题，若A尚未完成，则根据A的进度，会出现不同情况，一部分情况根据double-check可以无事发生，但还有一种情况则导致我的测试出了问题：

在A创建完Directory后，而未创建完成bucket的时候，B操作进行了，此时B会直接取走dirctory,而此时是没有bucket的，这时候进行的插入、删除等操作都会出现问题。

知道问题后解决方法很多，比如将锁加载判断是否有directory之前，整个查询到无-创建directory-创建bucket操作都变成原子操作，但这样每次fetch都要上锁，增加了开销，最初为了改代码方便我使用了这个方式。实际上还有其他方式比如一开始就在构造函数里创建好一个directory和bucket再进行操作。还有种方式原以为可以思考了下其实也不可，即先创建bucket再创建dirctory，这种方式也会出错，因为将bucket\_page\_id存入到directory中的操作一定发生在directory创建后，就一定会出现竞争(race)问题。

### GetValue

通过key在directory中找到所需的bucket，再调用bucket的函数即可。上好读锁即可

### Insert

通过key在directory中找到所需的bucket，调用再bucket中实现的函数判断bucket是否已满，未满则直接写入，之后再返回插入结果(bool),若bukcet已满，则需使用SplitInsert分裂bucket并插入。directory上好读锁，bucket上好写锁。

### SplitInsert

首先取到需要的bucket，增加local\_depth（若global\_depth等于local\_depth的话需要先扩张directory,global\_depth始终大于等于local\_depth）,之后得到兄弟bucket在directory中的索引,然后将原bucket的数据拷贝出来，清空两个bucket并将原数据分配给两个bucket,最后将directory到bucket的索引重新构建好(构建方式上上篇已解释)。之后再执行Insert进行一次插入。

不直接在此函数插入而调用Insert插入的原因：极端情况下分裂后可能一个bucket仍未满而另一个为空，需要再次调用Insert插入，若出现这种情况则会再次进入SplitInsert。

由于存在两个函数之间的跳转，而跳转前会进行解锁，进入新函数后才再次上锁，所以很容易出现并发中的竞争问题：

在Insert跳转到SplitInsert的时候，Insert先解锁，之后还未进行跳转，另外一些操作先进行，锁住了同一个bucket和directory并对其值进行修改，修改完后释放锁，被阻塞的SplitInsert才开始执行，此时bucket已发生变化，若直接进行分裂插入操作，就可能出现问题。一个解决方式就是进行前文用到的double-check,在进入SplitInsert后再次进行Insert中的判断，判断是否需要进行SplitInsert操作，若bucket确实已满需要分裂再插入，则再进行分裂并插入，否则函数直接返回Insert进行插入。

### Remove

同样是取到需要的页，上锁后调用bucket里的函数进行remove。remove成功之后，若页为空，则可调用Merge考虑进行页的合并。需要小心的是local\_depth为0时不能再进行合并了。

### Merge

在Merge中同样需要取到bucket和他的兄弟bucket,若他们的深度相同，可以开始合并。为了尽可能减少页的抖动，即频繁的创建删除页，我选择只在该页为空并且兄弟页面的容量小于一半时才进行合并。课程中也有地方提到可以采取合并不删除page或不合并的方式来减少页面的抖动。当然采取页面为空则立马合并的方式也是可以的。

合并过程：减少bucket的local\_depth,并同步到directory中。之后判断directory是否需要收缩。再将directory到bucket的映射重新构建。最后删除空页。

合并中需要注意多次连续合并的出现。

并发情况下存在和SplitInsert类似的竞争问题，在Remove跳转到Merge的中途页面发生了改变导致原本需要Merge的页面又不需要Merge了。在这里同样可以用double-check解决。

# BUFFER POOL MANAGER INSTANCE

### Page 与Frame

page是存储数据的单元，DBMS把文件分为很多个page来处理。每个page都有一个page\_id用于标识它。在buffer pool中，为了提高数据读写效率，将一些page缓存到buffer pool中，这些缓存到内存中的page，被称为frame，DBMS会维护一个叫做frame\_id的东西来记录这些frame的位置。区分frame\_id与page\_id是实验中重要的部分。

### 数据结构

Page \*page\_：frame的链表，链表索引即frame\_id。

Replacer \*replacer\_ ：用来决策替换frame时应牺牲哪个frame。

std::list\<frame\_id> free\_list\_：记录哪些frame空闲的，即可用来缓冲新的page。

std::unordered\_map\<page\_id, frame\_id>  page\_table\_:用来记录一个page被缓冲到哪个frame之中。

### 函数实现

#### 构造函数：

初始化各成员变量，申请所需的内存

#### 析构函数

释放申请的内存

#### FetchPgImp

先判断page\_id是否为INVALID\_PAGE

之后在page\_table\_里查找所需的页，若找到，说明所需的页已经缓存到了内存中，通过page\_table\_得到该page在内存中的frame\_id，在page\_中通过frame\_id得到该页，增加该页的pin\_count，并调用replacer\_的pin函数，然后用参数返回该页，函数返回true

若在page\_table\_中没找到该页，说明该页还未缓存到buffer\_pool\_中，需要从磁盘中缓存该页到bool\_pool中，则需要查找一个frame用于缓冲该page，先在free\_list\_中查找是否有空frame，若有则使用该frame，否则需要使用使用前面实现的replacer的victim方法（未找到则返回false），来找到一个需要牺牲的frame，再将page替换进该frame中。替换前需检查该frame是否为脏页，若为脏页需要将其内容先写回磁盘。最后将新缓存进来的page记录在page\_table\_以及replacer中。

#### UnpinPgImp

先判断传入的page是否合法

传入is\_dirty为true，则page记录为脏页。为false,则不管，而不能将page的is\_dirty\_设为false。

检查pin\_count，为0则无法unpin，返回false

pin\_count减一。再次检查pin\_count，为0说明没有人再使用该页，可调用replace的unpin函数，使该页加入可被替换列表。

#### NewPgImp

用类似于FetchPgImp中的方法得到一个frame，将该frame初始化（ResetMemory()函数+is\_dirty\_置false），清空数据为一个新页，调用AllocatePage在磁盘上创建一个page并得到page\_id，再将frame的page\_id设为该page\_id，再调用FlushPgImp将该frame刷入磁盘。最后类似于FetchPgImp，对该页上锁并返回该页。

#### FlushPgImp

在内存中找到要写回磁盘的页并调用disk\_manager\_的Write\_Page函数将页写回即可。

#### DeletePgImp

先找到该页是否存在并查看是否有锁

检查没问题则先对frame上锁，调用DeallocatePage删除物理page，将frame初始化,从page\_table中删除对应条目，replacer中unpin，解锁，将该frame加回free\_list。

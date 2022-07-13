# Extendible Hash

Extendible Hash是本次实验的核心。

本篇对于Extendible Hash的原理进行一个基本的记录。

![Extendible Hash总览](<../.gitbook/assets/extendible hash.jpg>)

顾名思义，Extendible Hash是一种Dynamic Hash,可以根据实际情况进行表的扩容及收缩。

从如图的结构上可以体现出Extendible Hash的特点：

* 首先，数据存储在一个个的bucket(即右列三个桶)之中，而Extendible设置了一个directory(即左列容器),用来指示某个key值对应数据存储在哪个bucket之中。在执行插入、查找、删除之类操作时，通过对key进行Hash操作，得到所需的directory\_index,再通过他在directory中找到存储所需数据的bucket（directory的每个位置用于指向一个bucket)，最后进入这个bucket之中查找得到所需数据。
* 其次，对于directory，存在一个globaldepth(全局深度),对于每一个bucket，存在一个localdepth。两者的作用分别在于，globaldepth用来指示一个key会映射到directory的哪个位置,从而得到这个key映射到了哪个bucket，而localdepth则在真正意义上**反映**了哪些key会映射到哪个bucket。               举例说明：现有两个key，一个k1,一个k2，hash(k1) = 001011,hash(k2) = 010111,此时globaldepth为2，按图中的方式可取hash(k1)和hash(k2)的最高两位（也可取最低两位,我在实验中使用的是最低n位）,分别得到00 和 01，则对于k1，可以得到k1映射在directory的第00(即十进制0）位，此位指向第一个bucket,说明k1映射到第一个bucket之中。同理，k2映射在directory的第01(即十进制1）位，此位指向第一个bucket，说明k2映射到第一个bucket之中。而对于两个key来说，他们所在的localdepth都为1，取他们的哈希值最高1位得到的都是0,这也反映了只要哈希值最高位取0的key最终一定会映射到第一个bucket中，因为同一个bucket中，Hash(key) & local\_depthmass得到的值一定相同(local\_depth\_mass表示通过一个掩码作位运算取哈希值的最高local\_depth位或最低local\_depth位)。
* 分裂(split)操作：从使用一个page来存储一个bucket的角度来说，bucket的容量是有限的。同时从其他角度来说，若一个bucket无限扩张，Hash Table最终和一个线性链表结构也没有区别，解决办法就是使用分裂操作，即在插入一个容量已满的bucket时，需要将其分裂为两个新的bucket,操作如下：          先判断当前bucket的local\_depth是否小于global\_depth，若是，则将local\_depth加一，并创建一个相同local\_depth的bucket,再根据新的local\_depth重新分配数据到两个bucket中(Hash(key) & local\_depthmass增加了一位，所以可以利用这一位来区分数据)。若local\_depth等于global\_depth,则两者都增加，并重新分配。理解“local\_depth反映了key映射在哪这个概念再看图将很容易理解如何重新分配”，但是这并不意味着重新分配完全是依赖local\_depth，对于未分裂的页，只需要将其对应的directory中新产生的位置映射到他的兄弟位置映射的bucket中即可(如下图中的000 和001，由于此图采用取最高几位的方式且他俩仅最低一位不同，说明他们是兄弟位置，只需将001映射到000指向的位置即可),真正使用到local\_depth去进行重新分配的地方在于进行分裂的两个页面，需要通过localdepth\_mass去决定directory的某些位置应该映射在两个bucket中的哪一个。

![split一次](<../.gitbook/assets/extendible hash2.jpg>)

* 合并(Merge)操作：与分裂相反，在某些情况下（如一个页面容量和兄弟页面容量都很小，如某个页面为空，或者某个页面为空且兄弟页面容量也很小），可考虑将一个页面与其兄弟页面合并为一个页面（删除这个页面），并减少local\_depth，若合并后所有页面的locla\_depth都小于global\_depth，还可压缩减少global\_depth,压缩directory，并重新分配directory(原理可参考分裂)，某些情况下也可考虑不合并以减少页面抖动。在实验中需注意连续的合并操作亦有可能存在。

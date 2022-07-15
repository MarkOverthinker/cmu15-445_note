# 实验要求

全面要求参见官网：

{% embed url="https://15445.courses.cs.cmu.edu/fall2021/project2/" %}

**本次实验要求用Extendible Hashing 实现一个Hash Index，可分为三部分实现。**

#### PAGE LAYOUTS

_**Hash\_Table\_Directory\_Page类**_,用于组织HashTable的directory的page形式。

成员变量包括：

* page\_id\_:记录对应物理页的page\_id;
* lsn\_:日志序列号，实验四使用；
* global\_depth\_;
* local\_depth\_:512bytes的数组，用于记录所有桶的local\_depth;
* bucket\_page\_ids\_:2048bytes的数组，用于记录所有桶的物理页page\_id;

需要实现的函数大部分看注释或函数名一眼即懂，以下仅记录部分：

* GetBucketPageId(uint32\_t bucket\_idx):  得到桶bucket\_idx的page\_id,即bucket\_page\_ids\[bucket\_idx];
* SetBucketPageId(uint32\_t bucket\_idx, page\_id\_t bucket\_page\_id):  即bucket\_page\_ids\[bucket\_idx] = bucket\_page\_id);
* GetSplitImageIndex(uint32\_t bucket\_idx):  得到bucket\_idx分裂后兄弟页面的idx;
* GetGlobalDepthMask():  得到全局深度的掩码；如3的掩码可以为0000...000111或111...0000；
* GetLocalDepthMask(uint32\_t bucket\_idx):  返回对应bucket的Local\_Depth的掩码；
* Canshrink():  判断directory是否可压缩；
* PrintDirectory():  打印当前directory结构信息，可直观看到当前directory的深度、bucket数、directory各位映射到的bucket,挺好用。



_**Hash\_Table\_Bucket\_Page**_类，用于描述每个bucket在Page中的组织形式

成员变量：

* occupied\_\[(BUCKET\_ARRAY\_SIZE - 1) / 8 + 1]：用于记录一个数据位是否被占据过，每一位表示一个数据位，1表示被占据过，0表示未被占据过。
* readable\_\[(BUCKET\_ARRAY\_SIZE - 1) / 8 + 1]:  用于记录一个数据位是否有数据。
* array\_\[0]:存储数据。这里的“0”是一种特殊写法，表示一个指针，指向结构体/类中声明这个数组的位置，不占实际内存空间（因为数组名表示一个偏移量）。

成员函数：

基本就是操作三个成员变量，理解三个成员变量即可。

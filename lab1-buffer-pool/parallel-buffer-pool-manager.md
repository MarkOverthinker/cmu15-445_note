# PARALLEL BUFFER POOL MANAGER

思路

该类管理了num\_instance个前面实现的buffer pool manager instance。所有页按对num\_instance取模的方式平均分配给这些bpm，在收到对某个页的请求后，调用该页对应的bpm去处理即可。

> 本文基于 Netty 4.1.112.Final 版本进行讨论

本来笔者是想在上篇文章 [《谈一谈 Netty 的内存管理 —— 且看 Netty 如何实现 Java 版的 Jemalloc》](https://mp.weixin.qq.com/s?__biz=Mzg2MzU3Mjc3Ng==&mid=2247490143&idx=1&sn=b85a1fe4be6578af8e968ef99fd6dd33&chksm=ce77dc18f900550ee7aa39c8e30c46eaabe984adf82c88a85abec5fd62d3cdf03af6928d9459&scene=21&cur_album_id=2217816582418956300#wechat_redirect) 中的最后一个小节，介绍一下 Netty 内存池所提供的所有 Metrics ， 从内存池监控的角度为文章收一个尾，没想到字数超过了公众号的限制，只能将最后一个小节删除，现在来给大家把这个小节补上。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUa0Arojwd3W7iaSrY7FIr5viamNjgxibzicXkQ0Ekq6AVzAvhtHwIIQrriaZoZDYl6NibtZHCaJJnugXCOQ/640?wx_fmt=other&from=appmsg#imgIndex=0)

3.png

## 9\. 内存池相关的 Metrics

为了更好的监控内存池的运行状态，Netty 为内存池中的每个组件都设计了一个对应的 Metrics 类，用于封装与该组件相关的 Metrics。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUa0Arojwd3W7iaSrY7FIr5viahEibx7fiaoG56rst8JnFiaT7pPiczI1Kz275RxhXj17gLGd3nWkvKDYKnQ/640?wx_fmt=other&from=appmsg#imgIndex=1)

image.png

其中内存池 PooledByteBufAllocator 提供的 Metrics 如下：

```
public final class PooledByteBufAllocatorMetric implements ByteBufAllocatorMetric {

   private final PooledByteBufAllocator allocator;

   PooledByteBufAllocatorMetric(PooledByteBufAllocator allocator) {
        this.allocator = allocator;
   }
   // 内存池一共有多少个 PoolArenas
   public int numDirectArenas() {
        return allocator.numDirectArenas();
   }
   // 内存池中一共绑定了多少线程
   public int numThreadLocalCaches() {
        return allocator.numThreadLocalCaches();
   }
   // 每一种 Small 规格最大可缓存的个数，默认为 256
   public int smallCacheSize() {
        return allocator.smallCacheSize();
   }
   // 每一种 Normal 规格最大可缓存的个数，默认为 64
   public int normalCacheSize() {
        return allocator.normalCacheSize();
   }
   // 内存池中 PoolChunk 的尺寸大小，默认为 4M
   public int chunkSize() {
        return allocator.chunkSize();
   }
   // 该内存池目前一共向 OS 申请了多少内存（包括 Huge 规格）单位为字节
   @Override
   public long usedDirectMemory() {
        return allocator.usedDirectMemory();
   }
```

PoolArena 提供的 Metrics 如下：

```
abstract class PoolArena<T> implements PoolArenaMetric {
    // 一共有多少线程与该 PoolArena 进行绑定
    @Override
    public int numThreadCaches() {
        return numThreadCaches.get();
    }
    // 一共有多少种 Small 规格尺寸，默认为 39
    @Override
    public int numSmallSubpages() {
        return smallSubpagePools.length;
    }
    // 该 PoolArena 中一共包含了几个 PoolChunkList
    @Override
    public int numChunkLists() {
        return chunkListMetrics.size();
    }
    // 该 PoolArena 总共分配内存块的次数（包含 Small 规格，Normal 规格，Huge 规格）
    // 一直累加，内存块释放不递减
    long numAllocations();
    // PoolArena 总共分配 Small 规格的次数，一直累加，释放不递减
    long numSmallAllocations();
    // PoolArena 总共分配 Normal 规格的次数，一直累加，释放不递减
    long numNormalAllocations();
    // PoolArena 总共分配 Huge 规格的次数，一直累加，释放不递减
    long numHugeAllocations();
    // 该 PoolArena 总共回收内存块的次数（包含 Small 规格，Normal 规格，Huge 规格）
    // 一直累加   
    long numDeallocations();
    // PoolArena 总共回收 Small 规格的次数，一直累加
    long numSmallDeallocations();
    // PoolArena 总共回收 Normal 规格的次数，一直累加
    long numNormalDeallocations();
    // PoolArena 总共释放 Huge 规格的次数，一直累加
    long numHugeDeallocations();
    // PoolArena 当前净内存分配次数
    // 总 Allocations 计数减去总 Deallocations 计数
    long numActiveAllocations();
    // 同理，PoolArena 对应规格的净内存分配次数
    long numActiveSmallAllocations();
    long numActiveNormalAllocations();
    long numActiveHugeAllocations();
    // 该 PoolArena 目前一共向 OS 申请了多少内存（包括 Huge 规格）单位为字节
    long numActiveBytes();
}
```

PoolSubpage 提供的 Metrics 如下：

```
final class PoolSubpage<T> implements PoolSubpageMetric {
    // 该 PoolSubpage 一共可以切分出多少个对应 Small 规格的内存块
    @Override
    public int maxNumElements() {
        return maxNumElems;
    }
    // 该 PoolSubpage 当前还剩余多少个未分配的内存块
    int numAvailable();
    // PoolSubpage 管理的 Small 规格尺寸
    int elementSize();
    // 内存池的 pageSize，默认为 8K
    int pageSize();
}
```

PoolChunkList 提供的 Metrics 如下：

```
final class PoolChunkList<T> implements PoolChunkListMetric {
    // 该 PoolChunkList 中管理的 PoolChunk 内存使用率下限，单位百分比
    @Override
    public int minUsage() {
        return minUsage0(minUsage);
    }
    // 该 PoolChunkList 中管理的 PoolChunk 内存使用率上限，单位百分比
    @Override
    public int maxUsage() {
        return min(maxUsage, 100);
    }
}
```

PoolChunk 提供的 Metrics 如下：

```
final class PoolChunk<T> implements PoolChunkMetric {
    // 当前 PoolChunk 的内存使用率，单位百分比
    int usage();
    // 默认 4M
    int chunkSize();
    // 当前 PoolChunk 中剩余内存容量，单位字节
    int freeBytes();
}
```
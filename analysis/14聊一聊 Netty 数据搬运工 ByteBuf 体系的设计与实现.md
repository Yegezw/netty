> 本文基于 Netty 4.1.56.Final 版本进行讨论

时光芿苒，岁月如梭，好久没有给大家更新 Netty 相关的文章了，在断更 Netty 的这段日子里，笔者一直在持续更新  [Linux 内存管理相关的文章](https://mp.weixin.qq.com/mp/appmsgalbum?__biz=Mzg2MzU3Mjc3Ng==&action=getalbum&album_id=2559805446807928833&scene=173&from_msgid=&from_itemidx=&count=3&nolastread=1&scene=21#wechat_redirect) ，目前为止，算是将 Linux 内存管理子系统相关的主干源码较为完整的给大家呈现了出来，同时也结识了很多喜欢内核的读者，经常在后台留言讨论一些代码的设计细节，在这个过程中，我们相互分享，相互学习，浓浓的感受到了大家对技术那份纯粹的热爱，对于我自己来说，也是一种激励，学习，提高的机会。

之前系列文章的视角一直是停留在内核态，笔者试图从 Linux 内核的角度来为大家揭秘内存管理的本质，那么从今天开始，我们把视角在往上挪一挪，从内核态转换到用户态，继续沿着内存管理这条主线，来看一看用户态的内存管理是如何进行的。

接下来笔者计划用三篇文章的篇幅为大家剖析一下 Netty 的内存管理模块，本文是第一篇，主要是围绕 Netty 内存管理的外围介绍一下  ByteBuf 的总体设计。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFDO9aZqF2Mw3JOmgJYRcyzVGlaXvjUGeQ4SgpZnplT5QaXfLSBQialhg/640?wx_fmt=other&from=appmsg#imgIndex=0)

image.png

别看 ByteBuf 体系涉及到的类比较多，一眼望过去比较头大，但是我们按照不同的视角，将它们一一分类，整个体系脉络就变得很清晰了：

*   从 JVM 内存区域布局的角度来看，Netty 的 ByteBuf 主要分为 HeapByteBuf（堆内） 和 DirectByteBuf（堆外）这两种类型。
    
*   从内存管理的角度来看，Netty 的 ByteBuf  又分为 PooledByteBuf （池化）和 UnpooledByteBuf（非池化）两种子类型。一种是被内存池统一管理，另一种则和普通的 ByteBuf 一样，用的时候临时创建，不用的时候释放。
    
*   从内存访问的角度来看，Netty 又将 ByteBuf 分为了 UnsafeByteBuf 和普通的 ByteBuf。UnsafeByteBuf 主要是依赖 Unsafe 类提供的底层 API 来直接对内存地址进行操作。而普通 ByteBuf 对内存的操作主要是依赖 NIO 中的 ByteBuffer。
    
*   从内存回收的角度来看，ByteBuf 又分为了带 Cleaner 的 ByteBuf 以及不带 Cleaner 的 NoCleanerByteBuf，Cleaner 在 JDK 中是用来释放 NIO ByteBuffer 背后所引用的 Native Memory 的，内存的释放由 JVM 统一管理。而 NoCleanerByteBuf 背后的 Native Memory 则需要我们进行手动释放。
    
*   从内存占用统计的角度来说，Netty 又近一步将 ByteBuf 分为了 InstrumentedByteBuf 和普通的 ByteBuf，其中 InstrumentedByteBuf 会带有内存占用相关 Metrics 的统计供我们进行监控，而普通的 ByteBuf 则不带有热任何 Metrics。
    
*   从零拷贝的角度来看，Netty 又引入了 CompositeByteBuf，目的是为多个 ByteBuf 在聚合的时候提供一个统一的逻辑视图，将多个 ByteBuf 聚合成一个逻辑上的 CompositeByteBuf，而传统的聚合操作则是首先要分配一个大的 ByteBuf，然后将需要聚合的多个 ByteBuf 中的内容在拷贝到新的 ByteBuf 中。CompositeByteBuf 避免了分配大段内存以及内存拷贝的开销。注意这里的零拷贝指的是 Netty 在用户态层面自己实现的避免内存拷贝的设计，而不是 OS 层面上的零拷贝。
    
*   另外 Netty 的 ByteBuf 支持引用计数以及自动地内存泄露探测，如果有内存泄露的情况，Netty 会将具体发生泄露的位置报告出来。
    
*   Netty 的 ByteBuf 支持扩容，而 NIO 的 ByteBuffer 则不支持扩容，
    

在将 Netty 的 ByteBuf 设计体系梳理完整之后，我们就会发现，Netty 的 ByteBuf 其实是对 JDK  ByteBuffer 的一种扩展和完善，所以下面笔者的行文思路是与 JDK  ByteBuffer 对比着进行介绍 Netty 的 ByteBuf ，有了对比，我们才能更加深刻的体会到 Netty 设计的精妙。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFbGI1qeUDPCWOQ1wQJOkmZNsxp4tzyCFGguYo93zicZfOhUVPlsvqH2w/640?wx_fmt=other&from=appmsg#imgIndex=1)

本文概要.png

## 1\. JDK 中的 ByteBuffer 设计有何不妥

笔者曾在 [《一步一图带你深入剖析 JDK NIO ByteBuffer 在不同字节序下的设计与实现》](https://mp.weixin.qq.com/s?__biz=Mzg2MzU3Mjc3Ng==&mid=2247485497&idx=1&sn=eb4afe6764b2b976fb80f6dc5c6fd68a&chksm=ce77ce7ef900476865864e09bb6f0688ca784afc396084ecc90a894bfd733692049c332edd11&token=927203489&lang=zh_CN&scene=21#wechat_redirect) 一文中完整的介绍过 JDK ByteBuffer 的整个设计体系，下面我们来简短回忆一下 ByteBuffer 的几个核心要素。

```
public abstract class Buffer {
    private int mark = -1;
    private int position = 0;
    private int limit;
    private int capacity;
}
```

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xF1R0dXaqIblueVOTB63f9ia4ibdwMribrrLpMelRzUlGjtduibk5lzn4cFg/640?wx_fmt=other&from=appmsg#imgIndex=2)

image.png

*   capacity 规定了整个 Buffer 的容量，具体可以容纳多少个元素。capacity 之前的元素均是 Buffer 可操作的空间，JDK 中的 ByteBuffer 是不可扩容的。
    
*   position 用于指向 Buffer 中下一个可操作性的元素，初始值为 0。对于 Buffer 的读写操作全部都共用这一个 position 指针，在 Buffer 的写模式下，position 指针用于指向下一个可写位置。在读模式下，position 指针指向下一个可读位置。
    
*   limit 用于限定 Buffer 可操作元素的上限，position 指针不能超过 limit。
    

由于 JDK ByteBuffer 只设计了一个 position 指针，所以我们在读写 ByteBuffer 的时候需要不断的调整 position 的位置。比如，利用 flip() ，rewind()，compact()，clear() 等方法不断的进行读写模式的切换。

一些具体的场景体现就是，当我们对一个 ByteBuffer 进行写入的时候，随着数据不断的向 ByteBuffer 写入，position 指针会不断的向后移动。在写入操作完成之后，如果我们想要从 ByteBuffer 读取刚刚写入的数据就麻烦了。

由于 JDK 在对 ByteBuffer 的设计中读写操作都是混用一个 position 指针，所以在读取 ByteBuffer 之前，我们还需要通过  flip() 调整 position 的位置，进行读模式的切换。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFMMXqfVQNHf3NMAL8jtIweFibenz9fj54EOGAR1VVkG6kIaMMsS31EtA/640?wx_fmt=other&from=appmsg#imgIndex=3)

flip切换读模式.png

```
    public final Buffer flip() {
        limit = position;
        position = 0;
        mark = -1;
        return this;
    }
```

当我们将 ByteBuffer 中的数据全部读取完之后，如果再次向 ByteBuffer 写入数据，那么还需要重新调整 position 的位置，通过 clear()  来进行写模式的切换。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFwicFwo9F9sJfHcEEf2icPYv1RXc96aibGdicZ4grL6Z5qrfrhqUpBhsGXA/640?wx_fmt=other&from=appmsg#imgIndex=4)

clear切换写模式.png

```
    public final Buffer clear() {
        position = 0;
        limit = capacity;
        mark = -1;
        return this;
    }
```

如果我们只是部分读取了 ByteBuffer 中的数据而不是全部读取，那么在写入的时候，为了避免未被读取的部分被接下来的写入操作覆盖，我们则需要通过 compact() 方法来切换写模式。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xF2Fv9iaCgTtPGVW3xCGosymicEiaW5l8lXb9WLRYQ7dCde1XbLr2ibACHmQ/640?wx_fmt=other&from=appmsg#imgIndex=5)

comapct切换写模式.png

```
class HeapByteBuffer extends ByteBuffer {

    //HeapBuffer中底层负责存储数据的数组
    final byte[] hb; 

    public ByteBuffer compact() {
        System.arraycopy(hb, ix(position()), hb, ix(0), remaining());
        position(remaining());
        limit(capacity());
        discardMark();
        return this;
    }

    public final int remaining() {
        return limit - position;
    }

   final void discardMark() {                          
        mark = -1;
    }
}
```

从上面列举的这些读写 ByteBuffer 场景可以看出，当我们在操作 ByteBuffer 的时候，需要时刻保持头脑清醒，对 ByteBuffer 中哪些部分是可读的，哪些部分是可写的要有一个清醒的认识，稍不留神就会出错。在复杂的编解码逻辑中，如果使用 ByteBuffer 的话，就需要不断的进行读写模式的切换，切的切的人就傻了。

除了对 ByteBuffer 的相关操作比较麻烦之外，JDK 对于 ByteBuffer 没有设计池化管理机制，而面对大量需要使用堆外内存的场景，我们就需要不断的创建 DirectBuffer，DirectBuffer 在使用完之后，回收又是个问题。

JDK 自身对于 DirectBuffer 的回收是有延迟的，我们需要等到一次 FullGc ，这些 DirectBuffer 背后引用的 Native Memory 才能被 JVM 自动回收。所以为了及时回收这些 Native Memory ，我们又需要操心 DirectBuffer 的手动释放。

JDK 的 ByteBuffer 不支持引用计数，没有引用计数的设计，我们就无从得知一个 DirectBuffer 被引用了多少次，又被释放了多少次，面对 DirectBuffer 引起的内存泄露问题，也就无法进行自动探测。

另外 JDK 的 ByteBuffer 不支持动态按需自适应扩容，当一个 ByteBuffer 被创建出来之后，它的容量就固定了。但实际上，我们很难在一开始就能准确的评估出到底需要多大的 ByteBuffer。分配的容量大了，会造成浪费。分配的容量小了，我们又需要每次在写入的时候判断剩余容量是否足够，如果不足，又需要手动去申请一个更大的 ByteBuffer，然后在将原有 ByteBuffer 中的数据迁移到新的 ByteBuffer 中，想想都麻烦。

还有就是当多个 JDK 的 ByteBuffer 在面对合并聚合的场景，总是要先创建一个更大的 ByteBuffer，然后将原有的多个 ByteBuffer 中的内容在拷贝到新的 ByteBuffer 中。这就涉及到了内存分配和拷贝的开销。

那为什么不能利用原有的这些 ByteBuffer 所占用的内存空间，在此基础上只创建一个逻辑上的视图 ByteBuffer，将对视图 ByteBuffer 的逻辑操作全部转移到原有的内存空间上，这样一来不就可以省去重新分配内存以及内存拷贝的开销了么 ？

下面我们就来一起看下，Netty 中的 ByteBuf 是如何解决并完善上述问题的~~~

## 2\. Netty  对于 ByteBuf 的设计与实现

在之前介绍 JDK ByteBuffer 整体设计的时候，笔者是以 HeapByteBuffer 为例将 ByteBuffer 的整个设计体系串联起来的，那么本文笔者将会用 DirectByteBuf 为大家串联 Netty ByteBuf 的设计体系。

### 2.1 ByteBuf 的基本结构

```
public abstract class AbstractByteBuf extends ByteBuf {
    int readerIndex;
    int writerIndex;
    private int markedReaderIndex;
    private int markedWriterIndex;
    private int maxCapacity;
}

public class UnpooledDirectByteBuf extends AbstractReferenceCountedByteBuf {
    private int capacity;
}
```

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFQYUNduNW4KgkPYnObJbxmZ33ldMCzeLT4FSzVa0XSo44bVAcaMVWCw/640?wx_fmt=other&from=appmsg#imgIndex=6)

image.png

为了避免 JDK ByteBuffer 在读写模式下共用一个 position 指针所引起的繁琐操作，Netty 为 ByteBuf 引入了两个指针，readerIndex 用于指向 ByteBuf 中第一个可读字节位置，writerIndex 用于指向 ByteBuf 中第一个可写的字节位置。有了这两个独立的指针之后，我们在对 Netty ByteBuf 进行读写操作的时候，就不需要进行繁琐的读写模式切换了。与之对应的 markedReaderIndex，markedWriterIndex 用于支持 ByteBuf 相关的 mark 和 reset 操作，这一点和 JDK  中的设计保持一致。

```
    @Override
    public ByteBuf markReaderIndex() {
        markedReaderIndex = readerIndex;
        return this;
    }

    @Override
    public ByteBuf resetReaderIndex() {
        readerIndex(markedReaderIndex);
        return this;
    }

    @Override
    public ByteBuf markWriterIndex() {
        markedWriterIndex = writerIndex;
        return this;
    }

    @Override
    public ByteBuf resetWriterIndex() {
        writerIndex(markedWriterIndex);
        return this;
    }
```

由于 JDK ByteBuffer 在设计上不支持扩容机制，所以 Netty 为 ByteBuf 额外引入了一个新的字段 maxCapacity，用于表示 ByteBuf 容量最多只能扩容至 maxCapacity。

```
    @Override
    public int calculateNewCapacity(int minNewCapacity, int maxCapacity) {
        if (minNewCapacity > maxCapacity) {
            throw new IllegalArgumentException(String.format(
                    "minNewCapacity: %d (expected: not greater than maxCapacity(%d)",
                    minNewCapacity, maxCapacity));
        }
    }
```

Netty ByteBuf 的 capacity 与 JDK ByteBuffer 中的 capacity 含义保持一致，用于表示 ByteBuf 的初始容量大小，也就是下面在创建 UnpooledDirectByteBuf 的时候传入的 initialCapacity 参数。

```
public class UnpooledDirectByteBuf extends AbstractReferenceCountedByteBuf {
      // Netty ByteBuf 底层依赖的 JDK ByteBuffer
      ByteBuffer buffer;
      // ByteBuf 初始的容量，也是真正的内存占用
      private int capacity;

      public UnpooledDirectByteBuf(ByteBufAllocator alloc, int initialCapacity, int maxCapacity) {
        // 设置最大可扩容的容量
        super(maxCapacity);
        this.alloc = alloc;
        // 按照 initialCapacity 指定的初始容量，创建 JDK ByteBuffer
        setByteBuffer(allocateDirect(initialCapacity), false);
    }

    void setByteBuffer(ByteBuffer buffer, boolean tryFree) {
        // UnpooledDirectByteBuf 底层会依赖一个 JDK 的 ByteBuffer
        // 后续对 UnpooledDirectByteBuf 的操作， Netty 全部会代理到 JDK ByteBuffer 中
        this.buffer = buffer;
        // 初始指定的 ByteBuf 容量 initialCapacity
        capacity = buffer.remaining();    
    }
}
```

由此一来，Netty 中的 ByteBuf 就会被 readerIndex，writerIndex，capacity，maxCapacity 这四个指针分割成四个部分，上图中笔者以按照不同的颜色进行了区分。

*   其中 `[0 , capacity)` 这部分是创建 ByteBuf 的时候分配的初始容量，这部分是真正占用内存的，而 `[capacity , maxCapacity)` 这部分表示 ByteBuf 可扩容的容量，这部分还未分配内存。
    
*   `[0 , readerIndex)` 这部分字节是已经被读取过的字节，是可以被丢弃的范围。
    
*   `[readerIndex , writerIndex)` 这部分字节表示 ByteBuf 中可以被读取的字节。
    
*   `[writerIndex , capacity)` 这部分表示 ByteBuf 的剩余容量，也就是可以写入的字节范围。
    

这四个指针他们之间的关系为 ：`0 <= readerIndex <= writerIndex <= capacity <= maxCapacity`。

```
  private static void checkIndexBounds(final int readerIndex, final int writerIndex, final int capacity) {
        if (readerIndex < 0 || readerIndex > writerIndex || writerIndex > capacity) {
            throw new IndexOutOfBoundsException(String.format(
                    "readerIndex: %d, writerIndex: %d (expected: 0 <= readerIndex <= writerIndex <= capacity(%d))",
                    readerIndex, writerIndex, capacity));
        }
    }
```

当我们对 ByteBuf 进行读取操作的时候，需要通过 `isReadable` 判断 ByteBuf 是否可读。以及通过 `readableBytes` 判断 ByteBuf 具体还有多少字节可读。当 readerIndex 等于 writerIndex 的时候，ByteBuf 就不可读了。 `[0 , readerIndex)` 这部分字节就可以被丢弃了。

```
    @Override
    public boolean isReadable() {
        return writerIndex > readerIndex;
    }

    @Override
    public int readableBytes() {
        return writerIndex - readerIndex;
    }
```

当我们对 ByteBuf 进行写入操作的时候，需要通过 `isWritable` 判断 ByteBuf 是否可写。以及通过 `writableBytes` 判断 ByteBuf 具体还可以写多少字节。当 writerIndex 等于 capacity 的时候，ByteBuf 就不可写了。

```
   @Override
    public boolean isWritable() {
        return capacity() > writerIndex;
    }

    @Override
    public int writableBytes() {
        return capacity() - writerIndex;
    }
```

当 ByteBuf 的容量已经被写满，变为不可写的时候，如果继续对 ByteBuf 进行写入，那么就需要扩容了，但扩容后的 capacity 最大不能超过 maxCapacity。

```
    final void ensureWritable0(int minWritableBytes) {
        // minWritableBytes 表示本次要写入的字节数
        // 获取当前 writerIndex 的位置
        final int writerIndex = writerIndex();
        // 为满足本次的写入操作，预期的 ByteBuf 容量大小
        final int targetCapacity = writerIndex + minWritableBytes;
        // 如果 targetCapacity 在（capacity , maxCapacity] 之间，则进行扩容
        if (targetCapacity >= 0 & targetCapacity <= capacity()) {
            // targetCapacity 在 [0 , capacity] 之间，则无需扩容，本来就可以满足
            return;
        }
        // 扩容后的 capacity 最大不能超过 maxCapacity
        if (checkBounds && (targetCapacity < 0 || targetCapacity > maxCapacity)) {
            throw new IndexOutOfBoundsException(String.format(
                    "writerIndex(%d) + minWritableBytes(%d) exceeds maxCapacity(%d): %s",
                    writerIndex, minWritableBytes, maxCapacity, this));
        }

        ..... 扩容 ByteBuf ......
    }
```

### 2.2 ByteBuf 的读取操作

明白了 ByteBuf 基本结构之后，我们来看一下针对 ByteBuf 的读写等基本操作是如何进行的。Netty 支持以多种基本类型为粒度对 ByteBuf 进行读写，除此之外还支持 `Unsigned` 基本类型的转换以及大小端的转换。下面笔者以 Byte 和 Int 这两种基本类型为例对 ByteBuf 的读取操作进行说明。

ByteBuf 中的 get 方法只是单纯地从 ByteBuf 中读取数据，并不改变其 readerIndex 的位置，我们可以通过 `getByte` 从 ByteBuf 中的指定位置 index 读取一个 Byte 出来，也可以通过 `getUnsignedByte` 从 ByteBuf 读取一个 Byte 并转换成 `UnsignedByte` 。

```
public abstract class AbstractByteBuf extends ByteBuf {
    @Override
    public byte getByte(int index) {
        // 检查 index 的边界，index 不能超过 capacity（index < capacity）
        checkIndex(index);
        return _getByte(index);
    }

    @Override
    public short getUnsignedByte(int index) {
        // 将获取到的 Byte 转换为 UnsignedByte
        return (short) (getByte(index) & 0xFF);
    }   

    protected abstract byte _getByte(int index);
}
```

其底层依赖的是一个抽象方法 `_getByte`，由 AbstractByteBuf 具体的子类负责实现。比如，在 UnpooledDirectByteBuf 类的实现中，直接将 \_getByte 操作代理给其底层依赖的 JDK DirectByteBuffer。

```
public class UnpooledDirectByteBuf  {
    // 底层依赖 JDK 的 DirectByteBuffer
    ByteBuffer buffer;

    @Override
    protected byte _getByte(int index) {
        return buffer.get(index);
    }
}
```

而在 UnpooledUnsafeDirectByteBuf 类的实现中，则是通过 `sun.misc.Unsafe` 直接从对应的内存地址中读取。

```
public class UnpooledUnsafeDirectByteBuf {
    // 直接操作 OS 的内存地址
    long memoryAddress;
    @Override
    protected byte _getByte(int index) {
        // 底层依赖 PlatformDependent0，直接通过内存地址读取 byte
        return UnsafeByteBufUtil.getByte(addr(index));
    }

    final long addr(int index) {
        // 获取偏移 index 对应的内存地址
        return memoryAddress + index;
    }
}

final class PlatformDependent0 {
  // sun.misc.Unsafe
  static final Unsafe UNSAFE;
  static byte getByte(long address) {
        return UNSAFE.getByte(address);
    }
}
```

Netty 另外还提供了批量读取 Bytes 的操作，比如我们可以通过 `getBytes` 方法将 ByteBuf 中的数据读取到一个字节数组 `byte[]` 中，也可以读取到另一个 ByteBuf 中。

```
    @Override
    public ByteBuf getBytes(int index, byte[] dst) {
        getBytes(index, dst, 0, dst.length);
        return this;
    }

    public abstract ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length);

    @Override
    public ByteBuf getBytes(int index, ByteBuf dst, int length) {
        getBytes(index, dst, dst.writerIndex(), length);
        // 调整 dst 的  writerIndex
        dst.writerIndex(dst.writerIndex() + length);
        return this;
    }
    
    // 注意这里的 getBytes 方法既不会改变原来 ByteBuf 的 readerIndex 和 writerIndex
    // 也不会改变目的 ByteBuf 的 readerIndex 和 writerIndex
    public abstract ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length);
```

通过 getBytes 方法将原来 ByteBuf 的数据读取到目的 ByteBuf 之后，原来 ByteBuf 的 readerIndex 不会发生变化，但是目的 ByteBuf 的 writerIndex 会重新调整。

对于 UnpooledDirectByteBuf 类的具体实现来说自然是将 getBytes 的操作直接代理给其底层依赖的 JDK DirectByteBuffer。对于 UnpooledUnsafeDirectByteBuf 类的具体实现来说，则是通过 `UNSAFE.copyMemory` 直接根据内存地址进行拷贝。

而 ByteBuf 中的 read 方法则不仅会从 ByteBuf 中读取数据，而且会改变其 readerIndex 的位置。比如，`readByte` 方法首先会通过前面介绍的 `_getByte` 从 ByteBuf 中读取一个字节，然后将 readerIndex 向后移动一位。

```
   @Override
    public byte readByte() {
        checkReadableBytes0(1);
        int i = readerIndex;
        byte b = _getByte(i);
        readerIndex = i + 1;
        return b;
    }
```

同样 Netty 也提供了从 ByteBuf 中批量读取数据的方法 readBytes，我们可以将一个 ByteBuf 中的数据通过 readBytes 方法读取到另一个 ByteBuf 中。但是这里，Netty 将会改变原来 ByteBuf 的 readerIndex 以及目的 ByteBuf 的 writerIndex。

```
   @Override
    public ByteBuf readBytes(ByteBuf dst, int length) {
        readBytes(dst, dst.writerIndex(), length);
        // 改变 dst 的 writerIndex
        dst.writerIndex(dst.writerIndex() + length);
        return this;
    }
```

另外我们还可以明确指定 dstIndex，使得我们可以从目的 ByteBuf 中的某一个位置处开始拷贝原来 ByteBuf 中的数据，但这里只会改变原来 ByteBuf 的 readerIndex，并不会改变目的 ByteBuf 的 writerIndex。这也很好理解，因为我们在写入目的 ByteBuf  的时候已经明确指定了 writerIndex（dstIndex），自然在写入完成之后，writerIndex 的位置并不需要改变。

```
    @Override
    public ByteBuf readBytes(ByteBuf dst, int dstIndex, int length) {
        checkReadableBytes(length);
        getBytes(readerIndex, dst, dstIndex, length);
        // 改变原来 ByteBuf 的 readerIndex
        readerIndex += length;
        return this;
    }
```

除此之外，Netty 还支持将 ByteBuf 中的数据读取到不同的目的地，比如，读取到 JDK ByteBuffer 中，读取到 FileChannel 中，读取到 OutputStream 中，以及读取到 GatheringByteChannel 中。

```
public abstract ByteBuf readBytes(ByteBuffer dst);
public abstract ByteBuf readBytes(OutputStream out, int length) throws IOException;
public abstract int readBytes(GatheringByteChannel out, int length) throws IOException;
public abstract int readBytes(FileChannel out, long position, int length) throws IOException;
```

Netty 除了支持以  Byte 为粒度对 ByteBuf 进行读写之外，还同时支持以多种基本类型对 ByteBuf 进行读写，这里笔者以 Int 类型为例进行说明。

我们可以通过 `readInt()` 从 ByteBuf 中读取一个 Int 类型的数据出来，随后 ByteBuf 的 readerIndex 向后移动 4 个位置。

```
   @Override
    public int readInt() {
        checkReadableBytes0(4);
        int v = _getInt(readerIndex);
        readerIndex += 4;
        return v;
    }

    protected abstract int _getInt(int index);
```

同理，真正负责读取数据的方法 \_getInt 方法需要由 AbstractByteBuf 具体的子类实现，但这里和 \_getByte 不同的是，\_getInt 需要考虑字节序的问题，**由于网络协议采用的是大端字节序传输，所以 Netty 的 ByteBuf 默认也是大端字节序**。

在 UnpooledDirectByteBuf 的实现中，同样也是将 getInt 的操作直接代理给其底层依赖的 JDK DirectByteBuffer。

```
public class UnpooledDirectByteBuf  {
    @Override
    protected int _getInt(int index) {
        // 代理给其底层依赖的 JDK DirectByteBuffer
        return buffer.getInt(index);
    }
}
```

在 UnpooledUnsafeDirectByteBuf 的实现中，由于是通过 `sun.misc.Unsafe` 直接对内存地址进行操作，所以需要考虑字节序转换的细节。Netty 的 ByteBuf 默认是大端字节序，所以这里直接依次将低地址的字节放到 Int 数据的高位就可以了。

```
public class UnpooledUnsafeDirectByteBuf {
    @Override
    protected int _getInt(int index) {
        return UnsafeByteBufUtil.getInt(addr(index));
    }
}

final class UnsafeByteBufUtil {
    static int getInt(long address) {    
        return PlatformDependent.getByte(address) << 24 |
               (PlatformDependent.getByte(address + 1) & 0xff) << 16 |
               (PlatformDependent.getByte(address + 2) & 0xff) <<  8 |
               PlatformDependent.getByte(address + 3)  & 0xff;
    }
}
```

同时 Netty 也支持以小端字节序来从 ByteBuf 中读取 Int 数据，这里就涉及到字节序的转换了。

```
    @Override
    public int readIntLE() {
        checkReadableBytes0(4);
        int v = _getIntLE(readerIndex);
        readerIndex += 4;
        return v;
    }

 protected abstract int _getIntLE(int index);
```

在 UnpooledDirectByteBuf 的实现中，首先通过其依赖的 JDK DirectByteBuffer    以大端序读取一个 Int 数据，然后通过 `ByteBufUtil.swapInt` 切换成小端序返回。

```
public class UnpooledDirectByteBuf  {
    @Override
    protected int _getIntLE(int index) {
        // 切换字节序，从大端变小端
        return ByteBufUtil.swapInt(buffer.getInt(index));
    }
}
```

在 UnpooledUnsafeDirectByteBuf 的实现中，则是直接将低地址上的字节依次放到 Int 数据的低位上就可以了。

```
public class UnpooledUnsafeDirectByteBuf {
    @Override
    protected int _getIntLE(int index) {
        return UnsafeByteBufUtil.getIntLE(addr(index));
    }
}

final class UnsafeByteBufUtil {
    static int getIntLE(long address) {
        return PlatformDependent.getByte(address) & 0xff |
               (PlatformDependent.getByte(address + 1) & 0xff) <<  8 |
               (PlatformDependent.getByte(address + 2) & 0xff) << 16 |
               PlatformDependent.getByte(address + 3) << 24;
    }
}
```

另外 Netty 也支持从 ByteBuf 中读取基本类型的 `Unsigned 类型`。

```
    @Override
    public long readUnsignedInt() {
        return readInt() & 0xFFFFFFFFL;
    }

    @Override
    public long readUnsignedIntLE() {
        return readIntLE() & 0xFFFFFFFFL;
    }
```

其他基本类型的相关读取操作实现的逻辑都是大同小异，笔者就不一一列举了。

### 2.3 discardReadBytes

随着 readBytes 方法的不断调用， ByteBuf 中的 readerIndex 也会不断的向后移动，Netty 对 readerIndex 的设计有两层语义：

1.  第一层的语义比较明显，就是用来表示当前 ByteBuf 的读取位置，当我们调用 readBytes 方法的时候就是从 readerIndex 开始读取数据，当 readerIndex 等于 writerIndex 的时候，ByteBuf 就不可读取了。
    
2.  第二层语义比较含蓄，它是用来表示当前 ByteBuf 可以被丢弃的字节数，因为 readerIndex 用来指示当前的读取位置，那么位于 readerIndex 之前的字节肯定是已经被读取完毕了，已经被读取的字节继续驻留在 ByteBuf 中就没有必要了，还不如把空间腾出来，还能在多写入些数据。
    

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFQYUNduNW4KgkPYnObJbxmZ33ldMCzeLT4FSzVa0XSo44bVAcaMVWCw/640?wx_fmt=other&from=appmsg#imgIndex=7)

image.png

所以一个 ByteBuf 真正的剩余可写容量的计算方式除了上小节中介绍的 `writableBytes()` 方法返回的字节数之外还需要在加上 readerIndex。

```
    @Override
    public int writableBytes() {
        return capacity() - writerIndex;
    }
```

举个具体点的例子就是，当我们准备向一个 ByteBuf 写入 n 个字节时，如果 `writableBytes()` 小于 n，那么就表示当前 ByteBuf 的剩余容量不能满足本次写入的字节数。

但是 `readerIndex + writableBytes()` 大于等于 n ， 则表示如果我们将 ByteBuf 中已经读取的字节数丢弃的话，那么就可以满足本次写入的请求。

在这种情况下，我们就可以使用 `discardReadBytes()` 方法将 readerIndex 之前的字节丢弃掉，这样一来，可写的字节容就可以满足本次写入要求了，那么如果丢弃呢 ？

我们先来看 `readerIndex < writerIndex` 的情况，这种情况下表示 ByteBuf 中还有未读取的字节。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFSMibsyMiadEnq9veNBwnb6DE7NzZ0pwN4qw2hmaRiaPwrqwVNQ0zH6JcA/640?wx_fmt=other&from=appmsg#imgIndex=8)

image.png

ByteBuf 目前可读取的字节范围为： `[readerIndex, writerIndex)`，位于 readerIndex 之前的字节均可以被丢弃，接下来我们就需要将 `[readerIndex, writerIndex)` 这段范围的字节全部拷贝到 ByteBuf 最前面，直接覆盖 readerIndex 之前的字节。

然后调整 readerIndex 和 writerIndex 的位置，因为 readerIndex 之前的字节现在已经全部被可读字节覆盖了，所以 readerIndex 重新调整为 0 ，writerIndex 向前移动 readerIndex 大小。这样一来，当前 ByteBuf 的可写容量就多出了 readerIndex 大小。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFeWmiaZIuBtfRXFDJarj755AKTibZfYDSmnxhibH7yiaY4quHM5xYsBq3Cw/640?wx_fmt=other&from=appmsg#imgIndex=9)

image.png

另外一种情况是 `readerIndex = writerIndex` 的情况，这种情况下表示 ByteBuf 中已经没有可读字节了。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFb7BbadTzOkQzzGISkLoV4mvOSxX3vPaQv5RZCXEll3ejWxKB2ENxTw/640?wx_fmt=other&from=appmsg#imgIndex=10)

image.png

既然 ByteBuf 中已经没有任何可读字节了，自然也就不需要将可读字节拷贝到 ByteBuf 的开头了，直接将 readerIndex 和 writerIndex 重新调整为 0 即可。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFryrPMUx9JBwjWLupR3eWzdgzUy3ncqnBU1wAicJFg1kfZXIAOiadzBcg/640?wx_fmt=other&from=appmsg#imgIndex=11)

image.png

```
public abstract class AbstractByteBuf extends ByteBuf {
    @Override
    public ByteBuf discardReadBytes() {
        // readerIndex 为 0 表示没有可以丢弃的字节
        if (readerIndex == 0) {
            return this;
        }

        if (readerIndex != writerIndex) {
            // 将 [readerIndex, writerIndex) 这段字节范围移动到 ByteBuf 的开头
            // 也就是丢弃 readerIndex 之前的字节
            setBytes(0, this, readerIndex, writerIndex - readerIndex);
            // writerIndex 和 readerIndex 都向前移动 readerIndex 大小
            writerIndex -= readerIndex;
            // 重新调整 markedReaderIndex 和 markedWriterIndex 的位置
            // 都对应向前移动 readerIndex 大小。
            adjustMarkers(readerIndex);
            readerIndex = 0;
        } else {
            // readerIndex = writerIndex 表示当前 ByteBuf 已经不可读了
            // 将 readerIndex 之前的字节全部丢弃，ByteBuf 恢复到最初的状态
            // 整个 ByteBuf 的容量都可以被写入
            ensureAccessible();
            adjustMarkers(readerIndex);
            writerIndex = readerIndex = 0;
        }
        return this;
    }
}
```

如果 ByteBuf 存在可以被丢弃的字节的时候（readerIndex > 0），只要我们调用 `discardReadBytes()` 就会无条件丢弃 readerIndex 之前的字节。

Netty 还另外提供了 `discardSomeReadBytes()` 方法进行有条件丢弃字节，丢弃条件有如下两种：

1.  当 ByteBuf 已经不可读的时候，则无条件丢弃已读字节。
    
2.  当已读的字节数超过整个 ByteBuf 一半容量时才会丢弃已读字节。否则无条件丢弃的话，收益就不高了。
    

```
    @Override
    public ByteBuf discardSomeReadBytes() {
        if (readerIndex > 0) {
            // 当 ByteBuf 已经不可读了，则无条件丢弃已读字节
            if (readerIndex == writerIndex) {
                adjustMarkers(readerIndex);
                writerIndex = readerIndex = 0;
                return this;
            }
            // 当已读的字节数超过整个 ByteBuf 的一半容量时才会丢弃已读字节
            if (readerIndex >= capacity() >>> 1) {
                setBytes(0, this, readerIndex, writerIndex - readerIndex);
                writerIndex -= readerIndex;
                adjustMarkers(readerIndex);
                readerIndex = 0;
                return this;
            }
        }
        return this;
    }
```

Netty 设计的这个丢弃字节的方法在解码的场景非常有用，由于 TCP 是一个面向流的网络协议，它只会根据滑动窗口的大小进行字节流的发送，所以我们在应用层接收到的数据可能是一个半包也可能是一个粘包，反正不会是一个完整的数据包。

这就要求我们在解码的时候，首先要判断 ByteBuf 中的数据是否构成一个完成的数据包，如果构成一个数据包，才会去读取 ByteBuf 中的字节，然后解码，随后 readerIndex 向后移动。

如果不够一个数据包，那就需要将 ByteBuf 累积缓存起来，一直等到一个完整的数据包到来。一种极端的情况是，即使我们已经解码很多次了，但是缓存的 ByteBuf 中仍然还有半包，由于不断的会有粘包过来，这就导致 ByteBuf 会越来越大。由于已经解码了很多次，所以 ByteBuf 中可以被丢弃的字节占据了很大的内存空间，如果半包情况持续存在，将会导致 OutOfMemory。

所以 Netty 规定，如果已经解码了 16 次之后，ByteBuf 中仍然有半包的情况，那么就会调用这里的 `discardSomeReadBytes()` 将已经解码过的字节全部丢弃，节省不必要的内存开销。

### 2.4 ByteBuf 的写入操作

ByteBuf 的写入操作与读取操作互为相反的操作，每一个读取方法 getBytes , readBytes , readInt 等都有一个对应的 setBytes , writeBytes , writeInt 等基础类型的写入操作。

和 get 方法一样，set 相关的方法也只是单纯的向 ByteBuf 中写入数据，并不会改变其 writerIndex 的位置，我们可以通过 `setByte` 向 ByteBuf 中的某一个指定位置 index 写入数据 value。

```
    @Override
    public ByteBuf setByte(int index, int value) {
        checkIndex(index);
        _setByte(index, value);
        return this;
    }

    protected abstract void _setByte(int index, int value);
```

执行具体的写入操作同样也是一个抽象方法，其具体的实现由 AbstractByteBuf 具体的子类负责。对于 UnpooledDirectByteBuf 的实现来说，\_setByte 操作直接会代理给其底层依赖的 JDK  DirectByteBuffer。

```
public class UnpooledDirectByteBuf  {
    // 底层依赖 JDK 的 DirectByteBuffer
    ByteBuffer buffer;

    @Override
    protected void _setByte(int index, int value) {
        buffer.put(index, (byte) value);
    }
}
```

对于 UnpooledUnsafeDirectByteBuf 的实现来说，则是直接通过 `sun.misc.Unsafe` 向对应的内存地址（memoryAddress + index）写入 Byte。

```
public class UnpooledUnsafeDirectByteBuf {
    // 直接操作 OS 的内存地址，不依赖 JDK 的 buffer
    long memoryAddress;

   @Override
    protected void _setByte(int index, int value) {
        // 底层依赖 PlatformDependent0，直接向内存地址写入 byte
        UnsafeByteBufUtil.setByte(addr(index), value);
    }

    final long addr(int index) {
        // 获取偏移 index 对应的内存地址
        return memoryAddress + index;
    }
}

final class PlatformDependent0 {
  // sun.misc.Unsafe
  static final Unsafe UNSAFE;
  static void putByte(long address, byte value) {
        UNSAFE.putByte(address, value);
  }
}
```

Netty 另外也提供了向 ByteBuf 批量写入 Bytes 的操作，setBytes 方法用于向 ByteBuf 的指定位置 index 批量写入一个字节数组 byte\[\] 中的数据。

```
   @Override
    public ByteBuf setBytes(int index, byte[] src) {
        setBytes(index, src, 0, src.length);
        return this;
    }

    public abstract ByteBuf setBytes(int index, byte[] src, int srcIndex, int length);
```

对于 UnpooledDirectByteBuf 的实现来说，同样也是将 setBytes 的操作直接代理给 JDK DirectByteBuffer，将字节数组 byte\[\] 中的字节直接写入 DirectByteBuffer 中。

对于 UnpooledUnsafeDirectByteBuf  的实现来说，则是直接操作字节数组和 ByteBuf 的内存地址，通过 `UNSAFE.copyMemory` 将字节数组对应内存地址中的数据拷贝到 ByteBuf 相应的内存地址上。

我们还可以通过 setBytes 方法将其他 ByteBuf 中的字节数据写入到 ByteBuf 中。

```
    @Override
    public ByteBuf setBytes(int index, ByteBuf src, int length) {
        setBytes(index, src, src.readerIndex(), length);
        // 调整 src 的  readerIndex
        src.readerIndex(src.readerIndex() + length);
        return this;
    }

    // 注意这里的 setBytes 方法既不会改变原来 ByteBuf 的 readerIndex 和 writerIndex
    // 也不会改变目的 ByteBuf 的 readerIndex 和 writerIndex
    public abstract ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length);
```

**这里需要注意的是被写入 ByteBuf 的 writerIndex 并不会改变，但是原来 ByteBuf 的 readerIndex 会重新调整**。

ByteBuf 中的 write 方法底层依赖的是相关的 set 方法，不同的是 write 方法会改变 ByteBuf 中 writerIndex 的位置。比如，我们通过 `writeByte` 方法向 ByteBuf 中写入一个字节之后，writerIndex 就会向后移动一位。

```
    @Override
    public ByteBuf writeByte(int value) {
        ensureWritable0(1);
        _setByte(writerIndex++, value);
        return this;
    }
```

我们也可以通过 writeBytes 向 ByteBuf 中批量写入数据，将一个字节数组中的数据或者另一个 ByteBuf 中的数据写入到 ByteBuf 中，但是这里，Netty 将会改变被写入 ByteBuf 的 writerIndex 以及数据来源 ByteBuf  的 readerIndex。

```
    @Override
    public ByteBuf writeBytes(ByteBuf src, int length) {
        writeBytes(src, src.readerIndex(), length);
        // 调整数据来源 ByteBuf 的 readerIndex
        src.readerIndex(src.readerIndex() + length);
        return this;
    }
```

如果我们明确指定了从数据来源 ByteBuf 中的哪一个位置（srcIndex）开始读取数据，那么数据来源 ByteBuf 中的 readerIndex 将不会被改变，只会改变被写入 ByteBuf 的 writerIndex。

```
    @Override
    public ByteBuf writeBytes(ByteBuf src, int srcIndex, int length) {
        ensureWritable(length);
        setBytes(writerIndex, src, srcIndex, length);
        // 调整被写入 ByteBuf 的 writerIndex
        writerIndex += length;
        return this;
    }
```

除此之外，Netty 还支持从不同的数据来源向 ByteBuf 批量写入数据，比如，从 JDK ByteBuffer ，从 FileChannel ，从 InputStream ，以及从 ScatteringByteChannel 中。

```
public ByteBuf writeBytes(ByteBuffer src) 
public int writeBytes(InputStream in, int length)
public int writeBytes(ScatteringByteChannel in, int length) throws IOException
public int writeBytes(FileChannel in, long position, int length) throws IOException
```

Netty 除了支持以 Byte 为粒度向 ByteBuf 中写入数据之外，还同时支持以多种基本类型为粒度向写入 ByteBuf ，这里笔者以 Int 类型为例进行说明。

我们可以通过 writeInt() 向 ByteBuf 写入一个 Int 类型的数据，随后 ByteBuf 的 writerIndex 向后移动 4 个位置。

```
    @Override
    public ByteBuf writeInt(int value) {
        ensureWritable0(4);
        _setInt(writerIndex, value);
        writerIndex += 4;
        return this;
    }

   protected abstract void _setInt(int index, int value);
```

和写入 Byte 数据不同的是，这里需要考虑字节序，Netty  ByteBuf 默认是大端字节序，和网络协议传输使用的字节序保持一致。这里我们需要将待写入数据 value 的高位依次放入到 ByteBuf 的低地址上。

```
public class UnpooledUnsafeDirectByteBuf {
   @Override
    protected void _setInt(int index, int value) {
        // 以大端字节序写入 ByteBuf 
        UnsafeByteBufUtil.setInt(addr(index), value);
    }
}

final class UnsafeByteBufUtil {
   static void setInt(long address, int value) {
            PlatformDependent.putByte(address, (byte) (value >>> 24));
            PlatformDependent.putByte(address + 1, (byte) (value >>> 16));
            PlatformDependent.putByte(address + 2, (byte) (value >>> 8));
            PlatformDependent.putByte(address + 3, (byte) value);   
    }
}
```

同时 Netty 也支持以小端字节序向 ByteBuf 写入数据。

```
    @Override
    public ByteBuf writeIntLE(int value) {
        ensureWritable0(4);
        _setIntLE(writerIndex, value);
        writerIndex += 4;
        return this;
    }

   protected abstract void _setIntLE(int index, int value);
```

这里需要将待写入数据 value 的低位依次放到 ByteBuf 的低地址上。

```
public class UnpooledUnsafeDirectByteBuf {
    @Override
    protected void _setIntLE(int index, int value) {
        // // 以小端字节序写入 ByteBuf 
        UnsafeByteBufUtil.setIntLE(addr(index), value);
    }
}

final class UnsafeByteBufUtil {
   static void setIntLE(long address, int value) {
            PlatformDependent.putByte(address, (byte) value);
            PlatformDependent.putByte(address + 1, (byte) (value >>> 8));
            PlatformDependent.putByte(address + 2, (byte) (value >>> 16));
            PlatformDependent.putByte(address + 3, (byte) (value >>> 24));
    }
}
```

### 2.5 ByteBuf 的扩容机制

在每次向 ByteBuf 写入数据的时候，Netty 都会调用 `ensureWritable0` 方法来判断当前 ByteBuf 剩余可写容量（capacity - writerIndex）是否能够满足本次需要写入的数据大小 minWritableBytes。如果剩余容量不足，那么就需要对 ByteBuf 进行扩容，但扩容后的容量不能超过 maxCapacity 的大小。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFUgic97dhJn0dXEiaEJokwJ6soDfIbXk1TsPtbYDHs964EkxoZPeePTwQ/640?wx_fmt=other&from=appmsg#imgIndex=12)

image.png

```
    final void ensureWritable0(int minWritableBytes) {
        final int writerIndex = writerIndex();
        // 为满足本次的写入操作，预期的 ByteBuf 容量大小
        final int targetCapacity = writerIndex + minWritableBytes;
        // 剩余容量可以满足本次写入要求，直接返回，不需要扩容
        if (targetCapacity >= 0 & targetCapacity <= capacity()) {
            return;
        }
        // 扩容后的容量不能超过 maxCapacity
        if (checkBounds && (targetCapacity < 0 || targetCapacity > maxCapacity)) {
            ensureAccessible();
            throw new IndexOutOfBoundsException(String.format(
                    "writerIndex(%d) + minWritableBytes(%d) exceeds maxCapacity(%d): %s",
                    writerIndex, minWritableBytes, maxCapacity, this));
        }

        // 如果 targetCapacity 在（capacity , maxCapacity] 之间，则进行扩容
        // fastWritable 表示在不涉及到 memory reallocation or data-copy 的情况下，当前 ByteBuf 可以直接写入的容量
        // 对于 UnpooledDirectBuffer 这里的 fastWritable = capacity - writerIndex
        // PooledDirectBuffer 有另外的实现，这里先暂时不需要关注
        final int fastWritable = maxFastWritableBytes();
        // 计算扩容后的容量 newCapacity
        // 对于 UnpooledDirectBuffer 来说这里直接通过 calculateNewCapacity 计算扩容后的容量。
        int newCapacity = fastWritable >= minWritableBytes ? writerIndex + fastWritable
                : alloc().calculateNewCapacity(targetCapacity, maxCapacity);

        // 根据 new capacity 对 ByteBuf 进行扩容
        capacity(newCapacity);
    }
```

#### 2.5.1 newCapacity 的计算逻辑

ByteBuf 的初始默认 capacity 为 256 个字节，初始默认 maxCapacity 为 `Integer.MAX_VALUE` 也就是 2G 大小。

```
public abstract class AbstractByteBufAllocator implements ByteBufAllocator {
    // ByteBuf 的初始默认 CAPACITY
    static final int DEFAULT_INITIAL_CAPACITY = 256;
    // ByteBuf 的初始默认 MAX_CAPACITY 
    static final int DEFAULT_MAX_CAPACITY = Integer.MAX_VALUE;

    @Override
    public ByteBuf directBuffer() {
        return directBuffer(DEFAULT_INITIAL_CAPACITY, DEFAULT_MAX_CAPACITY);
    }
}
```

为满足本次写入操作，对 ByteBuf 的最小容量要求为 minNewCapacity，它的值就是在 `ensureWritable0` 方法中计算出来的 `targetCapacity` , 计算方式为： `minNewCapacity = writerIndex + minWritableBytes（本次将要写入的字节数）`。

在 ByteBuf 的扩容逻辑中，Netty 设置了一个重要的阈值 `CALCULATE_THRESHOLD`, 大小为 4M，它决定了 ByteBuf 扩容的尺度。

```
    // 扩容的尺度
    static final int CALCULATE_THRESHOLD = 1048576 * 4; // 4 MiB page
```

如果 minNewCapacity 恰好等于 CALCULATE\_THRESHOLD，那么扩容后的容量 newCapacity 就是 4M。

如果 minNewCapacity 大于 CALCULATE\_THRESHOLD，那么 newCapacity 就会按照 4M 的尺度进行扩容，具体的扩容逻辑如下：

首先通过 `minNewCapacity / threshold * threshold` 计算出一个准备扩容之前的基准线，后面就会以此基准线为基础，按照 CALCULATE\_THRESHOLD 的粒度进行扩容。

该基准线的要求必须是 CALCULATE\_THRESHOLD 的最小倍数，而且必须要小于等于 minNewCapacity。

什么意思呢 ？ 假设 minNewCapacity 为 5M，那么它的扩容基准线就是 4M ， 这种情况下扩容之后的容量 `newCapacity = 4M + CALCULATE_THRESHOLD = 8M` 。

如果计算出来的基准线超过了 `maxCapacity - 4M` , 那么 newCapacity 直接就扩容到 maxCapacity 。

如果 minNewCapacity 小于 CALCULATE\_THRESHOLD，那么 newCapacity 就会从 64 开始，一直循环 double , 也就是按照 64 的倍数进行扩容。直到 newCapacity 大于等于 minNewCapacity。

```
        int newCapacity = 64;
        while (newCapacity < minNewCapacity) {
            newCapacity <<= 1;
        }
```

*   如果 minNewCapacity 在 `[0 , 64]` 这段范围内 ， 那么扩容后的 newCapacity 就是 64
    
*   如果 minNewCapacity 在 `[65 , 128]` 这段范围内 ， 那么扩容后的 newCapacity 就是 128 。
    
*   如果 minNewCapacity 在 `[129 , 256]` 这段范围内 ， 那么扩容后的 newCapacity 就是 256 。
    

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFkiaQ5pojEYzJsIe2e4wyyibY3Tia1rpEgOj8SjM0kPBN3OAUr0HbabwUw/640?wx_fmt=other&from=appmsg#imgIndex=13)

image.png

```
public abstract class AbstractByteBufAllocator implements ByteBufAllocator {

    @Override
    public int calculateNewCapacity(int minNewCapacity, int maxCapacity) {
        // 满足本次写入操作的最小容量 minNewCapacity 不能超过 maxCapacity
        if (minNewCapacity > maxCapacity) {
            throw new IllegalArgumentException(String.format(
                    "minNewCapacity: %d (expected: not greater than maxCapacity(%d)",
                    minNewCapacity, maxCapacity));
        }
        // 用于决定扩容的尺度
        final int threshold = CALCULATE_THRESHOLD; // 4 MiB page

        if (minNewCapacity == threshold) {
            return threshold;
        }

        // If over threshold, do not double but just increase by threshold.
        if (minNewCapacity > threshold) {
            // 计算扩容基准线。
            // 要求必须是 CALCULATE_THRESHOLD 的最小倍数，而且必须要小于等于 minNewCapacity
            int newCapacity = minNewCapacity / threshold * threshold;
            if (newCapacity > maxCapacity - threshold) {
                newCapacity = maxCapacity;
            } else {
                // 按照 threshold (4M)扩容
                newCapacity += threshold;
            }
            return newCapacity;
        }

        // Not over threshold. Double up to 4 MiB, starting from 64.
        // 按照 64 的倍数进行扩容。但 newCapacity 需要大于等于 minNewCapacity。
        int newCapacity = 64;
        while (newCapacity < minNewCapacity) {
            newCapacity <<= 1;
        }

        return Math.min(newCapacity, maxCapacity);
    }
}
```

#### 2.5.2 ByteBuf 的扩容逻辑

```
public class UnpooledDirectByteBuf  {
    // 底层依赖 JDK 的 DirectByteBuffer
    ByteBuffer buffer;
}
```

对于 UnpooledDirectByteBuf 来说，其底层真正存储数据的地方其实是依赖 JDK  中的 DirectByteBuffer，扩容的逻辑很简单，就是首先根据上一小节计算出的 newCapacity 重新分配一个新的 JDK  DirectByteBuffer ， 然后将原来 DirectByteBuffer 中的数据拷贝到新的 DirectByteBuffer 中，最后释放原来的 DirectByteBuffer，将新的 DirectByteBuffer 设置到 UnpooledDirectByteBuf 中。

```
public class UnpooledDirectByteBuf  {
    void setByteBuffer(ByteBuffer buffer, boolean tryFree) {
        if (tryFree) {
            ByteBuffer oldBuffer = this.buffer;
            // 释放原来的 buffer
            freeDirect(oldBuffer);
        }
        // 重新设置新的 buffer
        this.buffer = buffer;
        capacity = buffer.remaining();
    }
}
```

对于 UnpooledUnsafeDirectByteBuf 来说，由于它直接依赖的是 OS 内存地址，对 ByteBuf 的相关操作都是直接操作内存地址进行，所以 UnpooledUnsafeDirectByteBuf 的扩容逻辑除了要执行上面的内容之外，还需要将新 DirectByteBuffer 的内存地址设置到 memoryAddress 中。

```
public class UnpooledUnsafeDirectByteBuf extends UnpooledDirectByteBuf {
    // ByteBuf 的内存地址
    long memoryAddress;

    @Override
    final void setByteBuffer(ByteBuffer buffer, boolean tryFree) {
        super.setByteBuffer(buffer, tryFree);
        // 设置成新 buffer 的内存地址
        memoryAddress = PlatformDependent.directBufferAddress(buffer);
    }
}
```

下面是完整的扩容操作逻辑：

```
public class UnpooledDirectByteBuf  {
    // 底层依赖 JDK 的 DirectByteBuffer
    ByteBuffer buffer;

    @Override
    public ByteBuf capacity(int newCapacity) {
        // newCapacity 不能超过 maxCapacity
        checkNewCapacity(newCapacity);
        int oldCapacity = capacity;
        if (newCapacity == oldCapacity) {
            return this;
        }
        // 计算扩容之后需要拷贝的字节数
        int bytesToCopy;
        if (newCapacity > oldCapacity) {
            bytesToCopy = oldCapacity;
        } else {
            ........ 缩容 .......
        }
        ByteBuffer oldBuffer = buffer;
        // 根据 newCapacity 分配一个新的 ByteBuffer（JDK）
        ByteBuffer newBuffer = allocateDirect(newCapacity);
        oldBuffer.position(0).limit(bytesToCopy);
        newBuffer.position(0).limit(bytesToCopy);
        // 将原来 oldBuffer 中的数据拷贝到 newBuffer 中
        newBuffer.put(oldBuffer).clear();
        // 释放 oldBuffer，设置 newBuffer
        // 对于 UnpooledUnsafeDirectByteBuf 来说就是将 newBuffer 的地址设置到 memoryAddress 中
        setByteBuffer(newBuffer, true);
        return this;
    }
}
```

#### 2.5.3 强制扩容

前面介绍的 ensureWritable 方法会检查本次写入的数据大小 minWritableBytes 是否超过 ByteBuf 的最大可写容量：`maxCapacity - writerIndex`。

```
public ByteBuf ensureWritable(int minWritableBytes) 
```

如果超过，则会抛出 `IndexOutOfBoundsException` 异常停止扩容，Netty 提供了另外一个带有 force 参数的扩容方法，用来决定在这种情况下是否强制进行扩容。

```
 public int ensureWritable(int minWritableBytes, boolean force) 
```

当 minWritableBytes 已经超过 ByteBuf 的最大可写容量得时候：

*   `force = false` ， 那么停止扩容，直接返回，不抛异常。
    
*   `force = true` , 则进行强制扩容，将 ByteBuf 扩容至 maxCapacity，但是如果当前容量已经达到了 maxCapacity，则停止扩容 。
    

带 force 参数的 ensureWritable 并不会抛出异常，而是通过返回状态码来通知调用者 ByteBuf 的容量情况。

1.  返回 0 表示，ByteBuf 当前可写容量可以满足本次写入操作的需求，不需要扩容
    
2.  返回 1 表示，本次写入的数据大小已经超过了 ByteBuf 的最大可写容量，但 ByteBuf 的容量已经达到了 maxCapacity，无法进行扩容。
    
3.  返回 3 表示，本次写入的数据大小已经超过了 ByteBuf 的最大可写容量，这种情况下，强制将容量扩容至 maxCapacity。
    
4.  返回 2 表示，执行正常的扩容逻辑。
    

返回值 0 和 2 均表示 ByteBuf 容量（扩容前或者扩容后）可以满足本次写入的数据大小，而返回值 1 和 3 表示 ByteBuf 容量（扩容前或者扩容后）都无法满足本次写入的数据大小。

```
    @Override
    public int ensureWritable(int minWritableBytes, boolean force) {
        // 如果剩余容量可以满足本次写入操作，则不会扩容，直接返回
        if (minWritableBytes <= writableBytes()) {
            return 0;
        }

        final int maxCapacity = maxCapacity();
        final int writerIndex = writerIndex();
        // 如果本次写入的数据大小已经超过了 ByteBuf 的最大可写容量 maxCapacity - writerIndex
        if (minWritableBytes > maxCapacity - writerIndex) {
            // force = false ， 那么停止扩容，直接返回
            // force = true, 直接扩容到 maxCapacity，如果当前 capacity 已经等于 maxCapacity 了则停止扩容
            if (!force || capacity() == maxCapacity) {
                return 1;
            }
            // 虽然扩容之后还是无法满足写入需求，但还是强制扩容至 maxCapacity
            capacity(maxCapacity);
            return 3;
        }
        // 下面就是普通的扩容逻辑
        int fastWritable = maxFastWritableBytes();
        int newCapacity = fastWritable >= minWritableBytes ? writerIndex + fastWritable
                : alloc().calculateNewCapacity(writerIndex + minWritableBytes, maxCapacity);

        // Adjust to the new capacity.
        capacity(newCapacity);
        return 2;
    }
```

#### 2.5.4 自适应动态扩容

Netty 在接收网络数据的过程中，其实一开始是很难确定出该用多大容量的 ByteBuf 去接收的，所以 Netty 在一开始会首先预估一个初始容量 `DEFAULT_INITIAL (2048)`。

```
public class AdaptiveRecvByteBufAllocator {
    static final int DEFAULT_INITIAL = 2048;
}
```

用初始容量为 2048 大小的 ByteBuf 去读取 socket 中的数据，在每一次读取完 socket 之后，Netty 都会评估 ByteBuf 的容量大小是否合适。如果每一次都能把 ByteBuf 装满，那说明我们预估的容量太小了，socket 中还有更多的数据，那么就需要对 ByteBuf 进行扩容，下一次读取 socket 的时候就换一个容量更大的 ByteBuf。

```
 private final class HandleImpl extends MaxMessageHandle {
        @Override
        public void lastBytesRead(int bytes) {
            // bytes 为本次从 socket 中真实读取的数据大小
            // attemptedBytesRead 为 ByteBuf 可写的容量大小，初始为 2048
            if (bytes == attemptedBytesRead()) {
                // 如果本次读取 socket 中的数据将 ByteBuf 装满了
                // 那么就对 ByteBuf 进行扩容，在下一次读取的时候用更大的 ByteBuf 去读
                record(bytes);
            }
            // 记录本次从 socket 中读取的数据大小
            super.lastBytesRead(bytes);
        }
 }
```

Netty 会在一个 read loop 中不停的读取 socket 中的数据直到数据被读取完毕或者读满 16 次，结束 read loop 停止读取。ByteBuf 越大那么 Netty 读取的次数就越少，ByteBuf 越小那么 Netty 读取的次数就越多，所以需要一种机制将 ByteBuf 的容量控制在一个合理的范围内。

Netty 会统计每一轮 read loop 总共读取了多少数据 —— totalBytesRead。

```
 public abstract class MaxMessageHandle implements ExtendedHandle {
        // 用于统计在一轮 read loop 中总共接收到客户端连接上的数据大小
        private int totalBytesRead;
 }
```

在每一轮的 read loop 结束之后，Netty 都会根据这个 totalBytesRead 来判断是否应该对 ByteBuf 进行扩容或者缩容，这样在下一轮 read loop 开始的时候，Netty 就可以用一个相对合理的容量去接收 socket 中的数据，尽量减少读取 socket 的次数。

```
private final class HandleImpl extends MaxMessageHandle {
        @Override
        public void readComplete() {
                // 是否对 ByteBuf 进行扩容或者缩容
                record(totalBytesRead());
        }
}
```

**那么在什么情况下需要对 ByteBuf 扩容，每次扩容多少 ？ 什么情况下需要对 ByteBuf 进行缩容，每次缩容多少呢** ？

这就用到了一个重要的容量索引结构 ——  SIZE\_TABLE，它里边定义索引了 ByteBuf 的每一种容量大小。相当于是扩缩容的容量索引表。每次扩容多少，缩容多少全部记录在这个容量索引表中。

```
public class AdaptiveRecvByteBufAllocator {
    // 扩容步长
    private static final int INDEX_INCREMENT = 4;
    // 缩容步长
    private static final int INDEX_DECREMENT = 1;

    // ByteBuf分配容量表（扩缩容索引表）按照表中记录的容量大小进行扩缩容
    private static final int[] SIZE_TABLE;
}
```

当索引容量`小于 512` 时，`SIZE_TABLE` 中定义的容量是从 `16` 开始按照 `16` 递增。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFuPyupLgXgjJ68q5iazHdQZUia2346qyLbLnQE62pl8K7VVicWFYV1c4Ew/640?wx_fmt=other&from=appmsg#imgIndex=14)

image.png

当索引容量`大于 512` 时，SIZE\_TABLE 中定义的容量是按前一个索引容量的 `2 倍`递增。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFiaAGtRopic88k2TNnWOUqt2c5Q3dEFEBLYsdZOkWdDfggW31Sp7NxBMQ/640?wx_fmt=other&from=appmsg#imgIndex=15)

image.png

那么当前 ByteBuf 的初始容量为 2048 ， 它在 SIZE\_TABLE 中的 index 为 33 。当一轮 read loop 读取完毕之后，如果发现 totalBytesRead 在`SIZE_TABLE[index - INDEX_DECREMENT]` 与 `SIZE_TABLE[index]` 之间的话，也就是如果本轮 read loop 结束之后总共读取的字节数在 \[1024 , 2048\] 之间。说明此时分配的 ByteBuf 容量正好，不需要进行缩容也不需要进行扩容。比如本次 totalBytesRead = 2000，正好处在 1024 与 2048 之间。说明 2048 的容量正好。

如果 totalBytesRead 小于等于 `SIZE_TABLE[index - INDEX_DECREMENT]`，也就是如果本轮 read loop 结束之后总共读取的字节数小于等于1024。表示本次读取到的字节数比当前 ByteBuf 容量的下一级容量还要小，说明当前 ByteBuf 的容量分配的有些大了，设置缩容标识`decreaseNow = true`。当下次 read loop 的时候如果继续满足缩容条件，那么就开始进行缩容。缩容后的容量为 SIZE\_TABLE\[index - INDEX\_DECREMENT\]，但不能小于SIZE\_TABLE\[minIndex\]（16）。

> 注意，这里需要满足两次缩容条件才会进行缩容，且缩容步长为 1 (INDEX\_DECREMENT)，缩容比较谨慎。

如果 totalBytesRead  大于等于当前 ByteBuf 容量—— nextReceiveBufferSize 时，说明 ByteBuf 的容量有点小了，需要进行扩容。扩容后的容量为 `SIZE_TABLE[index + INDEX_INCREMENT]`，但不能超过 SIZE\_TABLE\[maxIndex\]（65535）。

> 满足一次扩容条件就进行扩容，并且扩容步长为 4 (INDEX\_INCREMENT)， 扩容比较奔放。

```
        private void record(int actualReadBytes) {
            if (actualReadBytes <= SIZE_TABLE[max(0, index - INDEX_DECREMENT)]) {
                // 缩容条件触发两次之后就进行缩容
                if (decreaseNow) {
                    index = max(index - INDEX_DECREMENT, minIndex);
                    nextReceiveBufferSize = SIZE_TABLE[index];
                    decreaseNow = false;
                } else {
                    decreaseNow = true;
                }
            } else if (actualReadBytes >= nextReceiveBufferSize) {
                // 扩容条件满足一次之后就进行扩容
                index = min(index + INDEX_INCREMENT, maxIndex);
                nextReceiveBufferSize = SIZE_TABLE[index];
                decreaseNow = false;
            }
        }
```

### 2.6 ByteBuf 的引用计数设计

Netty 为 ByteBuf 引入了引用计数的机制，在 ByteBuf 的整个设计体系中，所有的 ByteBuf 都会继承一个抽象类 AbstractReferenceCountedByteBuf ， 它是对接口 ReferenceCounted 的实现。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFDO9aZqF2Mw3JOmgJYRcyzVGlaXvjUGeQ4SgpZnplT5QaXfLSBQialhg/640?wx_fmt=other&from=appmsg#imgIndex=16)

image.png

```
public interface ReferenceCounted {
     int refCnt();
     ReferenceCounted retain();
     ReferenceCounted retain(int increment);
     boolean release();
     boolean release(int decrement);
}
```

每个 ByteBuf 的内部都维护了一个叫做 refCnt 的引用计数，我们可以通过 `refCnt()` 方法来获取 ByteBuf 当前的引用计数 refCnt。当 ByteBuf 在其他上下文中被引用的时候，我们需要通过 `retain()` 方法将 ByteBuf 的引用计数加 1。另外我们也可以通过 `retain(int increment)` 方法来指定 refCnt 增加的大小（increment）。

有对 ByteBuf 的引用那么就有对 ByteBuf 的释放，每当我们使用完 ByteBuf 的时候就需要手动调用 `release()` 方法将 ByteBuf 的引用计数减 1 。当引用计数 refCnt 变成 0 的时候，Netty 就会通过 `deallocate` 方法来释放 ByteBuf 所引用的内存资源。这时 `release()` 方法会返回 true , 如果 refCnt 还不为 0 ，那么就返回 false 。同样我们也可以通过 `release(int decrement)` 方法来指定 refCnt 减少多少（decrement）。

#### 2.6.1 为什么要引入引用计数

”在其他上下文中引用 ByteBuf “ 是什么意思呢 ？ 比如我们在线程 1 中创建了一个  ByteBuf，然后将这个 ByteBuf 丢给线程 2 进行处理，线程 2 又可能丢给线程 3， 而每个线程都有自己的上下文处理逻辑，比如对 ByteBuf 的处理，释放等操作。这样就使得 ByteBuf 在事实上形成了在多个线程上下文中被共享的情况。

面对这种情况我们就很难在一个单独的线程上下文中判断一个 ByteBuf 该不该被释放，比如线程  1 准备释放 ByteBuf 了，但是它可能正在被其他线程使用。所以这也是 Netty 为 ByteBuf 引入引用计数的重要原因，每当引用一次 ByteBuf 的时候就需要通过 `retain()` 方法将引用计数加 1， `release()` 释放的时候将引用计数减 1 ，当引用计数为 0 了，说明已经没有其他上下文引用 ByteBuf 了，这时 Netty 就可以释放它了。

另外相比于 JDK DirectByteBuffer 需要依赖 GC 机制来释放其背后引用的 Native Memory , Netty 更倾向于手动及时释放 DirectByteBuf 。因为 JDK DirectByteBuffer 的释放需要等到 GC 发生，由于 DirectByteBuffer 的对象实例所占的 JVM 堆内存太小了，所以一时很难触发 GC , 这就导致被引用的 Native Memory 的释放有了一定的延迟，严重的情况会越积越多，导致 OOM 。而且也会导致进程中对 DirectByteBuffer 的申请操作有非常大的延迟。

而 Netty 为了避免这些情况的出现，选择在每次使用完毕之后手动释放 Native Memory ，但是不依赖 JVM 的话，总会有内存泄露的情况，比如在使用完了 ByteBuf 却忘记调用 `release()` 方法来释放。

所以为了检测内存泄露的发生，这也是 Netty 为 ByteBuf 引入了引用计数的另一个原因，当 ByteBuf 不再被引用的时候，也就是没有任何强引用或者软引用的时候，如果此时发生 GC , 那么这个 ByteBuf 实例（位于 JVM 堆中）就需要被回收了，这时 Netty 就会检查这个 ByteBuf 的引用计数是否为 0 ， 如果不为 0 ，说明我们忘记调用 `release()` 释放了，近而判断出这个 ByteBuf 发生了内存泄露。

在探测到内存泄露发生之后，后续 Netty 就会通过 `reportLeak()` 将内存泄露的相关信息以 `error` 的日志级别输出到日志中。

看到这里，大家可能不禁要问，不就是引入了一个小小的引用计数嘛，这有何难 ？ 值得这里大书特书吗 ？ 不就是在创建 ByteBuf 的时候将引用计数 refCnt 初始化为 1 ， 每次在其他上下文引用的时候将 refCnt 加 1， 每次释放的时候再将 refCnt 减 1 吗 ？减到 0 的时候就释放 Native Memory  ，太简单了吧~~

事实上 Netty 对引用计数的设计非常讲究，绝非如此简单，甚至有些复杂，其背后隐藏着大大的性能考究以及对复杂并发问题的全面考虑，在性能与线程安全问题之间的反复权衡。

#### 2.6.2 引用计数的最初设计

所以为了理清关于引用计数的整个设计脉络，我们需要将版本回退到最初的起点 —— 4.1.16.Final 版本，来看一下原始的设计。

```
public abstract class AbstractReferenceCountedByteBuf extends AbstractByteBuf {
    // 原子更新 refCnt 的 Updater
    private static final AtomicIntegerFieldUpdater<AbstractReferenceCountedByteBuf> refCntUpdater =
            AtomicIntegerFieldUpdater.newUpdater(AbstractReferenceCountedByteBuf.class, "refCnt");
    // 引用计数，初始化为 1
    private volatile int refCnt;

    protected AbstractReferenceCountedByteBuf(int maxCapacity) {
        super(maxCapacity);
        // 引用计数初始化为 1
        refCntUpdater.set(this, 1);
    }

    // 引用计数增加 increment
    private ByteBuf retain0(int increment) {
        for (;;) {
            int refCnt = this.refCnt;
            // 每次 retain 的时候对引用计数加 1
            final int nextCnt = refCnt + increment;

            // Ensure we not resurrect (which means the refCnt was 0) and also that we encountered an overflow.
            if (nextCnt <= increment) {
                // 如果 refCnt 已经为 0 或者发生溢出，则抛异常
                throw new IllegalReferenceCountException(refCnt, increment);
            }
            // CAS 更新 refCnt
            if (refCntUpdater.compareAndSet(this, refCnt, nextCnt)) {
                break;
            }
        }
        return this;
    }

    // 引用计数减少 decrement
    private boolean release0(int decrement) {
        for (;;) {
            int refCnt = this.refCnt;
            if (refCnt < decrement) {
                // 引用的次数必须和释放的次数相等对应
                throw new IllegalReferenceCountException(refCnt, -decrement);
            }
            // 每次 release 引用计数减 1 
            // CAS 更新 refCnt
            if (refCntUpdater.compareAndSet(this, refCnt, refCnt - decrement)) {
                if (refCnt == decrement) {
                    // 如果引用计数为 0 ，则释放 Native Memory，并返回 true
                    deallocate();
                    return true;
                }
                // 引用计数不为 0 ，返回 false
                return false;
            }
        }
    }
}
```

在 4.1.16.Final 之前的版本设计中，确实和我们当初想象的一样，非常简单，创建 ByteBuf 的时候将 refCnt 初始化为 1。 每次引用 retain 的时候将引用计数加 1 ，每次释放 release 的时候将引用计数减 1，在一个 for 循环中通过 CAS 替换。当引用计数为 0 的时候，通过 `deallocate()` 释放 Native Memory。

#### 2.6.3 引入指令级别上的优化

4.1.16.Final 的设计简洁清晰，在我们看来完全没有任何问题，但 Netty 对性能的考究完全没有因此止步，由于在 x86 架构下 XADD 指令的性能要高于 CMPXCHG 指令， compareAndSet 方法底层是通过 CMPXCHG 指令实现的，而 getAndAdd 方法底层是 XADD 指令。

所以在对性能极致的追求下，Netty 在 4.1.17.Final 版本中用 getAndAdd 方法来替换 compareAndSet 方法。

```
public abstract class AbstractReferenceCountedByteBuf extends AbstractByteBuf {

    private volatile int refCnt;

    protected AbstractReferenceCountedByteBuf(int maxCapacity) {
        super(maxCapacity);
        // 引用计数在初始的时候还是为 1 
        refCntUpdater.set(this, 1);
    }

    private ByteBuf retain0(final int increment) {
        // 相比于 compareAndSet 的实现，这里将 for 循环去掉
        // 并且每次是先对 refCnt 增加计数 increment
        int oldRef = refCntUpdater.getAndAdd(this, increment);
        // 增加完 refCnt 计数之后才去判断异常情况
        if (oldRef <= 0 || oldRef + increment < oldRef) {
            // Ensure we don't resurrect (which means the refCnt was 0) and also that we encountered an overflow.
            // 如果原来的 refCnt 已经为 0 或者 refCnt 溢出，则对 refCnt 进行回退，并抛出异常
            refCntUpdater.getAndAdd(this, -increment);
            throw new IllegalReferenceCountException(oldRef, increment);
        }
        return this;
    }

    private boolean release0(int decrement) {
        // 先对 refCnt 减少计数 decrement
        int oldRef = refCntUpdater.getAndAdd(this, -decrement);
        // 如果 refCnt 已经为 0 则进行 Native Memory 的释放
        if (oldRef == decrement) {
            deallocate();
            return true;
        } else if (oldRef < decrement || oldRef - decrement > oldRef) {
            // 如果释放次数大于 retain 次数 或者 refCnt 出现下溢
            // 则对 refCnt 进行回退，并抛出异常
            refCntUpdater.getAndAdd(this, decrement);
            throw new IllegalReferenceCountException(oldRef, decrement);
        }
        return false;
    }
}
```

在 4.1.16.Final 版本的实现中，Netty 是在一个 for 循环中，先对 retain 和 release 的异常情况进行校验，之后再通过 CAS 更新 refCnt。否则直接抛出 IllegalReferenceCountException。采用的是一种悲观更新引用计数的策略。

而在 4.1.17.Final 版本的实现中 ， Netty 去掉了 for 循环，正好和 compareAndSet 的实现相反，而是先通过 getAndAdd 更新 refCnt，更新之后再来判断相关的异常情况，如果发现有异常，则进行回退，并抛出 IllegalReferenceCountException。采用的是一种乐观更新引用计数的策略。

比如在 retain 增加引用计数的时候，先对 refCnt 增加计数 increment，然后判断原来的引用计数 oldRef 是否已经为 0 或者 refCnt 是否发生溢出，如果是，则需要对 refCnt 的值进行回退，并抛异常。

在 release 减少引用计数的时候，先对 refCnt 减少计数 decrement，然后判断 release 的次数是否大于 retain 的次数防止 over-release ，以及 refCnt 是否发生下溢，如果是，则对  refCnt 的值进行回退，并抛异常。

#### 2.6.4 并发安全问题的引入

在 4.1.17.Final 版本的设计中，我们对引用计数的 retain 以及 release 操作都要比 4.1.16.Final 版本的性能要高，虽然现在性能是高了，但是同时引入了新的并发问题。

让我们先假设一个这样的场景，现在有一个 ByteBuf，它当前的 refCnt = 1 ，线程 1 对这个 ByteBuf 执行 `release()` 操作。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFuXXQIy2MB7ialphtMsAmJ74dJsIGVOKtDZbBqb0r4P0xXFwC3WYFia9Q/640?wx_fmt=other&from=appmsg#imgIndex=17)

image.png

在 4.1.17.Final 的实现中，Netty 会首先通过 getAndAdd 将 refCnt 更新为 0 ，然后接着调用 `deallocate()` 方法释放 Native Memory ，很简单也很清晰是吧，让我们再加点并发复杂度上去。

现在我们在上图步骤一与步骤二之间插入一个线程 2 ， 线程 2 对这个 ByteBuf 并发执行 `retain()` 方法。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFSjXzKbWib6su7L4FsHJgPr4lI8DzbYaC6fRXXs6a0abA6MLcx0ibmMDw/640?wx_fmt=other&from=appmsg#imgIndex=18)

image.png

在 4.1.17.Final 的实现中，线程 2 首先通过 getAndAdd 将 refCnt 从 0 更新为 1，紧接着线程 2 就会发现 refCnt 原来的值 oldRef 是等于 0 的，也就是说线程 2 在调用  `retain()` 的时候，ByteBuf 的引用计数已经为 0 了，并且线程 1 已经开始准备释放 Native Memory 了。

所以线程 2 需要再次调用 getAndAdd 方法将 refCnt 的值进行回退，从 1 再次回退到 0 ，最后抛出 IllegalReferenceCountException。这样的结果显然是正确的，也是符合语义的。毕竟不能对一个引用计数为 0  的 ByteBuf 调用 `retain()` 。

现在看来一切风平浪静，都是按照我们的设想有条不紊的进行，我们不妨再加点并发复杂度上去。在上图步骤 1.1 与步骤 1.2 之间在插入一个线程 3 ， 线程 3 对这个 ByteBuf 再次并发执行 `retain()` 方法。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFQdbBJS9rkVCzAqIwP2EIxhTl7XQ7ibhV5paJAFZ8AEQ2Kxg7Mv3kAhg/640?wx_fmt=other&from=appmsg#imgIndex=19)

image.png

由于引用计数的更新（步骤 1.1）与引用计数的回退（步骤 1.2）这两个操作并不是一个原子操作，如果在这两个操作之间不巧插入了一个线程 3 ，线程 3 在并发执行 `retain()` 方法的时候，首先会通过 getAndAdd 将引用计数 refCnt 从 1 增加到 2 。

> **注意，此时线程 2 还没来得及回退 refCnt ， 所以线程 3 此时看到的 refCnt 是 1 而不是 0** 。

由于此时线程 3 看到的 oldRef 是 1 ，所以线程 3 成功调用 `retain()` 方法将 ByteBuf 的引用计数增加到了 2 ，并且不会回退也不会抛出异常。在线程 3 看来此时的 ByteBuf 完完全全是一个正常可以被使用的 ByteBuf。

紧接着线程 1 开始执行步骤 2 —— `deallocate()` 方法释放 Native Memory，此后线程 3 在访问这个 ByteBuf 的时候就有问题了，因为  Native Memory 已经被线程1 释放了。

#### 2.6.5 在性能与并发安全之间的权衡

接下来 Netty 就需要在性能与并发安全之间进行权衡了，现在有两个选择，第一个选择是直接回滚到 4.1.16.Final 版本，放弃 XADD 指令带来的性能提升，之前的设计中采用的 CMPXCHG 指令虽然性能相对差一些，但是不会出现上述的并发安全问题。

因为 Netty 是在一个 for 循环中采用悲观的策略来更新引用计数，先是判断异常情况，然后在通过 CAS 来更新 refCnt。即使多个线程看到了 refCnt 的中间状态也没关系，因为接下来进行的 CAS 也会跟着失败。

比如上边例子中的线程 1 对 ByteBuf 进行 release 的时候，在线程 1 执行 CAS 将 refCnt 替换为 0 之前的这个间隙中，refCnt 是 1 ，如果在这个间隙中，线程 2 并发执行 retain 方法，此时线程 2 看到的 refCnt 确实为 1 ，它是一个中间状态，线程 2 执行 CAS 将 refCnt 替换为 2。

此时线程 1 执行 CAS 就会失败，但会在下一轮 for 循环中将 refCnt 替换为 1，这是完全符合引用计数语义的。

另外一种情况是线程 1 已经执行完 CAS 将 refCnt 替换为 0 ，这时候线程 2 去 retain  ，由于 4.1.16.Final 版本中的设计是先检查异常后 CAS 替换，所以线程 2 首先会在 retain 方法中检查到 ByteBuf 的 refCnt 已经为 0 ，直接抛出 IllegalReferenceCountException，并不会执行 CAS 。这同样符合引用计数的语义，毕竟不能对一个引用计数已经为 0 的 ByteBuf 执行任何访问操作。

第二个选择是既要保留 XADD 指令带来的性能提升，也要解决 4.1.17.Final 版本中引入的并发安全问题。毫无疑问，Netty 最终选择的是这种方案。

在介绍 Netty 的精彩设计之前，我想我们还是应该在回顾下这个并发安全问题出现的根本原因是什么 ？

在 4.1.17.Final 版本的设计中，Netty 首先是通过 getAndAdd 方法先对 refCnt 的值进行更新，如果出现异常情况，在进行回滚。而更新，回滚的这两个操作并不是原子的，之间的中间状态会被其他线程看到。

比如，线程 2 看到了线程 1 的中间状态（refCnt = 0），于是将引用计数加到 1 , 在线程 2 进行回滚之前，这期间的中间状态（refCnt = 1，oldRef = 0）又被线程 3 看到了，于是线程 3 将引用计数增加到了 2 （refCnt = 2，oldRef = 1）。 此时线程 3 觉得这是一种正常的状态，但在线程 1 看来 refCnt 的值已经是 0 了，后续线程 1 就会释放 Native Memory ，这就出问题了。

问题的根本原因其实是这里的 refCnt 不同的值均代表不同的语义，比如对于线程 1 来说，通过 release 将 refCnt 减到了 0 ，这里的语义是 ByteBuf 已经不在被引用了，可以释放 Native Memory 。

随后线程 2 通过 retain 将 refCnt 加到了 1 ，这就把 ByteBuf 语义改变了，表示该 ByteBuf 在线程 2 中被引用了一次。最后线程 3 又通过 retain 将 refCnt 加到了 2 ，再一次改变了 ByteBuf 的语义。

只要用到 XADD 指令来实现引用计数的更新，那么就不可避免的出现上述并发更新 refCnt 的情况，关键是 refCnt 的值每一次被其他线程并发修改之后，ByteBuf 的语义就变了。这才是 4.1.17.Final 版本中的关键问题所在。

如果 Netty 想在同时享受 XADD 指令带来的性能提升之外，又要解决上述提到的并发安全问题，就要重新对引用计数进行设计。首先我们的要求是继续采用 XADD 指令来实现引用计数的更新，但这就会带来多线程并发修改所引起的 ByteBuf 语义改变。

既然多线程并发修改无法避免，那么我们能不能重新设计一下引用计数，让  ByteBuf 语义无论多线程怎么修改，它的语义始终保持不变。也就是说只要线程 1 将 refCnt 减到了 0 ，那么无论线程 2 和线程 3 怎么并发修改 refCnt，怎么增加 refCnt 的值，refCnt 等于 0 的这个语义始终保持不变呢 ？

#### 2.6.6 奇偶设计的引入

这里 Netty 有一个极奇巧妙精彩的设计，引用计数的设计不再是逻辑意义上的 `0 , 1 , 2 , 3 .....`，而是分为了两大类，要么是偶数，要么是奇数。

*   偶数代表的语义是 ByteBuf 的 refCnt 不为 0 ，也就是说只要一个 ByteBuf 还在被引用，那么它的 refCnt 就是一个偶数，具体被引用多少次，可以通过 `refCnt >>> 1` 来获取。
    
*   奇数代表的语义是 ByteBuf 的 refCnt 等于 0 ，只要一个 ByteBuf 已经没有任何地方引用它了，那么它的 refCnt 就是一个奇数，其背后引用的 Native Memory 随后就会被释放。
    

ByteBuf 在初始化的时候，refCnt 不在是 1 而是被初始化为 2 （偶数），每次 retain 的时候不在是对 refCnt 加 1 而是加 2 （偶数步长），每次 release 的时候不再是对  refCnt 减 1 而是减 2 （同样是偶数步长）。这样一来，只要一个 ByteBuf 的引用计数为偶数，那么多线程无论怎么并发调用 retain 方法，引用计数还是一个偶数，语义仍然保持不变。

```
   public final int initialValue() {
        return 2;
    }
```

当一个 ByteBuf 被 release 到没有任何引用计数的时候，Netty 不在将 refCnt 设置为 0 而是设置为 1 （奇数），对于一个值为奇数的 refCnt，无论多线程怎么并发调用 retain 方法和 release 方法，引用计数还是一个奇数，ByteBuf 引用计数为 0 的这层语义一直会保持不变。

我们还是以上图中所展示的并发安全问题为例，在新的引用计数设计方案中，首先线程 1 对 ByteBuf 执行 release 方法，Netty 会将 refCnt 设置为 1 （奇数）。

线程 2 并发调用 retain 方法，通过 getAndAdd 将 refCnt 从 1 加到了 3 ，refCnt 仍然是一个奇数，按照奇数所表示的语义 —— ByteBuf 引用计数已经是 0  了，那么线程 2 就会在 retain 方法中抛出 IllegalReferenceCountException。

线程 3 并发调用 retain 方法，通过 getAndAdd 将 refCnt 从 3 加到了 5，看到了没 ，在新方案的设计中，无论多线程怎么并发执行 retain 方法，refCnt 的值一直都只会是一个奇数，随后线程 3 在 retain 方法中抛出 IllegalReferenceCountException。这完全符合引用计数的并发语义。

这个新的引用计数设计方案是在 4.1.32.Final 版本引入进来的，仅仅通过一个奇偶设计，就非常巧妙的解决了 4.1.17.Final 版本中存在的并发安全问题。现在新方案的核心设计要素我们已经清楚了，那么接下来笔者将以 4.1.56.Final 版本来为大家继续介绍下新方案的实现细节。

Netty 中的 ByteBuf 全部继承于 AbstractReferenceCountedByteBuf，在这个类中实现了所有对 ByteBuf 引用计数的操作，对于 ReferenceCounted 接口的实现就在这里。

```
public abstract class AbstractReferenceCountedByteBuf extends AbstractByteBuf {
    // 获取 refCnt 字段在 ByteBuf 对象内存中的偏移
    // 后续通过 Unsafe 对 refCnt 进行操作
    private static final long REFCNT_FIELD_OFFSET =
            ReferenceCountUpdater.getUnsafeOffset(AbstractReferenceCountedByteBuf.class, "refCnt");

    // 获取 refCnt 字段 的 AtomicFieldUpdater
    // 后续通过 AtomicFieldUpdater 来操作 refCnt 字段
    private static final AtomicIntegerFieldUpdater<AbstractReferenceCountedByteBuf> AIF_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(AbstractReferenceCountedByteBuf.class, "refCnt");

    // 创建 ReferenceCountUpdater，对于引用计数的所有操作最终都会代理到这个类中
    private static final ReferenceCountUpdater<AbstractReferenceCountedByteBuf> updater =
            new ReferenceCountUpdater<AbstractReferenceCountedByteBuf>() {
        @Override
        protected AtomicIntegerFieldUpdater<AbstractReferenceCountedByteBuf> updater() {
            // 通过 AtomicIntegerFieldUpdater 操作 refCnt 字段
            return AIF_UPDATER;
        }
        @Override
        protected long unsafeOffset() {
            // 通过 Unsafe 操作 refCnt 字段
            return REFCNT_FIELD_OFFSET;
        }
    };
    // ByteBuf 中的引用计数，初始为 2 （偶数）
    private volatile int refCnt = updater.initialValue();
}
```

其中定义了一个 refCnt 字段用于记录 ByteBuf 被引用的次数，由于采用了奇偶设计，在创建 ByteBuf 的时候，Netty 会将 refCnt 初始化为 2 （偶数），它的逻辑语义是该 ByteBuf 被引用一次。后续对 ByteBuf 执行 retain 就会对 refCnt 进行加 2 ，执行 release 就会对 refCnt 进行减 2 ，对于引用计数的单次操作都是以 2 为步长进行。

由于在 Netty 中除了 AbstractReferenceCountedByteBuf 这个专门用于实现 ByteBuf 的引用计数功能之外，还有一个更加通用的引用计数抽象类 AbstractReferenceCounted，它用于实现所有系统资源类的引用计数功能（ByteBuf 只是其中的一种内存资源）。

由于都是对引用计数的实现，所以在之前的版本中，这两个类中包含了很多重复的引用计数相关操作逻辑，所以 Netty 在 4.1.35.Final  版本中专门引入了一个 ReferenceCountUpdater 类，将所有引用计数的相关实现聚合在这里。

ReferenceCountUpdater 对于引用计数 refCnt 的操作有两种方式，一种是通过 AtomicFieldUpdater 来对 refCnt 进行操作，我们可以通过 `updater()` 获取到 refCnt 字段对应的 AtomicFieldUpdater。

另一种则是通过 Unsafe 来对 refCnt 进行操作，我们可以通过 `unsafeOffset()` 来获取到 refCnt 字段在 ByteBuf 实例对象内存中的偏移。

按理来说，我们采用一种方式就可以对 refCnt 进行访问或者更新了，那为什么 Netty 提供了两种方式呢 ？会显得有点多余吗 ？这个点大家可以先思考下为什么 ，后续在我们剖析到源码细节的时候笔者在为大家解答。

好了，下面我们正式开始介绍新版引用计数设计方案的具体实现细节，第一个问题，在新的设计方案中，我们如何获取 ByteBuf 的逻辑引用计数 ？

```
public abstract class ReferenceCountUpdater<T extends ReferenceCounted> {
    public final int initialValue() {
        // ByteBuf 引用计数初始化为 2
        return 2;
    }

    public final int refCnt(T instance) {
        // 通过 updater 获取 refCnt
        // 根据 refCnt 在  realRefCnt 中获取真实的引用计数
        return realRefCnt(updater().get(instance));
    }
    // 获取 ByteBuf 的逻辑引用计数
    private static int realRefCnt(int rawCnt) {
        // 奇偶判断
        return rawCnt != 2 && rawCnt != 4 && (rawCnt & 1) != 0 ? 0 : rawCnt >>> 1;
    }
}
```

由于采用了奇偶引用计数的设计，所以我们在获取逻辑引用计数的时候需要判断当前 rawCnt（refCnt）是奇数还是偶数，它们分别代表了不同的语义。

*   如果 rawCnt 是奇数，则表示当前 ByteBuf 已经没有任何地方引用了，逻辑引用计数返回 0.
    
*   如果 rawCnt 是偶数，则表示当前 ByteBuf 还有地方在引用，逻辑引用计数则为 `rawCnt >>> 1`。
    

realRefCnt 函数其实就是简单的一个奇偶判断逻辑，但在它的实现中却体现出了 Netty 对性能的极致追求。比如，我们判断一个数是奇数还是偶数其实很简单，直接通过  `rawCnt & 1` 就可以判断，如果返回 0 表示 rawCnt 是一个偶数，如果返回 1 表示 rawCnt 是一个奇数。

但是我们看到 Netty 在奇偶判断条件的前面又加上了 `rawCnt != 2 && rawCnt != 4` 语句，这是干嘛的呢 ？

其实 Netty 这里是为了尽量用性能更高的 `==` 运算来代替 `&` 运算，但又不可能用 `==` 运算来枚举出所有的偶数值（也没这必要），所以只用 `==` 运算来判断在实际场景中经常出现的引用计数，一般经常出现的引用计数值为 2 或者 4 ， 也就是说 ByteBuf 在大部分场景下只会被引用 1 次或者 2 次，对于这种高频出现的场景，Netty 用 `==` 运算来针对性优化，低频出现的场景就回退到 `&` 运算。

> 大部分性能优化的套路都是相同的，我们通常不能一上来就奢求一个大而全的针对全局的优化方案，这是不可能的，也是十分低效的。往往最有效的，可以立竿见影的优化方案都是针对局部热点进行专门优化。

对引用计数的设置也是一样，都需要考虑奇偶的转换，我们在 `setRefCnt` 方法中指定的参数 refCnt 表示逻辑上的引用计数 —— `0, 1 , 2 , 3 ....`，但要设置到 ByteBuf 时，就需要对逻辑引用计数在乘以 2 ，让它始终是一个偶数。

```
    public final void setRefCnt(T instance, int refCnt) {
        updater().set(instance, refCnt > 0 ? refCnt << 1 : 1); // overflow OK here
    }
```

有了这些基础之后，我们下面就来看一下在新版本的 retain 方法设计中，Netty 是如何解决 4.1.17.Final 版本存在的并发安全问题。首先 Netty 对引用计数的奇偶设计对于用户来说是透明的。引用计数对于用户来说仍然是普通的自然数 —— `0, 1 , 2 , 3 ....` 。

所以每当用户调用 retain 方法试图增加 ByteBuf 的引用计数时，通常是指定逻辑增加步长 —— increment（用户视角），而在具体的实现角度，Netty 会增加两倍的 increment （rawIncrement）到 refCnt 字段中。

```
    public final T retain(T instance) {
        // 引用计数逻辑上是加 1 ，但实际上是加 2 （实现角度）
        return retain0(instance, 1, 2);
    }

    public final T retain(T instance, int increment) {
        // all changes to the raw count are 2x the "real" change - overflow is OK
        // rawIncrement 始终是逻辑计数 increment 的两倍
        int rawIncrement = checkPositive(increment, "increment") << 1;
        // 将 rawIncrement 设置到 ByteBuf 的 refCnt 字段中
        return retain0(instance, increment, rawIncrement);
    }

    // rawIncrement = increment << 1
    // increment 表示引用计数的逻辑增长步长
    // rawIncrement 表示引用计数的实际增长步长
    private T retain0(T instance, final int increment, final int rawIncrement) {
        // 先通过 XADD 指令将  refCnt 的值加起来
        int oldRef = updater().getAndAdd(instance, rawIncrement);
        // 如果 oldRef 是一个奇数，也就是 ByteBuf 已经没有引用了，抛出异常
        if (oldRef != 2 && oldRef != 4 && (oldRef & 1) != 0) {
            // 如果 oldRef 已经是一个奇数了，无论多线程在这里怎么并发 retain ，都是一个奇数，这里都会抛出异常
            throw new IllegalReferenceCountException(0, increment);
        }
        // don't pass 0! 
        // refCnt 不可能为 0 ，只能是 1
        if ((oldRef <= 0 && oldRef + rawIncrement >= 0)
                || (oldRef >= 0 && oldRef + rawIncrement < oldRef)) {
            // 如果 refCnt 字段已经溢出，则进行回退，并抛异常
            updater().getAndAdd(instance, -rawIncrement);
            throw new IllegalReferenceCountException(realRefCnt(oldRef), increment);
        }
        return instance;
    }
```

首先新版本的 retain0 方法仍然保留了 4.1.17.Final 版本引入的  XADD 指令带来的性能优势，大致的处理逻辑也是类似的，一上来先通过 getAndAdd 方法将 refCnt 增加 rawIncrement，对于 `retain(T instance)` 来说这里直接加 2 。

然后判断原来的引用计数 oldRef 是否是一个奇数，如果是一个奇数，那么就表示 ByteBuf 已经没有任何引用了，逻辑引用计数早已经为 0 了，那么就抛出 IllegalReferenceCountException。

在引用计数为奇数的情况下，无论多线程怎么对 refCnt 并发加 2 ，refCnt 始终是一个奇数，最终都会抛出异常。解决并发安全问题的要点就在这里，一定要保证 retain 方法的并发执行不能改变原来的语义。

最后会判断一下 refCnt 字段是否发生溢出，如果溢出，则进行回退，并抛出异常。下面我们仍然以之前的并发场景为例，用一个具体的例子，来回味一下奇偶设计的精妙之处。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFBnmfsksxgJ3DAERq67QZv54SLN7NPNiaVmdIBkKFqHX9pDGhOk1nibiaA/640?wx_fmt=other&from=appmsg#imgIndex=20)

image.png

现在线程 1 对一个 refCnt 为 2 的 ByteBuf 执行 release 方法，这时 ByteBuf 的逻辑引用计数就为 0 了，对于一个没有任何引用的 ByteBuf 来说，新版的设计中它的 refCnt 只能是一个奇数，不能为 0 ，所以这里 Netty 会将 refCnt 设置为 1 。然后在步骤 2 中调用 deallocate 方法释放 Native Memory。

线程 2 在步骤 1 和步骤 2 之间插入进来对  ByteBuf 并发执行 retain 方法，这时线程 2 看到的 refCnt 是 1，然后通过 getAndAdd 将 refCnt 加到了 3 ，仍然是一个奇数，随后抛出 IllegalReferenceCountException 异常。

线程 3 在步骤 1.1 和步骤 1.2 之间插入进来再次对 ByteBuf 并发执行 retain 方法，这时线程 3 看到的 refCnt 是 3，然后通过 getAndAdd 将 refCnt 加到了 5 ，还是一个奇数，随后抛出 IllegalReferenceCountException 异常。

这样一来就保证了引用计数的并发语义 —— 只要一个 ByteBuf 没有任何引用的时候（refCnt = 1），其他线程无论怎么并发执行  retain 方法都会得到一个异常。

但是引用计数并发语义的保证不能单单只靠 retain 方法，它还需要与 release 方法相互配合协作才可以，所以为了并发语义的保证 ， release 方法的设计就不能使用性能更高的 XADD 指令，而是要回退到  CMPXCHG 指令来实现。

为什么这么说呢 ？因为新版引用计数的设计采用的是奇偶实现，refCnt 为偶数表示 ByteBuf 还有引用，refCnt 为奇数表示 ByteBuf 已经没有任何引用了，可以安全释放 Native Memory 。对于一个 refCnt 已经为奇数的 ByteBuf 来说，无论多线程怎么并发执行 retain 方法，得到的 refCnt 仍然是一个奇数，最终都会抛出 IllegalReferenceCountException，这就是引用计数的并发语义 。

为了保证这一点，就需要在每次调用 retain ，release 方法的时候，以偶数步长来更新 refCnt，比如每一次调用 retain 方法就对 refCnt 加 2 ，每一次调用 release 方法就对 refCnt 减 2 。

但总有一个时刻，refCnt 会被减到 0 的对吧，在新版的奇偶设计中，refCnt 是不允许为 0 的，因为一旦 refCnt 被减到了 0 ，多线程并发执行 retain 之后，就会将 refCnt 再次加成了偶数，这又会出现并发问题。

而每一次调用 release 方法是对 refCnt 减 2 ，如果我们采用 XADD 指令实现 release 的话，回想一下 4.1.17.Final 版本中的设计，它首先进来是通过 getAndAdd 方法对 refCnt 减 2 ，这样一来，refCnt 就变成 0 了，就有并发安全问题了。所以我们需要通过 CMPXCHG 指令将 refCnt 更新为 1。

这里有的同学可能要问了，那可不可以先进行一下 if 判断，如果 refCnt 减 2 之后变为 0 了，我们在通过 getAndAdd 方法将 refCnt 更新为 1 （减一个奇数），这样一来不也可以利用上 XADD 指令的性能优势吗 ？

答案是不行的，因为 if 判断与 getAndAdd 更新这两个操作之间仍然不是原子的，多线程可以在这个间隙仍然有并发执行 retain 方法的可能，如下图所示：

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFLXkWjHGXdBD25qQIH39Fia7H69JeDAGUmBBfo87icffOXwt7ibEQjKia9g/640?wx_fmt=other&from=appmsg#imgIndex=21)

image.png

在线程 1 执行 if 判断和 getAndAdd 更新这两个操作之间，线程 2 看到的 refCnt 其实 2 ，然后线程 2 会将 refCnt 加到 4 ，线程 3 紧接着会将 refCnt 增加到 6 ，在线程 2 和线程 3 看来这个 ByteBuf 完全是正常的，但是线程 1 马上就会释放 Native Memory 了。

而且采用这种设计的话，一会通过 getAndAdd 对 refCnt 减一个奇数，一会通过 getAndAdd 对 refCnt 加一个偶数，这样就把原本的奇偶设计搞乱掉了。

所以我们的设计目标是一定要保证在 ByteBuf 没有任何引用计数的时候，release 方法需要原子性的将 refCnt 更新为 1 。 因此必须采用 CMPXCHG 指令来实现而不能使用 XADD 指令。

**再者说， CMPXCHG 指令是可以原子性的判断当前是否有并发情况的，如果有并发情况出现，CAS  就会失败，我们可以继续重试。但 XADD 指令却无法原子性的判断是否有并发情况，因为它每次都是先更新，后判断并发，这就不是原子的了。这一点，在下面的源码实现中会体现的特别明显**。

#### 2.6.7 尽量避免内存屏障的开销

```
    public final boolean release(T instance) {
        // 第一次尝试采用 unSafe nonVolatile 的方式读取 refCnf 的值
        int rawCnt = nonVolatileRawCnt(instance);
        // 如果逻辑引用计数被减到 0 了，那么就通过 tryFinalRelease0 使用 CAS 将 refCnf 更新为 1
        // CAS 失败的话，则通过 retryRelease0 进行重试
        // 如果逻辑引用计数不为 0 ，则通过 nonFinalRelease0 将 refCnf 减 2
        return rawCnt == 2 ? tryFinalRelease0(instance, 2) || retryRelease0(instance, 1)
                : nonFinalRelease0(instance, 1, rawCnt, toLiveRealRefCnt(rawCnt, 1));
    }
```

这里有一个小的细节再次体现出 Netty 对于性能的极致追求，refCnt 字段在 ByteBuf 中被 Netty 申明为一个 volatile 字段。

```
private volatile int refCnt = updater.initialValue();
```

我们对 refCnt 的普通读写都是要走内存屏障的，但 Netty 在 release 方法中首次读取 refCnt 的值是采用 nonVolatile 的方式，不走内存屏障，直接读取 cache line，避免了屏障开销。

```
    private int nonVolatileRawCnt(T instance) {
        // 获取 REFCNT_FIELD_OFFSET
        final long offset = unsafeOffset();
        // 通过 UnSafe 的方式来访问 refCnt ， 避免内存屏障的开销
        return offset != -1 ? PlatformDependent.getInt(instance, offset) : updater().get(instance);
    }
```

那有的同学可能要问了，如果读取 refCnt 的时候不走内存屏障的话，读取到的 refCnt 不就可能是一个错误的值吗 ？

事实上确实是这样的，但 Netty 不 care , 读到一个错误的值也无所谓，因为这里的引用计数采用了奇偶设计，我们在第一次读取引用计数的时候并不需要读取到一个精确的值，既然这样我们可以直接通过 UnSafe 来读取，还能剩下一笔内存屏障的开销。

那为什么不需要一个精确的值呢 ？因为如果原来的 refCnt 是一个奇数，那无论多线程怎么并发 retain ，最终得到的还是一个奇数，我们这里只需要知道 refCnt 是一个奇数就可以直接抛 IllegalReferenceCountException 了。具体读到的是一个 3 还是一个 5 其实都无所谓。

那如果原来的 refCnt 是一个偶数呢 ？其实也无所谓，我们可能读到一个正确的值也可能读到一个错误的值，如果恰好读到一个正确的值，那更好。如果读取到一个错误的值，也无所谓，因为我们后面是用 CAS 进行更新，这样的话 CAS 就会更新失败，我们只需要在一下轮 for 循环中更新正确就可以了。

如果读取到的 refCnt 恰好是 2 ，那就意味着本次 release 之后，ByteBuf 的逻辑引用计数就为 0 了，Netty 会通过 CAS 将 refCnt 更新为 1 。

```
   private boolean tryFinalRelease0(T instance, int expectRawCnt) {
        return updater().compareAndSet(instance, expectRawCnt, 1); // any odd number will work
    }
```

如果 CAS 更新失败，则表示此时有多线程可能并发对 ByteBuf 执行 retain 方法，逻辑引用计数此时可能就不为 0 了，针对这种并发情况，Netty 会在 retryRelease0 方法中进行重试，将 refCnt 减 2 。

```
    private boolean retryRelease0(T instance, int decrement) {
        for (;;) {
            // 采用 Volatile 的方式读取 refCnt
            int rawCnt = updater().get(instance), 
            // 获取逻辑引用计数，如果 refCnt 已经变为奇数，则抛出异常
            realCnt = toLiveRealRefCnt(rawCnt, decrement);
            // 如果执行完本次 release , 逻辑引用计数为 0
            if (decrement == realCnt) {
                // CAS 将 refCnt 更新为 1
                if (tryFinalRelease0(instance, rawCnt)) {
                    return true;
                }
            } else if (decrement < realCnt) {
                // 原来的逻辑引用计数 realCnt 大于 1（decrement）
                // 则通过 CAS 将 refCnt 减 2
                if (updater().compareAndSet(instance, rawCnt, rawCnt - (decrement << 1))) {
                    return false;
                }
            } else {
                // refCnt 字段如果发生溢出，则抛出异常
                throw new IllegalReferenceCountException(realCnt, -decrement);
            }
            // CAS 失败之后调用 yield
            // 减少无畏的竞争，否则所有线程在高并发情况下都在这里 CAS 失败
            Thread.yield(); 
        }
    }
```

从 retryRelease0 方法的实现中我们可以看出，CAS 是可以原子性的探测到是否有并发情况出现的，如果有并发情况，这里的所有 CAS 都会失败，随后会在下一轮 for 循环中将正确的值更新到 refCnt 中。这一点 ，XADD 指令是做不到的。

如果在进入 release 方法后，第一次读取的 refCnt 不是 2 ，那么就不能走上面的 tryFinalRelease0 逻辑，而是在 nonFinalRelease0 中通过 CAS 将 refCnt 的值减 2 。

```
   private boolean nonFinalRelease0(T instance, int decrement, int rawCnt, int realCnt) {
        if (decrement < realCnt
                && updater().compareAndSet(instance, rawCnt, rawCnt - (decrement << 1))) {
            // ByteBuf 的 rawCnt 减少 2 * decrement
            return false;
        }
        // CAS  失败则一直重试，如果引用计数已经为 0 ，那么抛出异常，不能再次 release
        return retryRelease0(instance, decrement);
    }
```

到这里，Netty 对引用计数的精彩设计，笔者就为大家完整的剖析完了，一共有四处非常精彩的优化设计，我们总结如下：

1.  使用性能更优的  XADD 指令来替换 CMPXCHG 指令。
    
2.  引用计数采用了奇偶设计，保证了并发语义。
    
3.  采用性能更优的 `==` 运算来替换 `&` 运算。
    
4.  能不走内存屏障就尽量不走内存屏障。
    

### 2.7 ByteBuf 的视图设计

和 JDK 的设计一样，Netty 中的 ByteBuf 也可以通过 `slice()` 方法以及 `duplicate()` 方法创建一个视图 ByteBuf 出来，原生 ByteBuf 和它的视图 ByteBuf 底层都是共用同一片内存区域，也就是说在视图 ByteBuf 上做的任何改动都会反应到原生 ByteBuf 上。同理，在原生 ByteBuf 上做的任何改动也会反应到它的视图 ByteBuf 上。我们可以将视图 ByteBuf 看做是原生 ByteBuf 的一份浅拷贝。

原生 ByteBuf 和它的视图 ByteBuf 不同的是，它们都有各自独立的 readerIndex，writerIndex，capacity，maxCapacity。

`slice()` 方法是在原生 ByteBuf 的 `[readerIndex , writerIndex)` 这段内存区域内创建一个视图 ByteBuf。也就是原生 ByteBuf 和视图 ByteBuf 共用  `[readerIndex , writerIndex)` 这段内存区域。视图 ByteBuf 的数据区域其实就是原生 ByteBuf 的可读字节区域。

视图 ByteBuf 的 readerIndex = 0 ， writerIndex = capacity = maxCapacity = 原生 ByteBuf 的 `readableBytes()` 。

```
  @Override
    public int readableBytes() {
        // 原生 ByteBuf
        return writerIndex - readerIndex;
    }

```

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFGaeHLAPGSrMPZSo24FSE5blh9AnCfuLGSrAGW673ENmTpu62qFYvkg/640?wx_fmt=other&from=appmsg#imgIndex=22)

image.png

下面我们来看一下 `slice()` 方法创建视图 ByteBuf 的逻辑实现：

```
public abstract class AbstractByteBuf extends ByteBuf {
    @Override
    public ByteBuf slice() {
        return slice(readerIndex, readableBytes());
    }

    @Override
    public ByteBuf slice(int index, int length) {
        // 确保 ByteBuf 的引用计数不为 0 
        ensureAccessible();
        return new UnpooledSlicedByteBuf(this, index, length);
    }
}
```

Netty 会将 slice 视图 ByteBuf 封装在 UnpooledSlicedByteBuf 类中，在这里会初始化 slice 视图 ByteBuf 的 readerIndex，writerIndex，capacity，maxCapacity。

```
class UnpooledSlicedByteBuf extends AbstractUnpooledSlicedByteBuf {
    UnpooledSlicedByteBuf(AbstractByteBuf buffer, int index, int length) {
        // index = readerIndex
        // length = readableBytes()
        super(buffer, index, length);
    }

    @Override
    public int capacity() {
        // 视图 ByteBuf 的 capacity 和 maxCapacity 相等
        // 均为原生 ByteBuf 的 readableBytes() 
        return maxCapacity();
    }
}
```

如上图所示，这里的 index 就是原生 ByteBuf 的 readerIndex = 4 ，index 用于表示视图 ByteBuf 的内存区域相对于原生 ByteBuf 的偏移，因为视图 ByteBuf 与原生 ByteBuf 共用的是同一片内存区域，针对视图 ByteBuf 的操作其实底层最终是转换为对原生 ByteBuf 的操作。

但由于视图 ByteBuf  和原生 ByteBuf 各自都有独立的 readerIndex 和 writerIndex，比如上图中，视图 ByteBuf 中的 readerIndex = 0 其实指向的是原生 ByteBuf 中 readerIndex = 4 的位置。所以每次在我们对视图 ByteBuf 进行读写的时候都需要将视图 ByteBuf 的 readerIndex 加上一个偏移（index）转换成原生 ByteBuf 的 readerIndex，近而从原生 ByteBuf 中来读写数据。

```
   @Override
    protected byte _getByte(int index) {
        // 底层其实是对原生 ByteBuf 的访问
        return unwrap()._getByte(idx(index));
    }

    @Override
    protected void _setByte(int index, int value) {
        unwrap()._setByte(idx(index), value);
    }

   /**
     * Returns the index with the needed adjustment.
     */
    final int idx(int index) {
        // 转换为原生 ByteBuf 的 readerIndex 或者 writerIndex
        return index + adjustment;
    }
```

`idx(int index)` 方法中的 adjustment 就是上面 UnpooledSlicedByteBuf 构造函数中的 index 偏移，初始化为原生 ByteBuf 的 readerIndex。

length 则初始化为原生 ByteBuf 的 `readableBytes()`，视图 ByteBuf 中的 writerIndex，capacity，maxCapacity 都是用 length 来初始化。

```
abstract class AbstractUnpooledSlicedByteBuf extends AbstractDerivedByteBuf {
    // 原生 ByteBuf
    private final ByteBuf buffer;
    // 视图 ByteBuf 相对于原生 ByteBuf的数据区域偏移
    private final int adjustment;

    AbstractUnpooledSlicedByteBuf(ByteBuf buffer, int index, int length) {
        // 设置视图 ByteBuf 的 maxCapacity，readerIndex 为 0 
        super(length);
        // 原生 ByteBuf
        this.buffer = buffer;
        // 数据偏移为原生 ByteBuf 的 readerIndex
        adjustment = index;
        // 设置视图 ByteBuf 的 writerIndex
        writerIndex(length);
    }
}
```

但是通过 `slice()` 方法创建出来的视图 ByteBuf 并不会改变原生 ByteBuf 的引用计数，这会存在一个问题，就是由于视图 ByteBuf 和原生 ByteBuf 底层共用的是同一片内存区域，在原生 ByteBuf 或者视图 ByteBuf 各自的应用上下文中他们可能并不会意识到对方的存在。

如果对原生 ByteBuf 调用 release 方法，恰好引用计数就为 0 了，接着就会释放原生 ByteBuf 的 Native Memory 。此时再对视图 ByteBuf 进行访问就有问题了，因为  Native Memory 已经被原生 ByteBuf 释放了。同样的道理，对视图 ByteBuf 调用 release 方法 ，也会对原生 ByteBuf 产生影响。

为此 Netty 提供了一个 `retainedSlice()` 方法，在创建 slice 视图 ByteBuf 的同时对原生 ByteBuf 的引用计数加 1 ，两者共用同一个引用计数。

```
    @Override
    public ByteBuf retainedSlice() {
        // 原生 ByteBuf 的引用计数加 1
        return slice().retain();
    }
```

除了 `slice()` 之外，Netty 也提供了 `duplicate()` 方法来创建视图 ByteBuf 。

```
    @Override
    public ByteBuf duplicate() {
        // 确保 ByteBuf 的引用计数不为 0 
        ensureAccessible();
        return new UnpooledDuplicatedByteBuf(this);
    }
```

但和  `slice()` 不同的是， `duplicate()` 是完全复刻了原生 ByteBuf，复刻出来的视图 ByteBuf 虽然与原生 ByteBuf 都有各自独立的  readerIndex，writerIndex，capacity，maxCapacity。但他们的值都是相同的。duplicate 视图  ByteBuf 也是和原生 ByteBuf 共用同一块 Native Memory 。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xF3X4pKzYhWUCnNm8Z6TdthSmDibrZU4XoBKVExNqwyAJibIpVje8SGVZA/640?wx_fmt=other&from=appmsg#imgIndex=23)

image.png

```
public class DuplicatedByteBuf extends AbstractDerivedByteBuf {
    // 原生 ByteBuf
    private final ByteBuf buffer;

    public DuplicatedByteBuf(ByteBuf buffer) {
        this(buffer, buffer.readerIndex(), buffer.writerIndex());
    }

    DuplicatedByteBuf(ByteBuf buffer, int readerIndex, int writerIndex) {
        // 初始化视图 ByteBuf 的 maxCapacity 与原生的相同
        super(buffer.maxCapacity());
        // 原生 ByteBuf
        this.buffer = buffer;
        // 视图 ByteBuf 的 readerIndex ， writerIndex 也与原生相同
        setIndex(readerIndex, writerIndex);
        markReaderIndex();
        markWriterIndex();
    }

    @Override
    public int capacity() {
        // 视图 ByteBuf 的 capacity 也与原生相同
        return unwrap().capacity();
    }

}
```

Netty 同样也提供了对应的 `retainedDuplicate()` 方法，用于创建 duplicate 视图 ByteBuf  的同时增加原生 ByteBuf 的引用计数。视图 ByteBuf 与原生 ByteBuf 之间共用同一个引用计数。

```
   @Override
    public ByteBuf retainedDuplicate() {
        return duplicate().retain();
    }
```

上面介绍的两种视图 ByteBuf 可以理解为是对原生 ByteBuf 的一层浅拷贝，Netty 也提供了 `copy()` 方法来实现对原生 ByteBuf 的深拷贝，copy 出来的 ByteBuf 是原生  ByteBuf 的一个副本，两者底层依赖的 Native Memory 是不同的，各自都有独立的  readerIndex，writerIndex，capacity，maxCapacity 。

```
public abstract class AbstractByteBuf extends ByteBuf {
    @Override
    public ByteBuf copy() {
        // 从原生 ByteBuf 中的 readerIndex 开始，拷贝 readableBytes 个字节到新的 ByteBuf 中
        return copy(readerIndex, readableBytes());
    }
}
```

`copy()` 方法是对原生 ByteBuf 的 `[readerIndex , writerIndex)`这段数据范围内容进行拷贝。copy 出来的 ByteBuf，它的 readerIndex = 0 ， writerIndex = capacity = 原生 ByteBuf 的 `readableBytes()`。maxCapacity 与原生 maxCapacity 相同。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFUZxib7QjNpnagBJ0RUhkj5YIyFXuodFSXDAIf7pRQqorjmMSuFrmSJg/640?wx_fmt=other&from=appmsg#imgIndex=24)

image.png

```
public class UnpooledDirectByteBuf  {
  @Override
    public ByteBuf copy(int index, int length) {
        ensureAccessible();
        ByteBuffer src;
        try {
            // 将原生 ByteBuf 中 [index , index + lengh) 这段范围的数据拷贝到新的 ByteBuf 中
            src = (ByteBuffer) buffer.duplicate().clear().position(index).limit(index + length);
        } catch (IllegalArgumentException ignored) {
            throw new IndexOutOfBoundsException("Too many bytes to read - Need " + (index + length));
        }
        // 首先新申请一段 native memory , 新的 ByteBuf 初始容量为 length (真实容量)，最大容量与原生 ByteBuf 的 maxCapacity 相等
        // readerIndex = 0 , writerIndex = length
        return alloc().directBuffer(length, maxCapacity()).writeBytes(src);
    }
}
```

### 2.8 CompositeByteBuf 的零拷贝设计

这里的零拷贝并不是我们经常提到的那种 OS 层面上的零拷贝，而是 Netty 在用户态层面自己实现的避免内存拷贝的设计。比如在传统意义上，如果我们想要将多个独立的 ByteBuf  聚合成一个 ByteBuf 的时候，我们首先需要向 OS 申请一段更大的内存，然后依次将多个 ByteBuf 中的内容拷贝到这段新申请的内存上，最后在释放这些 ByteBuf 的内存。

这样一来就涉及到两个性能开销点，一个是我们需要向 OS 重新申请更大的内存，另一个是内存的拷贝。Netty 引入 CompositeByteBuf 的目的就是为了解决这两个问题。巧妙地利用原有 ByteBuf 所占的内存，在此基础之上，将它们组合成一个逻辑意义上的 CompositeByteBuf ，提供一个统一的逻辑视图。

CompositeByteBuf 其实也是一种视图 ByteBuf ，这一点和上小节中我们介绍的 SlicedByteBuf ， DuplicatedByteBuf 一样，它们本身并不会占用 Native Memory，底层数据的存储全部依赖于原生的 ByteBuf。

不同点在于，SlicedByteBuf，DuplicatedByteBuf 它们是在单一的原生 ByteBuf 基础之上创建出的视图 ByteBuf。而 CompositeByteBuf 是基于多个原生 ByteBuf 创建出的统一逻辑视图  ByteBuf。

CompositeByteBuf 对于我们用户来说和其他的普通 ByteBuf 没有任何区别，有自己独立的 readerIndex，writerIndex，capacity，maxCapacity，前面几个小节中介绍的各种 ByteBuf 的设计要素，在 CompositeByteBuf 身上也都会体现。

但从实现的角度来说，CompositeByteBuf 只是一个逻辑上的 ByteBuf，其本身并不会占用任何的 Native Memory ，对于 CompositeByteBuf 的任何操作，最终都需要转换到其内部具体的 ByteBuf 上。本小节我们就来深入到 CompositeByteBuf 的内部，来看一下 Netty 的巧妙设计。

#### 2.8.1 CompositeByteBuf 的总体架构

从总体设计上来讲，CompositeByteBuf 包含如下五个重要属性，其中最为核心的就是 components 数组，那些需要被聚合的原生 ByteBuf 会被 Netty 封装在 Component 类中，并统一组织在 components 数组中。后续针对 CompositeByteBuf 的所有操作都需要和这个数组打交道。

```
public class CompositeByteBuf extends AbstractReferenceCountedByteBuf implements Iterable<ByteBuf> {
    // 内部 ByteBuf 的分配器，用于后续扩容，copy , 合并等操作
    private final ByteBufAllocator alloc;
    // compositeDirectBuffer 还是 compositeHeapBuffer ?
    private final boolean direct;
    // 最大的 components 数组容量（16）
    private final int maxNumComponents;
    // 当前 CompositeByteBuf 中包含的 components 个数
    private int componentCount;
    // 存储 component 的数组
    private Component[] components; // resized when needed
}
```

maxNumComponents 表示 components 数组最大的容量，CompositeByteBuf 默认能够包含 Component 的最大个数为 16，如果超过这个数量的话，Netty 会将当前 CompositeByteBuf 中包含的所有 Components 重新合并成一个更大的 Component。

```
public abstract class AbstractByteBufAllocator implements ByteBufAllocator {
    static final int DEFAULT_MAX_COMPONENTS = 16;
}
```

componentCount 表示当前 CompositeByteBuf 中包含的 Component 个数。每当我们通过 `addComponent`  方法向 CompositeByteBuf 添加一个新的 ByteBuf 时，Netty 都会用一个新的 Component 实例来包装这个 ByteBuf，然后存放在  components 数组中，最后 componentCount 的个数加 1 。

CompositeByteBuf 与其底层聚合的真实 ByteBuf 架构设计关系，如下图所示：

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFzYD7Zq7FBo3VibSYk4mkj6YsFv9s98APffJ2fFVWBmUuazFpkmKnKcA/640?wx_fmt=other&from=appmsg#imgIndex=25)

image.png

而创建一个 CompositeByteBuf 的核心其实就是创建底层的 components 数组，后续添加到该 CompositeByteBuf 的所有原生 ByteBuf 都会被组织在这里。

```
   private CompositeByteBuf(ByteBufAllocator alloc, boolean direct, int maxNumComponents, int initSize) {
        // 设置 maxCapacity
        super(AbstractByteBufAllocator.DEFAULT_MAX_CAPACITY);

        this.alloc = ObjectUtil.checkNotNull(alloc, "alloc");
        this.direct = direct;
        this.maxNumComponents = maxNumComponents;
        // 初始 Component 数组的容量为 maxNumComponents
        components = newCompArray(initSize, maxNumComponents);
    }
```

这里的参数  `initSize` 表示的并不是 CompositeByteBuf 所包含的字节数，而是初始包装的原生 ByteBuf 个数，也就是初始  Component 的个数。components 数组的总体大小由参数 maxNumComponents 决定，但不能超过 16 。

```
   private static Component[] newCompArray(int initComponents, int maxNumComponents) {
        // MAX_COMPONENT
        int capacityGuess = Math.min(AbstractByteBufAllocator.DEFAULT_MAX_COMPONENTS, maxNumComponents);
        // 初始 Component 数组的容量为 maxNumComponents
        return new Component[Math.max(initComponents, capacityGuess)];
    }
```

现在我们只是清楚了 CompositeByteBuf 的一个基本骨架，那么接下来 Netty 如何根据这个基本的骨架将多个原生 ByteBuf 组装成一个逻辑上的统一视图 ByteBuf 呢 ？

也就是说我们依据 CompositeByteBuf 中的 readerIndex 以及 writerIndex 进行的读写操作逻辑如何转换到对应的底层原生 ByteBuf 之上呢 ？ 这个是整个设计的核心所在。

下面笔者就带着大家从外到内，从易到难地一一拆解 CompositeByteBuf 中的那些核心设计要素。从 CompositeByteBuf 的最外层来看，其实我们并不陌生，对于用户来说它就是一个普通的 ByteBuf，拥有自己独立的 readerIndex ，writerIndex 。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFZaj2RotYTHJw77AZaNbXicic5arSaB3681owcYOh4vXa1nkybB2Riastg/640?wx_fmt=other&from=appmsg#imgIndex=26)

image.png

但 CompositeByteBuf 中那些逻辑上看起来连续的字节，背后其实存储在不同的原生 ByteBuf 中。不同 ByteBuf 的内存之间其实是不连续的。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFPaTicNCcEx4ibZrlnPf2JXMsIGn8UTHiaxEH5QJKpbJd7Mziba115XFhDw/640?wx_fmt=other&from=appmsg#imgIndex=27)

image.png

那么现在问题的关键就是我们如何判断 CompositeByteBuf 中的某一段逻辑数据背后对应的究竟是哪一个真实的 ByteBuf，如果我们能够通过 CompositeByteBuf 的相关 Index , 找到这个 Index 背后对应的 ByteBuf，近而可以找到 ByteBuf 的 Index ，这样是不是就可以将 CompositeByteBuf 的逻辑操作转换成对真实内存的读写操作了。

CompositeByteBuf 到原生 ByteBuf 的转换关系，Netty 封装在 Component 类中，每一个被包装在 CompositeByteBuf 中的原生 ByteBuf 都对应一个 Component 实例。它们会按照顺序统一组织在 components 数组中。

```
    private static final class Component {
        // 原生 ByteBuf
        final ByteBuf srcBuf; 
        // CompositeByteBuf 的 index 加上 srcAdjustment 就得到了srcBuf 的相关 index
        int srcAdjustment; 
        // srcBuf 可能是一个被包装过的 ByteBuf，比如 SlicedByteBuf ， DuplicatedByteBuf
        // 被 srcBuf 包装的最底层的 ByteBuf 就存放在 buf 字段中
        final ByteBuf buf;      
        // CompositeByteBuf 的 index 加上 adjustment 就得到了 buf 的相关 index      
        int adjustment; 
 
        // 该 Component 在 CompositeByteBuf 视角中表示的数据范围 [offset , endOffset)
        int offset; 
        int endOffset;        
    }
```

一个 Component 在 CompositeByteBuf 的视角中所能表示的数据逻辑范围是 `[offset , endOffset)`。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFicjxbJicsiaEaMfXgFGGRwnuk4mwRc1ia0ic3xNibQjCgicy8c2cPiccbUNshA/640?wx_fmt=other&from=appmsg#imgIndex=28)

image.png

比如上图中第一个绿色的 ByteBuf , 它里边存储的数据组成了 CompositeByteBuf 中 `[0 , 4)` 这段逻辑数据范围。第二个黄色的 ByteBuf，它里边存储的数据组成了 CompositeByteBuf 中 `[4 , 8)` 这段逻辑数据范围。第三个蓝色的 ByteBuf，它里边存储的数据组成了 CompositeByteBuf 中 `[8 , 12)` 这段逻辑数据范围。 上一个 Component 的 endOffset 恰好是下一个 Component 的 offset 。

而这些真实存储数据的 ByteBuf 则存储在对应 Component 中的 srcBuf 字段中，当我们通过 CompositeByteBuf 的 readerIndex 或者 writerIndex 进行读写操作的时候，首先需要确定相关 index 所对应的 srcBuf，然后将 CompositeByteBuf 的 index 转换为 srcBuf 的 srcIndex，近而通过 srcIndex 对 srcBuf 进行读写。

这个 index 的转换就是通过 srcAdjustment 来进行的，比如，当前 CompositeByteBuf 的 readerIndex 为 5 ，它对应的是第二个黄色的 ByteBuf。而 ByteBuf 的 readerIndex 却是 1 。

所以第二个 Component 的 srcAdjustment 就是 -4 ， 这样我们读取 CompositeByteBuf 的时候，首先将它的 readerIndex 加上 srcAdjustment 就得到了 ByteBuf 的 readerIndex ，后面就是普通的 ByteBuf 读取操作了。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xF4Xc2nJP5cIZNVnJJOEr5LpncvCicsqiaHQFialYhlVmzsYQTeahHuKT0w/640?wx_fmt=other&from=appmsg#imgIndex=29)

image.png

在比如说，我们要对 CompositeByteBuf 进行写操作，当前的 writerIndex 为 10 ，对应的是第三个蓝色的 ByteBuf，它的 writerIndex 为 2 。

所以第三个 Component 的 srcAdjustment 就是 -8 ，CompositeByteBuf 的 writerIndex 加上 srcAdjustment 就得到了 ByteBuf 的 writerIndex，后续就是普通的 ByteBuf 写入操作。

```
       int srcIdx(int index) {
            // CompositeByteBuf 相关的 index 转换成 srcBuf 的相关 index
            return index + srcAdjustment;
        }
```

除了 srcBuf 之外，Component 实例中还有一个 buf 字段，这里大家可能会比较好奇，为什么设计了两个 ByteBuf 字段呢 ？Component 实例与 ByteBuf 不是一对一的关系吗 ？

srcBuf 是指我们通过 `addComponent` 方法添加到 CompositeByteBuf 中的原始 ByteBuf。而这个 srcBuf 可能是一个视图 ByteBuf，比如上一小节中介绍到的 SlicedByteBuf 和 DuplicatedByteBuf。srcBuf 还可能是一个被包装过的 ByteBuf，比如 WrappedByteBuf , SwappedByteBuf。

假如 srcBuf 是一个 SlicedByteBuf 的话，我们需要将它的原生 ByteBuf 拆解出来并保存在 Component 实例的 buf 字段中。事实上 Component 中的 buf 才是真正存储数据的地方。

```
abstract class AbstractUnpooledSlicedByteBuf {
    // 原生 ByteBuf
    private final ByteBuf buffer;
}
```

与 buf 对应的就是 adjustment ， 它用于将 CompositeByteBuf 的相关 index 转换成 buf 相关的 index ，假如我们在向一个 CompositeByteBuf 执行 read 操作，它的当前 readerIndex 是 5，而 buf 的 readerIndex 是 6 。

所以在读取操作之前，我们需要将 CompositeByteBuf 的 readerIndex 加上 adjustment 得到 buf 的 readerIndex，近而将读取操作转移到 buf 中。其实就和上小节中介绍的视图 ByteBuf 是一模一样的，在读写之前都需要修正相关的 index 。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFhDCNKricdmeYtwebiaAiaBun03JEYXcVicZicszpLclgD0OpiaJ1iaIViaEnFQ/640?wx_fmt=other&from=appmsg#imgIndex=30)

image.png

```
   @Override
    public byte getByte(int index) {
        // 通过 CompositeByteBuf 的 index , 找到数据所属的 component
        Component c = findComponent(index);
        // 首先通过 idx 转换为 buf 相关的 index
        // 将对 CompositeByteBuf 的读写操作转换为 buf 的读写操作
        return c.buf.getByte(c.idx(index));
    }

    int idx(int index) {
        // 将 CompositeByteBuf 的相关 index 转换为 buf 的相关 index
        return index + adjustment;
     }
```

那么我们如何根据指定的 CompositeByteBuf 的 index 来查找其对应的底层数据究竟存储在哪个 Component 中呢 ？

核心思想其实很简单，因为每个 Component 都会描述自己表示 CompositeByteBuf 中的哪一段数据范围 —— `[offset , endOffset)`。所有的 Components 都被有序的组织在 components 数组中。我们可以通过二分查找的方法来寻找这个 index 到底是落在了哪个 Component 表示的范围中。

这个查找的过程是在 `findComponent` 方法中实现的，Netty 会将最近一次访问到的  Component 缓存在 CompositeByteBuf 的 lastAccessed 字段中，每次进行查找的时候首先会判断 index 是否落在了 lastAccessed 所表示的数据范围内 —— `[ la.offset , la.endOffset)` 。

如果 index 恰好被缓存的 Component（lastAccessed）所包含，那么就直接返回 lastAccessed 。

```
    // 缓存最近一次查找到的 Component
    private Component lastAccessed;

    private Component findComponent(int offset) {
        Component la = lastAccessed;
        // 首先查找 offset 是否恰好落在 lastAccessed 的区间中
        if (la != null && offset >= la.offset && offset < la.endOffset) {
           return la;
        }
        // 在所有 Components 中进行二分查找
        return findIt(offset);
    }
```

如果 index 不巧没有命中缓存，那么就在整个 components 数组中进行二分查找 ：

```
    private Component findIt(int offset) {
        for (int low = 0, high = componentCount; low <= high;) {
            int mid = low + high >>> 1;
            Component c = components[mid];
            if (offset >= c.endOffset) {
                low = mid + 1;
            } else if (offset < c.offset) {
                high = mid - 1;
            } else {
                lastAccessed = c;
                return c;
            }
        }

        throw new Error("should not reach here");
    }
```

#### 2.8.2 CompositeByteBuf 的创建

好了，现在我们已经熟悉了 CompositeByteBuf 的总体架构，那么接下来我们就来看一下 Netty 是如何将多个 ByteBuf 逻辑聚合成一个 CompositeByteBuf 的。

```
public final class Unpooled {
   public static ByteBuf wrappedBuffer(ByteBuf... buffers) {
        return wrappedBuffer(buffers.length, buffers);
    }
}
```

CompositeByteBuf 的初始 maxNumComponents 为 buffers 数组的长度，如果我们只是传入一个 ByteBuf 的话，那么就无需创建 CompositeByteBuf，而是直接返回该 ByteBuf 的 slice 视图。

如果我们传入的是多个 ByteBuf 的话，则将这多个 ByteBuf 包装成 CompositeByteBuf 返回。

```
public final class Unpooled {
    public static ByteBuf wrappedBuffer(int maxNumComponents, ByteBuf... buffers) {
        switch (buffers.length) {
        case 0:
            break;
        case 1:
            ByteBuf buffer = buffers[0];
            if (buffer.isReadable()) {
                // 直接返回 buffer.slice() 视图
                return wrappedBuffer(buffer.order(BIG_ENDIAN));
            } else {
                buffer.release();
            }
            break;
        default:
            for (int i = 0; i < buffers.length; i++) {
                ByteBuf buf = buffers[i];
                if (buf.isReadable()) {
                    // 从第一个可读的 ByteBuf —— buffers[i] 开始创建 CompositeByteBuf
                    return new CompositeByteBuf(ALLOC, false, maxNumComponents, buffers, i);
                }
                // buf 不可读则 release
                buf.release();
            }
            break;
        }
        return EMPTY_BUFFER;
    }
}
```

在进入 CompositeByteBuf 的创建流程之后，首先是创建出一个空的 CompositeByteBuf，也就是先把 CompositeByteBuf 的骨架搭建起来，这时它的 initSize 为 `buffers.length - offset` 。

注意 initSize 表示的并不是 CompositeByteBuf 初始包含的字节个数，而是表示初始 Component 的个数。offset 则表示从 buffers 数组中的哪一个索引开始创建 CompositeByteBuf，就是上面 CompositeByteBuf 构造函数中最后一个参数 i 。

随后通过 `addComponents0` 方法为 buffers 数组中的每一个 ByteBuf 创建初始化 Component 实例，并将他们有序的添加到 CompositeByteBuf 的 components 数组中。

但这时 Component 实例的个数可能已经超过 maxNumComponents 限制的个数，那么接下来就会在 `consolidateIfNeeded()` 方法中将当前 CompositeByteBuf 中的所有 Components 合并成一个更大的 Component。CompositeByteBuf 中的 components 数组长度是不可以超过 maxNumComponents 限制的，如果超过就需要在这里合并。

最后设置当前 CompositeByteBuf 的 readerIndex 和 writerIndex，在初始状态下 CompositeByteBuf 的 readerIndex 会被设置为 0 ，writerIndex 会被设置为最后一个 Component 的 endOffset 。

```
    CompositeByteBuf(ByteBufAllocator alloc, boolean direct, int maxNumComponents,
            ByteBuf[] buffers, int offset) {
        // 先初始化一个空的 CompositeByteBuf
        // initSize 为 buffers.length - offset
        this(alloc, direct, maxNumComponents, buffers.length - offset);
        // 为所有的 buffers 创建  Component 实例，并添加到 components 数组中
        addComponents0(false, 0, buffers, offset);
        // 如果当前 component 的个数已经超过了 maxNumComponents，则将所有 component 合并成一个
        consolidateIfNeeded();
        // 设置 CompositeByteBuf 的 readerIndex = 0
        // writerIndex 为最后一个 component 的 endOffset
        setIndex0(0, capacity());
    }
```

#### 2.8.3 shiftComps 为新的 ByteBuf 腾挪空间

在整个 CompositeByteBuf 的构造过程中，最核心也是最复杂的步骤其实就是 `addComponents0` 方法，将多个 ByteBuf  有序的添加到 CompositeByteBuf 的 components 数组中看似简单，其实还有很多种复杂的情况需要考虑。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xF1UFx98F5ibRGNicLU4WR9vZnHtIibx7IsPlfU14prBwz2iavzDGTksLHibw/640?wx_fmt=other&from=appmsg#imgIndex=31)

image.png

复杂之处在于这些 ByteBuf 需要插在 components 数组的哪个位置上 ？ 比较简单直观的情况是我们直接在 components 数组的末尾插入，也就是说要插入的位置索引 cIndex 等于 componentCount。这里分为两种情况：

1.  `cIndex = componentCount = 0` ，这种情况表示我们在向一个空的 CompositeByteBuf 插入 ByteBufs , 很简单，直接插入即可。
    
2.  `cIndex = componentCount > 0` ， 这种情况表示我们再向一个非空的 CompositeByteBuf 插入 ByteBufs，正如上图所示。同样也很简单，直接在 componentCount 的位置处插入即可。
    

稍微复杂一点的情况是我们在 components 数组的中间位置进行插入而不是在末尾，也就是 `cIndex < componentCount` 的情况。如下如图所示，假设我们现在需要在 `cIndex = 3` 的位置处插入两个 ByteBuf 进来，但现在 components\[3\] 以及 components\[4\] 的位置已经被占用了。所以我们需要将这两个位置上的原有 component 向后移动两个位置，将 components\[3\] 和 components\[4\] 的位置腾出来。

```
// i = 3 , count = 2 , size = 5
System.arraycopy(components, i, components, i + count, size - i);
```

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xF8DXEM6czlHQicj1YD9PXn83B1a2Gb44fM7VFCqc3uunep1HxVbNKIfg/640?wx_fmt=other&from=appmsg#imgIndex=32)

image.png

在复杂一点的情况就是 components 数组需要扩容，当一个 CompositeByteBuf 刚刚被初始化出来的时候，它的 components 数组长度等于 maxNumComponents。

如果当前 components 数组中包含的 component 个数 —— componentCount 加上本次需要添加的 ByteBuf 个数 —— count 已经超过了 maxNumComponents 的时候，就需要对 components 数组进行扩容。

```
        // 初始为 0，当前 CompositeByteBuf 中包含的 component 个数
        final int size = componentCount, 
        // 本次 addComponents0 操作之后，新的 component 个数
        newSize = size + count;
       
        // newSize 超过了 maxNumComponents 则对 components 数组进行扩容
        if (newSize > components.length) {
            ....... 扩容 ....

            // 扩容后的新数组
            components = newArr;
        }
```

扩容之后的 components 数组长度是在 newSize 与原来长度的 `3 / 2` 之间取一个最大值。

```
int newArrSize = Math.max(size + (size >> 1), newSize);
```

如果我们原来恰好是希望在 components 数组的末尾插入，也就是 `cIndex = componentCount` 的情况，那么就需要通过 `Arrays.copyOf` 首先申请一段长度为 newArrSize 的数组，然后将原来的 components 数组中的内容原样拷贝过去。

```
newArr = Arrays.copyOf(components, newArrSize, Component[].class);
```

这样新的 components 数组就有位置可以容纳本次需要加入的 ByteBuf 了。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFkxGibQcXGraJAvC6QvOrEZTa5fjsTibW2JZcnghS29utiaU4iaLvaQR8GA/640?wx_fmt=other&from=appmsg#imgIndex=33)

image.png

如果我们希望在原来 components 数组的中间插入，也就是 `cIndex < componentCount` 的情况，如下图所示：

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFA8RtHd4fdCKs8ssOhCFPMlgDFA8255mt64ee9pMywkibibeNuvL8hclA/640?wx_fmt=other&from=appmsg#imgIndex=34)

image.png

这种情况在扩容的时候就不能原样拷贝原 components 数组了，而是首先通过 `System.arraycopy` 将 `[0 , cIndex)` 这段范围的内容拷贝过去，在将 `[cIndex , componentCount)` 这段范围的内容拷贝到新数组的 `cIndex + count` 位置处。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFXMEkZ2ibqrGhia5jo5qjxicLofZTrxmDCzs5UPicVh3frc7jwEvrG4oYyg/640?wx_fmt=other&from=appmsg#imgIndex=35)

image.png

这样一来，就在新 components 数组的 cIndex 索引处，空出了两个位置出来用来添加本次这两个 ByteBuf。最后更新 componentCount 的值。以上腾挪空间的逻辑封装在 shiftComps 方法中：

```
    private void shiftComps(int i, int count) {
        // 初始为 0，当前 CompositeByteBuf 中包含的 component 个数
        final int size = componentCount, 
        // 本次 addComponents0 操作之后，新的 component 个数
        newSize = size + count;
       
        // newSize 超过了 max components（16） 则对 components 数组进行扩容
        if (newSize > components.length) {
            // grow the array，扩容到原来的 3 / 2
            int newArrSize = Math.max(size + (size >> 1), newSize);
            Component[] newArr;
            if (i == size) {
                // 在 Component[] 数组的末尾进行插入
                // 初始状态 i = size = 0
                // size - 1 是 Component[] 数组的最后一个元素，指定的 i 恰好越界
                // 原来 Component[] 数组中的内容全部拷贝到 newArr 中
                newArr = Arrays.copyOf(components, newArrSize, Component[].class);
            } else {
                // 在 Component[] 数组的中间进行插入
                newArr = new Component[newArrSize];
                if (i > 0) {
                    // [0 , i) 之间的内容拷贝到 newArr 中
                    System.arraycopy(components, 0, newArr, 0, i);
                }
                if (i < size) {
                    // 将剩下的 [i , size) 内容从 newArr 的 i + count 位置处开始拷贝。
                    // 因为需要将原来的 [ i , i+count ） 这些位置让出来，添加本次新的 components，
                    System.arraycopy(components, i, newArr, i + count, size - i);
                }
            }
            // 扩容后的新数组
            components = newArr;
        } else if (i < size) {
            // i < size 本次操作要覆盖原来的 [ i , i+count ） 之间的位置，所以这里需要将原来位置上的 component 向后移动
            System.arraycopy(components, i, components, i + count, size - i);
        }
        // 更新 componentCount
        componentCount = newSize;
    }
```

#### 2.8.4 Component 如何封装 ByteBuf

经过上一小节 shiftComps 方法的辗转腾挪之后，现在 CompositeByteBuf 中的 components 数组终于有位置可以容纳本次需要添加的 ByteBuf 了。接下来就需要为每一个 ByteBuf 创建初始化一个 Component 实例，最后将这些 Component 实例放到 components 数组对应的位置上。

```
    private static final class Component {
        // 原生 ByteBuf
        final ByteBuf srcBuf; 
        // CompositeByteBuf 的 index 加上 srcAdjustment 就得到了srcBuf 的相关 index
        int srcAdjustment; 
        // srcBuf 可能是一个被包装过的 ByteBuf，比如 SlicedByteBuf ， DuplicatedByteBuf
        // 被 srcBuf 包装的最底层的 ByteBuf 就存放在 buf 字段中
        final ByteBuf buf;      
        // CompositeByteBuf 的 index 加上 adjustment 就得到了 buf 的相关 index      
        int adjustment; 
 
        // 该 Component 在 CompositeByteBuf 视角中表示的数据范围 [offset , endOffset)
        int offset; 
        int endOffset;        
    }
```

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFhDCNKricdmeYtwebiaAiaBun03JEYXcVicZicszpLclgD0OpiaJ1iaIViaEnFQ/640?wx_fmt=other&from=appmsg#imgIndex=36)

image.png

我们首先需要初始化 Component 实例的 offset ， endOffset 属性，前面我们已经介绍了，一个 Component 在 CompositeByteBuf 的视角中所能表示的数据逻辑范围是 `[offset , endOffset)`。在 components 数组中，一般前一个 Component 的 endOffset 往往是后一个 Component 的 offset。

如果我们期望从 components 数组的第一个位置处开始插入（cIndex = 0），那么第一个 Component 的 offset 自然是 0 。

如果 cIndex > 0 , 那么我们就需要找到它上一个 Component —— components\[cIndex - 1\] ， 上一个 Component 的 endOffset 恰好就是当前 Component 的 offset。

然后通过 `newComponent` 方法利用 ByteBuf 相关属性以及 offset 来初始化 Component 实例。随后将创建出来的 Component 实例放置在对应的位置上 —— components\[cIndex\] 。

```
           // 获取当前正在插入 Component 的 offset
           int nextOffset = cIndex > 0 ? components[cIndex - 1].endOffset : 0;
            for (ci = cIndex; arrOffset < len; arrOffset++, ci++) {
                // 待插入 ByteBuf
                ByteBuf b = buffers[arrOffset];
                if (b == null) {
                    break;
                }
                // 将 ByteBuf 封装在 Component 中
                Component c = newComponent(ensureAccessible(b), nextOffset);
                components[ci] = c;
                // 下一个 Component 的 Offset 是上一个 Component 的 endOffset
                nextOffset = c.endOffset;
            }
```

假设现在有一个空的 CompositeByteBuf，我们需要将一个数据范围为 `[1 , 4]` , readerIndex = 1 的 srcBuf ， 插入到 CompositeByteBuf 的 components 数组中。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFoOIOfWV9gOADW5iacPVsiaibQ4snlMR8SCDZH3JZibibl4gTNicJ46pKhBWg/640?wx_fmt=other&from=appmsg#imgIndex=37)

image.png

但是如果该 srcBuf 是一个视图 ByteBuf 的话，比如：SlicedByteBuf ， DuplicatedByteBuf。或者是一个被包装过的 ByteBuf ，比如：WrappedByteBuf ， SwappedByteBuf。

那么我们就需要对 srcBuf 不断的执行 `unwrap()`, 将其最底层的原生 ByteBuf 提取出来，如上图所示，原生 buf 的数据范围为 `[4 , 7]` , srcBuf 与 buf 之间相关 index 的偏移 adjustment 等于 3  , 原生 buf 的 readerIndex = 4 。

最后我们会根据 srcBuf ， srcIndex（srcBuf 的 readerIndex），原生 buf ，unwrappedIndex（buf 的 readerIndex），offset ， len （srcBuf 中的可读字节数）来初始化 Component 实例。

```
    private Component newComponent(final ByteBuf buf, final int offset) {
        // srcBuf 的 readerIndex = 1
        final int srcIndex = buf.readerIndex();
        // srcBuf 中的可读字节数 = 4
        final int len = buf.readableBytes();

        // srcBuf 可能是一个被包装过的 ByteBuf，比如 SlicedByteBuf，DuplicatedByteBuf
        // 获取 srcBuf 底层的原生 ByteBuf
        ByteBuf unwrapped = buf;
        // 原生 ByteBuf 的 readerIndex
        int unwrappedIndex = srcIndex;
        while (unwrapped instanceof WrappedByteBuf || unwrapped instanceof SwappedByteBuf) {
            unwrapped = unwrapped.unwrap();
        }

        // unwrap if already sliced
        if (unwrapped instanceof AbstractUnpooledSlicedByteBuf) {
            // 获取视图 ByteBuf  相对于 原生 ByteBuf 的相关 index 偏移
            // adjustment = 3
            // unwrappedIndex = srcIndex + adjustment = 4
            unwrappedIndex += ((AbstractUnpooledSlicedByteBuf) unwrapped).idx(0);
            // 获取原生 ByteBuf
            unwrapped = unwrapped.unwrap();
        } else if (unwrapped instanceof PooledSlicedByteBuf) {
            unwrappedIndex += ((PooledSlicedByteBuf) unwrapped).adjustment;
            unwrapped = unwrapped.unwrap();
        } else if (unwrapped instanceof DuplicatedByteBuf || unwrapped instanceof PooledDuplicatedByteBuf) {
            unwrapped = unwrapped.unwrap();
        }

        return new Component(buf.order(ByteOrder.BIG_ENDIAN), srcIndex,
                unwrapped.order(ByteOrder.BIG_ENDIAN), unwrappedIndex, offset, len, slice);
    }
```

由于当前的 CompositeByteBuf 还是空的，里面没有包含任何逻辑数据，当长度为 4 的 srcBuf 加入之后，CompositeByteBuf 就产生了 `[0 , 3]` 这段逻辑数据范围，所以 srcBuf 所属 Component 的 offset = 0 , endOffset = 4 ，srcAdjustment = 1 ，adjustment = 4。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFWiaews0CdECtlr4QC1JcEPTJAhiaEscjLz1cuT9BTl8rdLqRKnA0ewSA/640?wx_fmt=other&from=appmsg#imgIndex=38)

image.png

```
        Component(ByteBuf srcBuf, int srcOffset, ByteBuf buf, int bufOffset,
                int offset, int len, ByteBuf slice) {
            this.srcBuf = srcBuf;
            // 用于将 CompositeByteBuf 的 index 转换为 srcBuf 的index
            // 1 - 0 = 1
            this.srcAdjustment = srcOffset - offset;
            this.buf = buf;
            // 用于将 CompositeByteBuf 的 index 转换为 buf 的index
            // 4 - 0 = 4
            this.adjustment = bufOffset - offset;
            // CompositeByteBuf [offset , endOffset) 这段范围的字节存储在该 Component 中
            //  0 
            this.offset = offset;
            // 下一个 Component 的 offset
            // 4
            this.endOffset = offset + len;
        }
```

当我们继续初始化下一个 Component 的时候，它的 Offset 其实就是这个 Component 的 endOffset 。后面的流程都是一样的了。

#### 2.8.5 addComponents0

在我们清楚了以上背景知识之后，在看 addComponents0 方法的逻辑就很清晰了：

```
    private CompositeByteBuf addComponents0(boolean increaseWriterIndex,
            final int cIndex, ByteBuf[] buffers, int arrOffset) {
        // buffers 数组长度
        final int len = buffers.length, 
        // 本次批量添加的 ByteBuf 个数
        count = len - arrOffset;
        // ci 表示从 components 数组的哪个索引位置处开始添加
        // 这里先给一个初始值，后续 shiftComps 完成之后还会重新设置
        int ci = Integer.MAX_VALUE;
        try {
            // cIndex >= 0 && cIndex <= componentCount
            checkComponentIndex(cIndex);
            // 为新添加进来的 ByteBuf 腾挪位置，以及增加 componentCount 计数
            shiftComps(cIndex, count); // will increase componentCount
            // 获取当前正在插入 Component 的 offset
            int nextOffset = cIndex > 0 ? components[cIndex - 1].endOffset : 0;
            for (ci = cIndex; arrOffset < len; arrOffset++, ci++) {
                ByteBuf b = buffers[arrOffset];
                if (b == null) {
                    break;
                }
                // 将 ByteBuf 封装在 Component 中
                Component c = newComponent(ensureAccessible(b), nextOffset);
                components[ci] = c;
                // 下一个 Component 的 Offset 是上一个 Component 的 endOffset
                nextOffset = c.endOffset;
            }
            return this;
        } finally {
            // ci is now the index following the last successfully added component
            // ci = componentCount 说明是一直按照顺序向后追加 component
            // ci < componentCount 表示在 components 数组的中间插入新的 component
            if (ci < componentCount) {
                // 如果上面 for 循环完整的走完，ci = cIndex + count
                if (ci < cIndex + count) {
                    // 上面 for 循环中有 break 的情况出现或者有异常发生
                    // ci < componentCount ，在上面的 shiftComps 中将会涉及到 component 移动，因为要腾出位置
                    // 如果发生异常，则将后面没有加入 components 数组的 component 位置删除掉
                    // [ci, cIndex + count) 这段位置要删除，因为在 ci-1 处已经发生异常，重新调整 components 数组
                    removeCompRange(ci, cIndex + count);
                    for (; arrOffset < len; ++arrOffset) {
                        ReferenceCountUtil.safeRelease(buffers[arrOffset]);
                    }
                }
                // （在中间插入的情况下）需要调整 ci 到 size -1 之间的 component 的相关 Offset
                updateComponentOffsets(ci); // only need to do this here for components after the added ones
            }
            if (increaseWriterIndex && ci > cIndex && ci <= componentCount) {
                // 本次添加的最后一个 components[ci - 1]
                // 本次添加的第一个 components[cIndex]
                // 最后一个 endOffset 减去第一个的 offset 就是本次添加的字节个数
                writerIndex += components[ci - 1].endOffset - components[cIndex].offset;
            }
        }
    }
```

这里我们重点介绍下 `finally {}` 代码块中的逻辑。首先 addComponents0 方法中的核心逻辑是先通过 shiftComps 方法为接下来新创建出来的 Component 腾挪位置，因为我们有可能是在原有 components 数组的中间位置插入。

然后会在一个 `for ()` 循环中不停的将新创建的 Component 放置到 `components[ci]` 位置上。

当跳出 for 循环进入 finally 代码块的时候，ci 的值恰恰就是最后一个成功加入 components 数组的 Component 下一个位置，如下图所示，假设 components\[0\] ， components\[1\] ，components\[2\]  是我们刚刚在 for 循环中插入的新值，那么 for 循环结束之后，ci 的值就是 3 。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFPPRE6bEibmQCibGSibbM1icKxY1te9EmMMRZicEBpU79X5l4xVUUsPCvqrA/640?wx_fmt=other&from=appmsg#imgIndex=39)

image.png

如果 `ci = componentCount` 这恰恰说明我们一直是在 components 数组的末尾进行插入，这种情况下各个 Component 实例中的 \[offset , endOffset) 都是连续的不需要做任何调整。

但如果 `ci < componentCount` 这就说明了我们是在原来 components 数组的中间位置处开始插入，下图中的 components\[3\] ，components\[4\] 是插入位置，当插入完成之后 ci 的值为 5。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFsWLeSmtQgskFLkbG8xoRjX9EfLicY98k7jlicH7f4r7kJ4fRLEYZhAlA/640?wx_fmt=other&from=appmsg#imgIndex=40)

image.png

这时候就需要重新调整 components\[5\]，components\[6\] 中的  `[offset , endOffset)` 范围，因为 shiftComps 方法只负责帮你腾挪位置，不负责重新调整 `[offset , endOffset)` 范围，当新的 Component 实例插入之后，原来彼此相邻的 Component 实例之间的 `[offset , endOffset)` 就不连续了，所以这里需要重新调整。

比如下图中所展示的情况，原来的 components 数组包含五个 Component 实例，分别在 0 - 4 位置，它们之间原本的是连续的 `[offset , endOffset)`。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xF8DXEM6czlHQicj1YD9PXn83B1a2Gb44fM7VFCqc3uunep1HxVbNKIfg/640?wx_fmt=other&from=appmsg#imgIndex=41)

image.png

现在我们要在位置 3 ，4 处插入两个新的 Component 实例，所以原来的  components\[3\] ，components\[4\] 需要移动到  components\[5\] ，components\[6\] 的位置上，但 shiftComps 只负责移动而不负责重新调整它们的 `[offset , endOffset)`。

当新的 Component 实例插入之后，components\[4\]，components\[5\] ，components\[6\] 之间的  `[offset , endOffset)` 就不连续了。所以需要通过 `updateComponentOffsets` 方法重新调整。

```
    private void updateComponentOffsets(int cIndex) {
        int size = componentCount;
        if (size <= cIndex) {
            return;
        }
        // 重新调整 components[5] ，components[6] 之间的 [offset , endOffset)
        int nextIndex = cIndex > 0 ? components[cIndex - 1].endOffset : 0;
        for (; cIndex < size; cIndex++) {
            Component c = components[cIndex];
            // 重新调整 Component 的 offset ， endOffset
            c.reposition(nextIndex);
            nextIndex = c.endOffset;
        }
    }

     void reposition(int newOffset) {
            int move = newOffset - offset;
            endOffset += move;
            srcAdjustment -= move;
            adjustment -= move;
            offset = newOffset;
      }
        
```

以上介绍的是正常情况下的逻辑，如果在执行 for 循环的过程中出现了 break 或者发生了异常，那么 ci 的值一定是小于 `cIndex + count` 的。什么意思呢 ？

比如我们要向一个 components 数组 `cIndex = 0` 的位置插入 `count = 5` 个 Component 实例，但是在插入第四个 Component 的时候，也就是在 components\[3\] 的位置处出现了 break 或者异常的情况，那么就会退出 for 循环来到这里的 finally 代码块。

此时的 ci 值为 3 ，cIndex + count 的值为 5，那么就说明出现了异常情况。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFg2DVssMyAW07Mr31dReEenMnakq5zeaVeQ8yoIc3d74cbAN1yBGFYA/640?wx_fmt=other&from=appmsg#imgIndex=42)

image.png

值得我们注意的是，components\[3\] 以及 components\[4\] 这两个位置是之前通过 shiftComps 方法腾挪出来的，由于异常情况的发生，这两个位置将不会放置任何 Component 实例。

这样一来 components 数组就出现了空洞，所以接下来我们还需要将 components\[5\] ， components\[6\] 位置上的 Component 实例重新移动回 components\[3\] 以及 components\[4\] 的位置上。

由于异常情况，那些 ByteBuf 数组中没有被添加进 CompositeByteBuf 的  ByteBuf 需要执行 release 。

#### 2.8.6 consolidateIfNeeded

到现在为止一个空的 CompositeByteBuf 就算被填充好了，但是这里有一个问题，就是 CompositeByteBuf 中所能包含的 Component 实例个数是受到 maxNumComponents 限制的。

我们回顾一下整个 addComponents 的过程，好像还没有一个地方对 Component 的个数做出限制，甚至在 shiftComps 方法中还会对 components 数组进行扩容。

那么这样一来，Component 的个数有很大可能会超过 maxNumComponents 的限制，如果当前 CompositeByteBuf 中包含的 component 个数已经超过了 maxNumComponents ，那么就需要在 `consolidate0` 方法中，将所有的 component 合并。

```
    private void consolidateIfNeeded() {
        int size = componentCount;
        // 如果当前 component 的个数已经超过了 maxNumComponents，则将所有 component 合并成一个
        if (size > maxNumComponents) {
            consolidate0(0, size);
        }
    }
```

在这里，Netty 会将当前 CompositeByteBuf 中包含的所有 Component 合并成一个更大的 Component。合并之后 ，CompositeByteBuf 中就只包含一个 Component 了。合并的核心逻辑如下：

1.  根据当前  CompositeByteBuf 的 capacity 重新申请一个更大的 ByteBuf ，该  ByteBuf 需要容纳下 CompositeByteBuf 所能表示的所有字节。
    
2.  将所有 Component 底层的 buf 中存储的内容全部转移到新的 ByteBuf 中，并释放原有 buf 的内存。
    
3.  删除 Component 数组中所有的 Component。
    
4.  根据新的 ByteBuf 创建一个新的 Component 实例，并放置在 components 数组的第一个位置上。
    

```
    private void consolidate0(int cIndex, int numComponents) {
        if (numComponents <= 1) {
            return;
        }
        // 将 [cIndex , endCIndex) 之间的 Components 合并成一个
        final int endCIndex = cIndex + numComponents;
        final int startOffset = cIndex != 0 ? components[cIndex].offset : 0;
        // 计算合并范围内 Components 的存储的字节总数
        final int capacity = components[endCIndex - 1].endOffset - startOffset;
        // 重新申请一个新的 ByteBuf
        final ByteBuf consolidated = allocBuffer(capacity);
        // 将合并范围内的 Components 中的数据全部转移到新的 ByteBuf 中
        for (int i = cIndex; i < endCIndex; i ++) {
            components[i].transferTo(consolidated);
        }
        lastAccessed = null;
        // 数据转移完成之后，将合并之前的这些 components 删除
        removeCompRange(cIndex + 1, endCIndex);
        // 将合并之后的新 Component 存储在 cIndex 位置处
        components[cIndex] = newComponent(consolidated, 0);
        if (cIndex != 0 || numComponents != componentCount) {
            // 如果 cIndex 不是从 0 开始的，那么就更新 newComponent 的相关 offset
            updateComponentOffsets(cIndex);
        }
    }
```

#### 2.8.7 CompositeByteBuf 的应用

当我们在传输层采用 TCP 协议进行数据传输的时候，经常会遇到半包或者粘包的问题，我们从 socket 中读取出来的 ByteBuf 很大可能还构不成一个完整的包，这样一来，我们就需要将每次从 socket 中读取出来的 ByteBuf 在用户态缓存累加起来。

当累加起来的 ByteBuf 达到一个完整的数据包之后，我们在从这个被缓存的 ByteBuf 中读取字节，然后进行解码，最后将解码出来的对象沿着 pipeline 向后传递。

```
public abstract class ByteToMessageDecoder extends ChannelInboundHandlerAdapter {
    // 缓存累加起来的 ByteBuf
    ByteBuf cumulation;
    // ByteBuf 的累加聚合器
    private Cumulator cumulator = MERGE_CUMULATOR;
    // 是否是第一次收包
    private boolean first;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            // 用于存储解码之后的对象
            CodecOutputList out = CodecOutputList.newInstance();
            try {
                // 第一次收包
                first = cumulation == null;
                // 将新进来的 (ByteBuf) msg 与之前缓存的 cumulation 聚合累加起来
                cumulation = cumulator.cumulate(ctx.alloc(),
                        first ? Unpooled.EMPTY_BUFFER : cumulation, (ByteBuf) msg);
                // 解码
                callDecode(ctx, cumulation, out);
            } catch (DecoderException e) {
                throw e;
            } catch (Exception e) {
                throw new DecoderException(e);
            } finally {
                    ........ 省略 ........
                    // 解码成功之后，就将解码出来的对象沿着 pipeline 向后传播
                    fireChannelRead(ctx, out, size); 
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }
}
```

Netty 为此专门定义了一个 Cumulator 接口，用于将每次从 socket 中读取到的 ByteBuf 聚合累积起来。参数 alloc 是一个 ByteBuf 分配器，用于在聚合的过程中如果涉及到扩容，合并等操作可以用它来申请内存。

参数 cumulation 就是之前缓存起来的 ByteBuf，当第一次收包的时候，这里的 cumulation 就是一个空的 ByteBuf —— Unpooled.EMPTY\_BUFFER 。

参数 in 则是本次刚刚从 socket 中读取出来的 ByteBuf，可能是一个半包，Cumulator 的作用就是将新读取出来的 ByteBuf （in），累加合并到之前缓存的 ByteBuf （cumulation）中。

```
    public interface Cumulator {
        ByteBuf cumulate(ByteBufAllocator alloc, ByteBuf cumulation, ByteBuf in);
    }
```

Netty 提供了 Cumulator 接口的两个实现，一个是 MERGE\_CUMULATOR ， 另一个是 COMPOSITE\_CUMULATOR 。

```
public abstract class ByteToMessageDecoder extends ChannelInboundHandlerAdapter {

    public static final Cumulator MERGE_CUMULATOR

    public static final Cumulator COMPOSITE_CUMULATOR
}
```

MERGE\_CUMULATOR 是 Netty 默认的 Cumulator ，也是传统意义上最为普遍的一种聚合 ByteBuf 的实现，它的核心思想是在聚合多个 ByteBuf 的时候，首先会申请一块更大的内存，然后将这些需要被聚合的 ByteBuf 中的内容全部拷贝到新的 ByteBuf 中。然后释放掉原来的 ByteBuf 。

效果就是将多个 ByteBuf 重新聚合成一个更大的 ByteBuf ，但这种方式涉及到内存申请以及内存拷贝的开销，优势就是内存都是连续的，读取速度快。

另外一种实现就是 COMPOSITE\_CUMULATOR ，也是本小节的主题，它的核心思想是将多个 ByteBuf 聚合到一个 CompositeByteBuf 中，不需要额外申请内存，更不需要内存的拷贝。

但由于 CompositeByteBuf 只是逻辑上的一个视图 ByteBuf，其底层依赖的内存还是原来的那些 ByteBuf，所以就导致了 CompositeByteBuf 中的内存不是连续的，在加上 CompositeByteBuf 的相关 index 设计的比较复杂，所以在读取速度方面可能会比 MERGE\_CUMULATOR 更慢一点，所以我们需要根据自己的场景来权衡考虑，灵活选择。

```
    public static final Cumulator COMPOSITE_CUMULATOR = new Cumulator() {
        @Override
        public ByteBuf cumulate(ByteBufAllocator alloc, ByteBuf cumulation, ByteBuf in) {
            if (!cumulation.isReadable()) {
                // 之前缓存的已经解码完毕，这里将它释放，并从 in 开始重新累加。
                cumulation.release();
                return in;
            }
            CompositeByteBuf composite = null;
            try {
                // cumulation 是一个 CompositeByteBuf，说明 cumulation 之前是一个被聚合过的 ByteBuf
                if (cumulation instanceof CompositeByteBuf && cumulation.refCnt() == 1) {
                    composite = (CompositeByteBuf) cumulation;
                    // 这里需要保证 CompositeByteBuf 的 writerIndex 与 capacity 相等
                    // 因为我们需要每次在 CompositeByteBuf 的末尾聚合添加新的 ByteBuf
                    if (composite.writerIndex() != composite.capacity()) {
                        composite.capacity(composite.writerIndex());
                    }
                } else {
                    // 如果 cumulation 不是 CompositeByteBuf，只是一个普通的 ByteBuf
                    // 说明 cumulation 之前还没有被聚合过，这里是第一次聚合，所以需要先创建一个空的 CompositeByteBuf
                    // 然后将 cumulation 添加到 CompositeByteBuf 中
                    composite = alloc.compositeBuffer(Integer.MAX_VALUE).addFlattenedComponents(true, cumulation);
                }
                // 将本次新接收到的 ByteBuf（in）添加累积到 CompositeByteBuf 中
                composite.addFlattenedComponents(true, in);
                in = null;
                return composite;
            } finally {
                 ........ 省略聚合失败的处理 ..........
            }
        }
    };
```

## 3\. Heap or Direct

在前面的几个小节中，我们讨论了很多 ByteBuf 的设计细节，接下来让我们跳出这些细节，重新站在全局的视角下来看一下 ByteBuf 的总体设计。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZFdNJKVEk1X07SFzmQ63xFDO9aZqF2Mw3JOmgJYRcyzVGlaXvjUGeQ4SgpZnplT5QaXfLSBQialhg/640?wx_fmt=other&from=appmsg#imgIndex=43)

image.png

在 ByteBuf 的整个设计体系中，Netty 从 ByteBuf 内存布局的角度上，将整个体系分为了 HeapByteBuf 和 DirectByteBuf 两个大类。Netty 提供了 `PlatformDependent.directBufferPreferred()` 方法来指定在默认情况下，是否偏向于分配 Direct Memory。

```
public final class PlatformDependent {
    // 是否偏向于分配 Direct Memory
    private static final boolean DIRECT_BUFFER_PREFERRED;

    public static boolean directBufferPreferred() {
        return DIRECT_BUFFER_PREFERRED;
    }
}
```

要想使得 DIRECT\_BUFFER\_PREFERRED 为 true ，必须同时满足以下两个条件：

1.  `-Dio.netty.noPreferDirect` 参数必须指定为 false（默认）。
    
2.  CLEANER 不为 NULL , 也就是需要 JDK 中包含有效的 CLEANER 机制。
    

```
 static {
        DIRECT_BUFFER_PREFERRED = CLEANER != NOOP
                                  && !SystemPropertyUtil.getBoolean("io.netty.noPreferDirect", false);
        if (logger.isDebugEnabled()) {
            logger.debug("-Dio.netty.noPreferDirect: {}", !DIRECT_BUFFER_PREFERRED);
        }
 }
```

如果是安卓平台，那么 CLEANER 直接就是 NOOP，不会做任何判断，默认情况下直接走 Heap Memory , 除非特殊指定要走 Direct Memory。

```
        if (!isAndroid()) {
            if (javaVersion() >= 9) {
                // 检查 sun.misc.Unsafe 类中是否包含有效的 invokeCleaner 方法
                CLEANER = CleanerJava9.isSupported() ? new CleanerJava9() : NOOP;
            } else {
                // 检查 java.nio.ByteBuffer 中是否包含了 cleaner 字段
                CLEANER = CleanerJava6.isSupported() ? new CleanerJava6() : NOOP;
            }
        } else {
            CLEANER = NOOP;
        }
```

如果是 JDK 9 以上的版本，Netty 会检查是否可以通过  `sun.misc.Unsafe` 的 `invokeCleaner` 方法正确执行 DirectBuffer 的 Cleaner，如果执行过程中发生异常，那么 CLEANER 就为 NOOP，Netty 在默认情况下就会走 Heap Memory。

```
public final class Unsafe {
    public void invokeCleaner(java.nio.ByteBuffer directBuffer) {
        if (!directBuffer.isDirect())
            throw new IllegalArgumentException("buffer is non-direct");

        theInternalUnsafe.invokeCleaner(directBuffer);
    }
}
```

如果是 JDK 9 以下的版本，Netty 就会通过反射的方式先去获取 DirectByteBuffer 的 cleaner 字段，如果 cleaner 为 null 或者在执行 clean 方法的过程中出现了异常，那么 CLEANER 就为 NOOP，Netty 在默认情况下就会走 Heap Memory。

```
class DirectByteBuffer extends MappedByteBuffer implements DirectBuffer
{
    private final Cleaner cleaner;

    DirectByteBuffer(int cap) {                   // package-private

        ...... 省略 .....   

        base = UNSAFE.allocateMemory(size);
        cleaner = Cleaner.create(this, new Deallocator(base, size, cap));
    }
}
```

如果 `PlatformDependent.directBufferPreferred()` 方法返回 true ,那么 ByteBufAllocator 接下来在分配内存的时候，默认情况下就会分配  directBuffer。

```
public final class UnpooledByteBufAllocator  extends AbstractByteBufAllocator {
    // ByteBuf 分配器
    public static final UnpooledByteBufAllocator DEFAULT =
            new UnpooledByteBufAllocator(PlatformDependent.directBufferPreferred());
}

public abstract class AbstractByteBufAllocator implements ByteBufAllocator {
    // 是否默认分配 directBuffer
    private final boolean directByDefault;

    protected AbstractByteBufAllocator(boolean preferDirect) {
        directByDefault = preferDirect && PlatformDependent.hasUnsafe();
    }

    @Override
    public ByteBuf buffer() {
        if (directByDefault) {
            return directBuffer();
        }
        return heapBuffer();
    }
}
```

一般情况下，JDK 都会包含有效的 CLEANER 机制，所以我们完全可以仅是通过 `-Dio.netty.noPreferDirect` （默认 false）来控制 Netty 默认情况下走  Direct Memory。

但如果是安卓平台，那么无论  `-Dio.netty.noPreferDirect`  如何设置，Netty 默认情况下都会走  Heap Memory 。

## 4\. Cleaner or NoCleaner

站在内存回收的角度，Netty 将 ByteBuf 分为了带有 Cleaner 的 DirectByteBuf 和没有 Cleaner 的 DirectByteBuf 两个大类。在之前的文章[《以 ZGC 为例，谈一谈 JVM 是如何实现 Reference 语义的》](https://mp.weixin.qq.com/s?__biz=Mzg2MzU3Mjc3Ng==&mid=2247489586&idx=1&sn=4306549c480f668458ab4df0d4b2ea47&scene=21#wechat_redirect) 中的第三小节，笔者详细的介绍过，JVM  如何利用 Cleaner 机制来回收 DirectByteBuffer 背后的 Native Memory 。

而 Cleaner 回收 DirectByteBuffer 的 Native Memory 需要依赖 GC 的发生，当一个 DirectByteBuffer 没有任何强引用或者软引用的时候，如果此时发生 GC , Cleaner 才会去回收 Native Memory。如果很久都没发生 GC ,那么这些 DirectByteBuffer 所引用的 Native Memory 将一直不会释放。

所以仅仅是依赖 Cleaner 来释放 Native Memory 是有一定延迟的，极端情况下，如果一直等不来 GC ,很有可能就会发生 OOM 。

而 Netty 的 ByteBuf 设计相当于是对 NIO ByteBuffer 的一种完善扩展，其底层其实都会依赖一个 JDK 的 ByteBuffer。比如，前面介绍的 UnpooledDirectByteBuf ， UnpooledUnsafeDirectByteBuf 其底层依赖的就是 JDK  DirectByteBuffer , 而这个 DirectByteBuffer 就是带有 Cleaner 的 ByteBuf 。

```
public class UnpooledDirectByteBuf extends AbstractReferenceCountedByteBuf {
    // 底层依赖的 JDK DirectByteBuffer
    ByteBuffer buffer;

    public UnpooledDirectByteBuf(ByteBufAllocator alloc, int initialCapacity, int maxCapacity) {
        // 创建 DirectByteBuffer
        setByteBuffer(allocateDirect(initialCapacity), false);
    }

   protected ByteBuffer allocateDirect(int initialCapacity) {
        return ByteBuffer.allocateDirect(initialCapacity);
    }
```

```
public class UnpooledUnsafeDirectByteBuf extends UnpooledDirectByteBuf {
    // 底层依赖的 JDK DirectByteBuffer 的内存地址
    long memoryAddress;


    public UnpooledUnsafeDirectByteBuf(ByteBufAllocator alloc, int initialCapacity, int maxCapacity) {
         // 调用父类 UnpooledDirectByteBuf 构建函数创建底层依赖的 JDK DirectByteBuffer 
        super(alloc, initialCapacity, maxCapacity);
    }

    @Override
    final void setByteBuffer(ByteBuffer buffer, boolean tryFree) {
        super.setByteBuffer(buffer, tryFree);
        // 获取 JDK DirectByteBuffer 的内存地址
        memoryAddress = PlatformDependent.directBufferAddress(buffer);
    }
```

在 JDK NIO  中，凡是通过 `ByteBuffer.allocateDirect` 方法申请到 DirectByteBuffer 都是带有 Cleaer 的。

```
public abstract class ByteBuffer {
  public static ByteBuffer allocateDirect(int capacity) {
        return new DirectByteBuffer(capacity);
    }
}

class DirectByteBuffer extends MappedByteBuffer implements DirectBuffer
{
    private final Cleaner cleaner;

    DirectByteBuffer(int cap) {                   // package-private

        ...... 省略 .....   
        // 通过该构造函数申请到的 Direct Memory 会受到 -XX:MaxDirectMemorySize 参数的限制
        Bits.reserveMemory(size, cap);   
        // 底层调用 malloc 申请内存
        base = UNSAFE.allocateMemory(size);

        ...... 省略 .....   
        // 创建 Cleaner
        cleaner = Cleaner.create(this, new Deallocator(base, size, cap));
    }
}
```

**而带有 Cleaner 的 DirectByteBuffer 背后所能引用的 Direct Memory 是受到 `-XX:MaxDirectMemorySize` JVM 参数限制的**。由于 UnpooledDirectByteBuf 以及 UnpooledUnsafeDirectByteBuf 都带有 Cleaner，所以当他们在系统中没有任何强引用或者软引用的时候，如果发生 GC , Cleaner 就会释放他们的 Direct Memory 。

由于 Cleaner 执行会依赖 GC , 而 GC 的发生往往不那么及时，会有一定的延时，所以 Netty 为了可以及时的释放  Direct Memory ，往往选择不依赖 JDK 的 Cleaner 机制，手动进行释放。所以就有了 NoCleaner 类型的 DirectByteBuf —— UnpooledUnsafeNoCleanerDirectByteBuf 。

```
class UnpooledUnsafeNoCleanerDirectByteBuf extends UnpooledUnsafeDirectByteBuf {

    @Override
    protected ByteBuffer allocateDirect(int initialCapacity) {
        // 创建没有 Cleaner 的 JDK DirectByteBuffer 
        return PlatformDependent.allocateDirectNoCleaner(initialCapacity);
    }

    @Override
    protected void freeDirect(ByteBuffer buffer) {
        // 既然没有了 Cleaner ， 所以 Netty 要手动进行释放
        PlatformDependent.freeDirectNoCleaner(buffer);
    }
}
```

UnpooledUnsafeNoCleanerDirectByteBuf 的底层同样也会依赖一个 JDK  DirectByteBuffer , 但和之前不同的是，这里的 DirectByteBuffer 是不带有 cleaner 的。

我们通过 JNI 来调用 `DirectByteBuffer(long addr, int cap)` 构造函数创建出来的 JDK  DirectByteBuffer 都是没有 cleaner 的。**但通过这种方式创建出来的 DirectByteBuffer 背后引用的 Native Memory 是不会受到 `-XX:MaxDirectMemorySize` JVM 参数限制的**。

```
class DirectByteBuffer {
    // Invoked only by JNI: NewDirectByteBuffer(void*, long)
    private DirectByteBuffer(long addr, int cap) {
        super(-1, 0, cap, cap, null);
        address = addr;
        // cleaner 为 null
        cleaner = null;
    }
}
```

既然没有了 cleaner ， 所以 Netty 就无法依赖 GC 来释放 Direct Memory 了，这就要求 Netty 必须手动调用 `freeDirect` 方法及时地释放 Direct Memory。

> 事实上，无论 Netty 中的 DirectByteBuf 有没有 Cleaner， Netty 都会选择手动的进行释放，目的就是为了避免 GC 的延迟 ， 从而及时的释放 Direct Memory。

那么 Netty 中的 DirectByteBuf 在什么情况下带有 Cleaner，又在什么情况下不带 Cleaner 呢 ？我们可以通过 `PlatformDependent.useDirectBufferNoCleaner` 方法的返回值进行判断：

```
public final class PlatformDependent {
    // Netty 的 DirectByteBuf 是否带有 Cleaner
    private static final boolean USE_DIRECT_BUFFER_NO_CLEANER;
    public static boolean useDirectBufferNoCleaner() {
        return USE_DIRECT_BUFFER_NO_CLEANER;
    }
}
```

*   USE\_DIRECT\_BUFFER\_NO\_CLEANER = TRUE 表示 Netty 创建出来的 DirectByteBuf 不带有 Cleaner 。  Direct Memory 的用量不会受到 JVM 参数 -`XX:MaxDirectMemorySize` 的限制。
    
*   USE\_DIRECT\_BUFFER\_NO\_CLEANER = FALSE 表示 Netty 创建出来的 DirectByteBuf 带有 Cleaner 。  Direct Memory 的用量会受到 JVM 参数 -`XX:MaxDirectMemorySize` 的限制。
    

我们可以通过 `-Dio.netty.maxDirectMemory` 来设置 USE\_DIRECT\_BUFFER\_NO\_CLEANER 的值，除此之外，该参数还可以指定在 Netty 层面上可以使用的最大 DirectMemory 用量。

`io.netty.maxDirectMemory = 0` 那么 USE\_DIRECT\_BUFFER\_NO\_CLEANER 就为 FALSE , 表示在 Netty 层面创建出来的 DirectByteBuf 都是带有 Cleaner 的，**这种情况下 Netty 并不会限制 maxDirectMemory 的用量，因为限制了也没用，具体能用多少 maxDirectMemory，还是由 JVM 参数 `-XX:MaxDirectMemorySize` 决定的**。

`io.netty.maxDirectMemory < 0` ，默认为 -1，也就是在默认情况下 USE\_DIRECT\_BUFFER\_NO\_CLEANER 为 TRUE , 创建出来的 DirectByteBuf 都是不带 Cleaner 的。由于在这种情况下 maxDirectMemory 的用量并不会受到 JVM 参数 `-XX:MaxDirectMemorySize` 的限制，所以在 Netty 层面上必须限制 maxDirectMemory 的用量，默认值就是  `-XX:MaxDirectMemorySize`  指定的值。

**这里需要特别注意的是**，Netty 层面对于 maxDirectMemory 的容量限制和 JVM 层面对于 maxDirectMemory 的容量限制是单独分别计算的，互不影响。因此站在 JVM 进程的角度来说，总体 maxDirectMemory 的用量是 `-XX:MaxDirectMemorySize` 的两倍。

`io.netty.maxDirectMemory > 0` 的情况和小于 0 的情况一样，唯一不同的是 Netty 层面的 maxDirectMemory 用量是专门由  `-Dio.netty.maxDirectMemory` 参数指定，仍然独立于 JVM 层面的 maxDirectMemory 限制之外单独计算。

**所以从这个层面来说，Netty 设计 NoCleaner 类型的 DirectByteBuf 的另外一个目的就是为了突破 JVM 对于 maxDirectMemory 用量的限制**。

```
public final class PlatformDependent {
    // Netty 层面  Direct Memory 的用量统计
    // 为 NULL 表示在 Netty 层面不进行特殊限制，完全由 JVM 进行限制 Direct Memory 的用量
    private static final AtomicLong DIRECT_MEMORY_COUNTER;
    // Netty 层面 Direct Memory 的最大用量
    private static final long DIRECT_MEMORY_LIMIT;
    // JVM 指定的 -XX:MaxDirectMemorySize 最大堆外内存
    private static final long MAX_DIRECT_MEMORY = maxDirectMemory0();

    static {
        long maxDirectMemory = SystemPropertyUtil.getLong("io.netty.maxDirectMemory", -1);

        if (maxDirectMemory == 0 || !hasUnsafe() || !PlatformDependent0.hasDirectBufferNoCleanerConstructor()) {
            // maxDirectMemory = 0 表示后续创建的 DirectBuffer 是带有 Cleaner 的，Netty 自己不会强制限定 maxDirectMemory 的用量，完全交给 JDK 的 maxDirectMemory 来限制
            // 因为 Netty 限制了也没用，其底层依然依赖的是 JDK  DirectBuffer（Cleaner），JDK 会限制 maxDirectMemory 的用量
            // 在没有 Unsafe 的情况下，那么就必须使用 Cleaner，因为如果不使用 Cleaner 的话，又没有 Unsafe，我们就无法释放 Native Memory 了
            // 如果 JDK 本身不包含创建 NoCleaner DirectBuffer 的构造函数 —— DirectByteBuffer(long, int)，那么自然只能使用 Cleaner
            USE_DIRECT_BUFFER_NO_CLEANER = false;
            // Netty 自身不会统计 Direct Memory 的用量，完全交给 JDK 来统计
            DIRECT_MEMORY_COUNTER = null;
        } else {
            USE_DIRECT_BUFFER_NO_CLEANER = true;
            if (maxDirectMemory < 0) {
                // maxDirectMemory < 0 (默认 -1) 后续创建 NoCleaner DirectBuffer
                // Netty 层面会单独限制 maxDirectMemory 用量，maxDirectMemory 的值与 -XX:MaxDirectMemorySize 的值相同
                // 因为 JDK 不会统计和限制 NoCleaner DirectBuffer 的用量
                // 注意，这里 Netty 的 maxDirectMemory 和 JDK 的 maxDirectMemory 是分别单独统计的
                // 在 JVM 进程的角度来说，整体 maxDirectMemory 的用量是 -XX:MaxDirectMemorySize 的两倍（Netty用的和 JDK 用的之和）
                maxDirectMemory = MAX_DIRECT_MEMORY;
                if (maxDirectMemory <= 0) {
                    DIRECT_MEMORY_COUNTER = null;
                } else {
                    // 统计 Netty DirectMemory 的用量
                    DIRECT_MEMORY_COUNTER = new AtomicLong();
                }
            } else {
                // maxDirectMemory > 0 后续创建 NoCleaner DirectBuffer,Netty 层面的 maxDirectMemory 就是 io.netty.maxDirectMemory 指定的值
                DIRECT_MEMORY_COUNTER = new AtomicLong();
            }
        }
        logger.debug("-Dio.netty.maxDirectMemory: {} bytes", maxDirectMemory);
        DIRECT_MEMORY_LIMIT = maxDirectMemory >= 1 ? maxDirectMemory : MAX_DIRECT_MEMORY;
    }  
}
```

当 Netty 层面的 direct memory 用量超过了 `-Dio.netty.maxDirectMemory` 参数指定的值时，那么就会抛出 `OutOfDirectMemoryError` ，分配 DirectByteBuf 将会失败。

```
    private static void incrementMemoryCounter(int capacity) {
        if (DIRECT_MEMORY_COUNTER != null) {
            long newUsedMemory = DIRECT_MEMORY_COUNTER.addAndGet(capacity);
            if (newUsedMemory > DIRECT_MEMORY_LIMIT) {
                DIRECT_MEMORY_COUNTER.addAndGet(-capacity);
                throw new OutOfDirectMemoryError("failed to allocate " + capacity
                        + " byte(s) of direct memory (used: " + (newUsedMemory - capacity)
                        + ", max: " + DIRECT_MEMORY_LIMIT + ')');
            }
        }
    }
```

## 5\. Unsafe or NoUnsafe

站在内存访问方式的角度上来说 ， Netty 又会将 ByteBuf 分为了 Unsafe 和 NoUnsafe 两个大类，其中 NoUnsafe 的内存访问方式是依赖底层的 JDK ByteBuffer，对于 Netty ByteBuf 的任何操作最终都是会代理给底层 JDK 的 ByteBuffer。

```
public class UnpooledDirectByteBuf extends AbstractReferenceCountedByteBuf {
    // 底层依赖的 JDK DirectByteBuffer
    ByteBuffer buffer;

   @Override
    protected byte _getByte(int index) {
        return buffer.get(index);
    }

    @Override
    protected void _setByte(int index, int value) {
        buffer.put(index, (byte) value);
    }
}
```

而 Unsafe 的内存访问方式则是通过 `sun.misc.Unsafe` 类中提供的众多 low-level direct buffer access API 来对内存地址直接进行访问，由于是脱离 JVM 相关规范直接对内存地址进行访问，所以我们在调用 Unsafe 相关方法的时候需要考虑 JVM 以及 OS 的各种细节，一不小心就会踩坑出错，所以它是一种不安全的访问方式，但是足够灵活，高效。

```
public class UnpooledUnsafeDirectByteBuf extends UnpooledDirectByteBuf {
    // 底层依赖的 JDK DirectByteBuffer 的内存地址
    long memoryAddress;

    @Override
    protected byte _getByte(int index) {
        return UnsafeByteBufUtil.getByte(addr(index));
    }

   final long addr(int index) {
        // 直接通过内存地址进行访问
        return memoryAddress + index;
    }

    @Override
    protected void _setByte(int index, int value) {
        UnsafeByteBufUtil.setByte(addr(index), value);
    }

}
```

Netty 提供了 `-Dio.netty.noUnsafe` 参数来让我们决定是否采用 Unsafe 的内存访问方式，默认值是 false , 表示 Netty 默认开启 Unsafe 访问方式。

```
final class PlatformDependent0 {
    // 是否明确禁用 Unsafe，null 表示开启  Unsafe
    private static final Throwable EXPLICIT_NO_UNSAFE_CAUSE = explicitNoUnsafeCause0();

    private static Throwable explicitNoUnsafeCause0() {
        final boolean noUnsafe = SystemPropertyUtil.getBoolean("io.netty.noUnsafe", false);
        logger.debug("-Dio.netty.noUnsafe: {}", noUnsafe);

        if (noUnsafe) {
            logger.debug("sun.misc.Unsafe: unavailable (io.netty.noUnsafe)");
            return new UnsupportedOperationException("sun.misc.Unsafe: unavailable (io.netty.noUnsafe)");
        }

        return null;
    }
}
```

在确认开启了 Unsafe 方式之后，我们就需要近一步确认在当前 JRE 的 classpath 下是否存在 `sun.misc.Unsafe` 类，是否能通过反射的方式获取到 Unsafe 实例 —— theUnsafe 。

```
public final class Unsafe {
    // Unsafe 实例
    private static final Unsafe theUnsafe = new Unsafe();
}
```

```
final class PlatformDependent0 {
    // 验证 Unsafe 是否可用，null 表示 Unsafe 是可用状态
    private static final Throwable UNSAFE_UNAVAILABILITY_CAUSE;
    static {
           // 尝试通过反射的方式拿到 theUnsafe 实例
           final Object maybeUnsafe = AccessController.doPrivileged(new PrivilegedAction<Object>() {
                @Override
                public Object run() {
                    try {
                        final Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
                        Throwable cause = ReflectionUtil.trySetAccessible(unsafeField, false);
                        if (cause != null) {
                            return cause;
                        }
                        // the unsafe instance
                        return unsafeField.get(null);
                    } catch (NoSuchFieldException e) {
                        return e;
                    } catch (SecurityException e) {
                        return e;
                    } catch (IllegalAccessException e) {
                        return e;
                    } catch (NoClassDefFoundError e) {
                        // Also catch NoClassDefFoundError in case someone uses for example OSGI and it made
                        // Unsafe unloadable.
                        return e;
                    }
                }
            });
    }
}
```

在获取到 Unsafe 实例之后，我们还需要检查 Unsafe 中是否包含所有 Netty 用到的 low-level direct buffer access API ，确保这些 API 可以正常有效的运行。比如，是否包含 `copyMemory` 方法。

```
public final class Unsafe {
    @ForceInline
    public void copyMemory(Object srcBase, long srcOffset,
                           Object destBase, long destOffset,
                           long bytes) {
        theInternalUnsafe.copyMemory(srcBase, srcOffset, destBase, destOffset, bytes);
    }
}
```

是否可以通过 Unsafe 访问到 NIO Buffer 的 address 字段，因为后续我们需要直接操作内存地址。

```
public abstract class Buffer {
    // 内存地址
    long address;
}
```

在整个过程中如果发生任何异常，则表示在当前 classpath 下，不存在 `sun.misc.Unsafe` 类或者是由于不同版本 JDK 的设计，Unsafe 中没有 Netty 所需要的一些必要的访存 API 。这样一来我们就无法使用 Unsafe，内存的访问方式就需要回退到 NoUnsafe。

```
            if (maybeUnsafe instanceof Throwable) {
                unsafe = null;
                unsafeUnavailabilityCause = (Throwable) maybeUnsafe;
                logger.debug("sun.misc.Unsafe.theUnsafe: unavailable", (Throwable) maybeUnsafe);
            } else {
                unsafe = (Unsafe) maybeUnsafe;
                logger.debug("sun.misc.Unsafe.theUnsafe: available");
            }
            // 为 null 表示 Unsafe 可用
            UNSAFE_UNAVAILABILITY_CAUSE = unsafeUnavailabilityCause;
            UNSAFE = unsafe;
```

如果在整个过程中没有发生任何异常，我们获取到了一个有效的 UNSAFE 实例，那么后续将正式开启 Unsafe 的内存访问方式。

```
final class PlatformDependent0 {
    static boolean hasUnsafe() {
        return UNSAFE != null;
    }
}
```

完整的 `hasUnsafe()` 判断逻辑如下：

1.  如果当前平台是安卓或者 .NET ，则不能开启 Unsafe，因为这些平台并不包含 `sun.misc.Unsafe` 类。
    
2.  `-Dio.netty.noUnsafe` 参数需要设置为 false （默认开启）。
    

3.. 当前 classpath 下是否包含有效的 `sun.misc.Unsafe` 类。

4.  Unsafe 实例需要包含必要的访存 API 。
    

```
public final class PlatformDependent {
    private static final Throwable UNSAFE_UNAVAILABILITY_CAUSE = unsafeUnavailabilityCause0();

    public static boolean hasUnsafe() {
        return UNSAFE_UNAVAILABILITY_CAUSE == null;
    }
    private static Throwable unsafeUnavailabilityCause0() {
        if (isAndroid()) {
            logger.debug("sun.misc.Unsafe: unavailable (Android)");
            return new UnsupportedOperationException("sun.misc.Unsafe: unavailable (Android)");
        }

        if (isIkvmDotNet()) {
            logger.debug("sun.misc.Unsafe: unavailable (IKVM.NET)");
            return new UnsupportedOperationException("sun.misc.Unsafe: unavailable (IKVM.NET)");
        }

        Throwable cause = PlatformDependent0.getUnsafeUnavailabilityCause();
        if (cause != null) {
            return cause;
        }

        try {
            boolean hasUnsafe = PlatformDependent0.hasUnsafe();
            logger.debug("sun.misc.Unsafe: {}", hasUnsafe ? "available" : "unavailable");
            return hasUnsafe ? null : PlatformDependent0.getUnsafeUnavailabilityCause();
        } catch (Throwable t) {
            logger.trace("Could not determine if Unsafe is available", t);
            // Probably failed to initialize PlatformDependent0.
            return new UnsupportedOperationException("Could not determine if Unsafe is available", t);
        }
    }
}
```

如果 `PlatformDependent.hasUnsafe()` 方法返回 true , 那么后续 Netty 都会创建 Unsafe 类型的 ByteBuf。

## 6\. Pooled or Unpooled

站在内存管理的角度上来讲，Netty 将 ByteBuf 分为了 池化（Pooled） 和 非池化（Unpooled）两个大类，其中 Unpooled 类型的 ByteBuf  是用到的时候才去临时创建，使用完的时候再去释放。

而 Direct Memory 的申请和释放开销相较于 Heap Memory 会大很多，Netty 在面对高并发网络通信的场景下，Direct Memory 的申请和释放是一个非常频繁的操作，这种大量频繁地内存申请释放操作对程序的性能影响是巨大的，因此 Netty 引入了内存池将这些 Direct Memory 统一池化管理起来。

Netty 提供了 `-Dio.netty.allocator.type` 参数来让我们决定是否采用内存池来管理 ByteBuf ， 默认值是 `pooled` , 也就是说 Netty 默认是采用池化的方式来管理 PooledByteBuf 。如果是安卓平台，那么默认是使用非池化的 ByteBuf （unpooled）。

*   当参数 `io.netty.allocator.type` 的值为 pooled 时，Netty 的默认 ByteBufAllocator 是 `PooledByteBufAllocator.DEFAULT` 。
    
*   当参数 `io.netty.allocator.type` 的值为 unpooled 时，Netty 的默认 ByteBufAllocator 是 `UnpooledByteBufAllocator.DEFAULT` 。
    

```
public final class ByteBufUtil {
    // 默认 PooledByteBufAllocator，池化管理 ByteBuf
    static final ByteBufAllocator DEFAULT_ALLOCATOR;

    static {
        // 默认为 pooled
        String allocType = SystemPropertyUtil.get(
                "io.netty.allocator.type", PlatformDependent.isAndroid() ? "unpooled" : "pooled");
        allocType = allocType.toLowerCase(Locale.US).trim();

        ByteBufAllocator alloc;
        if ("unpooled".equals(allocType)) {
            alloc = UnpooledByteBufAllocator.DEFAULT;
            logger.debug("-Dio.netty.allocator.type: {}", allocType);
        } else if ("pooled".equals(allocType)) {
            alloc = PooledByteBufAllocator.DEFAULT;
            logger.debug("-Dio.netty.allocator.type: {}", allocType);
        } else {
            alloc = PooledByteBufAllocator.DEFAULT;
            logger.debug("-Dio.netty.allocator.type: pooled (unknown: {})", allocType);
        }

        DEFAULT_ALLOCATOR = alloc;
    }
}
```

后续 Netty 在创建 SocketChannel 的时候，在 SocketChannelConfig 中指定的 ByteBufAllocator 就是这里的 `ByteBufUtil.DEFAULT_ALLOCATOR`，默认情况下为 PooledByteBufAllocator。

```
public interface ByteBufAllocator {
    ByteBufAllocator DEFAULT = ByteBufUtil.DEFAULT_ALLOCATOR;
}

public class DefaultChannelConfig implements ChannelConfig {
    // PooledByteBufAllocator
    private volatile ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
}
```

当 Netty 读取 Socket 中的网络数据时，首先会从 DefaultChannelConfig 中将 ByteBufAllocator 获取到，然后利用 ByteBufAllocator 从内存池中获取一个 DirectByteBuf ，最后将 Socket 中的数据读取到 DirectByteBuf 中，随后沿着 pipeline 向后传播，进行 IO 处理。

```
protected class NioByteUnsafe extends AbstractNioUnsafe {
        @Override
        public final void read() {
            // 获取 SocketChannelConfig
            final ChannelConfig config = config();
            // 获取 ByteBufAllocator ， 默认为 PooledByteBufAllocator
            final ByteBufAllocator allocator = config.getAllocator();
            // 从内存池中获取 byteBuf
            byteBuf = allocHandle.allocate(allocator);
            // 读取 socket 中的数据到 byteBuf
            allocHandle.lastBytesRead(doReadBytes(byteBuf));
            // 将 byteBuf 沿着 pipeline 向后传播
            pipeline.fireChannelRead(byteBuf);

            ....... 省略 .......
        }
}
```

除此之外，Netty 还提供了 `ChannelOption.ALLOCATOR` 选项，让我们可以在配置 ServerBootstrap 的时候为 SocketChannel 灵活指定自定义的 ByteBufAllocator 。

```
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
            // 灵活配置 ByteBufAllocator
          .childOption(ChannelOption.ALLOCATOR, UnpooledByteBufAllocator.DEFAULT;);
```

这里通过 ChannelOption 来配置 Socket 相关的属性是最高优先级的，它会覆盖掉一切默认配置。

## 7\. Metric

在第四小节中，我们介绍了 Cleaner 和 NoCleaner 这两种 DirectByteBuf，其中 CleanerDirectByteBuf  的整体 Direct Memory 的用量是受到 JVM 参数 `-XX:MaxDirectMemorySize` 限制的，而 NoCleanerDirectByteBuf 的整体 Direct Memory  可以突破该参数的限制，JVM 并不会统计这块 Direct Memory 的用量。

Netty 为了及时地释放这些 Direct Memory，通常默认选择 NoCleanerDirectByteBuf，这就要求 Netty 需要对这部分 Direct Memory 的用量进行自行统计限制。NoCleanerDirectByteBuf 的最大可用 Direct Memory 我们可以通过 `-Dio.netty.maxDirectMemory` 来指定，默认情况下等于 `-XX:MaxDirectMemorySize` 设置的值。

PlatformDependent 类中的 `DIRECT_MEMORY_COUNTER` 字段用于统计在 Netty 层面上，所有 NoCleanerDirectByteBuf 占用的 Direct Memory 大小。注意这里并不会统计 CleanerDirectByteBuf 的 Direct Memory 占用，这部分统计由 JVM 负责。

```
public final class PlatformDependent { 
    // 用于统计 NoCleaner 的 DirectByteBuf 所引用的 Native Memory 大小
    private static final AtomicLong DIRECT_MEMORY_COUNTER;

    public static ByteBuffer allocateDirectNoCleaner(int capacity) {
        // 增加 Native Memory 用量统计
        incrementMemoryCounter(capacity);
        try {
            // 分配 Native Memory
            // 初始化 NoCleaner 的 DirectByteBuffer
            return PlatformDependent0.allocateDirectNoCleaner(capacity);
        } catch (Throwable e) {
            decrementMemoryCounter(capacity);
            throwException(e);
            return null;
        }
    

    public static void freeDirectNoCleaner(ByteBuffer buffer) {
        int capacity = buffer.capacity();
        // 释放 Native Memory
        PlatformDependent0.freeMemory(PlatformDependent0.directBufferAddress(buffer));
        // 减少 Native Memory 用量统计
        decrementMemoryCounter(capacity);
    }  
}
```

PlatformDependent 类是 Netty 最底层的一个类，所有内存的分配，释放动作最终都是在该类中执行，因此 DIRECT\_MEMORY\_COUNTER 字段统计的是全局的 Direct Memory 大小（Netty 层面）。

每一次的内存申请 —— allocateDirectNoCleaner ， 都会增加 DIRECT\_MEMORY\_COUNTER 计数，每一次的内存释放 —— freeDirectNoCleaner，都会减少 DIRECT\_MEMORY\_COUNTER 计数。

我们可以通过 `PlatformDependent.usedDirectMemory()` 方法来获取 Netty 当前所占用的 Direct Memory 大小。但如果我们特殊指定了需要使用 CleanerDirectByteBuf ， 比如，将 `-Dio.netty.maxDirectMemory` 参数设置为 `0` , 那么这里将会返回  -1 。

```
    private static void incrementMemoryCounter(int capacity) {
        // 只统计 NoCleaner 的 DirectByteBuf 所引用的 Native Memory 
        if (DIRECT_MEMORY_COUNTER != null) {
            long newUsedMemory = DIRECT_MEMORY_COUNTER.addAndGet(capacity);
            if (newUsedMemory > DIRECT_MEMORY_LIMIT) {
                DIRECT_MEMORY_COUNTER.addAndGet(-capacity);
                throw new OutOfDirectMemoryError("failed to allocate " + capacity
                        + " byte(s) of direct memory (used: " + (newUsedMemory - capacity)
                        + ", max: " + DIRECT_MEMORY_LIMIT + ')');
            }
        }
    }

    private static void decrementMemoryCounter(int capacity) {
        if (DIRECT_MEMORY_COUNTER != null) {
            long usedMemory = DIRECT_MEMORY_COUNTER.addAndGet(-capacity);
            assert usedMemory >= 0;
        }
    }

    public static long usedDirectMemory() {
        return DIRECT_MEMORY_COUNTER != null ? DIRECT_MEMORY_COUNTER.get() : -1;
    }

```

除了 PlatformDependent 这里的全局统计之外，Netty 还提供了以 ByteBufAllocator 为粒度的内存占用统计，统计的维度包括 Heap Memory 的占用和 Direct Memory 的占用。

```
public final class UnpooledByteBufAllocator extends AbstractByteBufAllocator implements ByteBufAllocatorMetricProvider {
    // 从该 ByteBufAllocator 分配出去的内存统计
    private final UnpooledByteBufAllocatorMetric metric = new UnpooledByteBufAllocatorMetric();

    @Override
    public ByteBufAllocatorMetric metric() {
        return metric;
    }
    // 统计 Direct Memory 的占用
    void incrementDirect(int amount) {
        metric.directCounter.add(amount);
    }

    void decrementDirect(int amount) {
        metric.directCounter.add(-amount);
    }
    // 统计 Heap Memory 的占用
    void incrementHeap(int amount) {
        metric.heapCounter.add(amount);
    }

    void decrementHeap(int amount) {
        metric.heapCounter.add(-amount);
    }

}
```

Netty 定义的每一个  ByteBufAllocator 中，都会有一个 ByteBufAllocatorMetric 类型的字段，该类定义两个计数字段：directCounter，heapCounter。 分别用于统计 Direct Memory  和 Heap Memory 的占用。

```
    private static final class UnpooledByteBufAllocatorMetric implements ByteBufAllocatorMetric {
        final LongCounter directCounter = PlatformDependent.newLongCounter();
        final LongCounter heapCounter = PlatformDependent.newLongCounter();

        @Override
        public long usedHeapMemory() {
            return heapCounter.value();
        }

        @Override
        public long usedDirectMemory() {
            return directCounter.value();
        }

        @Override
        public String toString() {
            return StringUtil.simpleClassName(this) +
                    "(usedHeapMemory: " + usedHeapMemory() + "; usedDirectMemory: " + usedDirectMemory() + ')';
        }
    }
```

因此从内存占用统计的角度上来说，Netty 又会将整个 ByteBuf 体系分为 Instrumented 和 NoInstrumented 两大类，带有 Instrumented 前缀的 ByteBuf ，无论你是 Heap or Direct ， Cleaner or NoCleaner，Unsafe or NoUnsafe 类型的 ByteBuf ，Netty 都会统计这部分内存占用。

```
    private static final class InstrumentedUnpooledUnsafeNoCleanerDirectByteBuf
            extends UnpooledUnsafeNoCleanerDirectByteBuf {
        InstrumentedUnpooledUnsafeNoCleanerDirectByteBuf(
                UnpooledByteBufAllocator alloc, int initialCapacity, int maxCapacity) {
            // 构造普通的 UnpooledUnsafeNoCleanerDirectByteBuf
            super(alloc, initialCapacity, maxCapacity);
        }
        
        // 分配，释放 的时候更新 Direct Memory 
        @Override
        protected ByteBuffer allocateDirect(int initialCapacity) {
            ByteBuffer buffer = super.allocateDirect(initialCapacity);
            ((UnpooledByteBufAllocator) alloc()).incrementDirect(buffer.capacity());
            return buffer;
        }

        @Override
        protected void freeDirect(ByteBuffer buffer) {
            int capacity = buffer.capacity();
            super.freeDirect(buffer);
            ((UnpooledByteBufAllocator) alloc()).decrementDirect(capacity);
        }
    }
```

```
    private static final class InstrumentedUnpooledUnsafeDirectByteBuf extends UnpooledUnsafeDirectByteBuf {
        InstrumentedUnpooledUnsafeDirectByteBuf(
                UnpooledByteBufAllocator alloc, int initialCapacity, int maxCapacity) {
            // 构造普通的 UnpooledUnsafeDirectByteBuf
            super(alloc, initialCapacity, maxCapacity);
        }

        // 分配，释放 的时候更新 Direct Memory 
        @Override
        protected ByteBuffer allocateDirect(int initialCapacity) {
            ByteBuffer buffer = super.allocateDirect(initialCapacity);
            ((UnpooledByteBufAllocator) alloc()).incrementDirect(buffer.capacity());
            return buffer;
        }

        @Override
        protected void freeDirect(ByteBuffer buffer) {
            int capacity = buffer.capacity();
            super.freeDirect(buffer);
            ((UnpooledByteBufAllocator) alloc()).decrementDirect(capacity);
        }
    }
```

## 8\. ByteBufAllocator

在 Netty 中，ByteBuf 的创建必须通过 ByteBufAllocator 进行，不能直接显示地调用 ByteBuf 相关的构造函数自行创建。Netty 定义了两种类型的 ByteBufAllocator  ：

1.  PooledByteBufAllocator 负责池化 ByteBuf，这里正是 Netty 内存管理的核心，在下一篇文章中，笔者会详细的和大家介绍它。
    
2.  UnpooledByteBufAllocator 负责分配非池化的 ByteBuf，创建 ByteBuf 的时候临时向 OS 申请 Native Memory ，使用完之后，需要及时的手动调用 release 将 Native Memory 释放给 OS 。
    

`-Dio.netty.allocator.type` 参数可以让我们自行选择 ByteBufAllocator 的类型，默认值为 `pooled` , Netty 默认是采用池化的方式来管理 ByteBuf 。

```
public interface ByteBufAllocator {
    // 默认为 PooledByteBufAllocator
    ByteBufAllocator DEFAULT = ByteBufUtil.DEFAULT_ALLOCATOR;
}
```

除了以上两种官方定义的 ByteBufAllocator 之外，我们还可以根据自己实际业务场景来自行定制 ByteBufAllocator ， 然后通过第六小节中介绍的 `ChannelOption.ALLOCATOR` 选项，将 ByteBufAllocator 灵活指定为我们自行定制的实现。

对于 UnpooledByteBuf 来说，Netty 还专门提供了一个工具类 `Unpooled`，这里定义实现了很多针对 ByteBuf 的实用操作，比如，allocate，wrapped，copied 等。这里笔者以 DirectByteBuf 的创建为例进行说明：

```
public final class Unpooled {

    private static final ByteBufAllocator ALLOC = UnpooledByteBufAllocator.DEFAULT;

    public static ByteBuf directBuffer() {
        return ALLOC.directBuffer();
    }
}
```

Unpooled 底层依赖了 UnpooledByteBufAllocator ， 所有对 ByteBuf 的创建动作最终都会代理给这个 Allocator 。在 DirectBuffer 的创建过程中，我们可以看到前面介绍的所有类型的 ByteBuf。

```
public final class UnpooledByteBufAllocator {
    @Override
    protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
        final ByteBuf buf;
        if (PlatformDependent.hasUnsafe()) {
            buf = noCleaner ? new InstrumentedUnpooledUnsafeNoCleanerDirectByteBuf(this, initialCapacity, maxCapacity) :
                    new InstrumentedUnpooledUnsafeDirectByteBuf(this, initialCapacity, maxCapacity);
        } else {
            buf = new InstrumentedUnpooledDirectByteBuf(this, initialCapacity, maxCapacity);
        }
        // 是否启动内存泄露探测，如果启动则额外用 LeakAwareByteBuf 进行包装返回
        return disableLeakDetector ? buf : toLeakAwareBuffer(buf);
    }
}
```

*   首先 Netty 创建出来的所有 ByteBuf 都是带有 Metric 统计的，具体的 ByteBuf 类型都会带有 Instrumented 前缀。
    
*   如果当前 JRE 环境支持 Unsafe ， 那么后续就会通过 Unsafe 的方式来对 ByteBuf 进行相关操作（默认），具体的 ByteBuf 类型都会带有 Unsafe 前缀。
    
*   如果我们明确指定了 NoCleaner 类型的 DirectByteBuf（默认），那么创建出来的 ByteBuf 类型就会带有 NoCleaner 前缀，由于没有 Cleaner ，这就要求我们使用完 ByteBuf 的时候必须及时地手动进行释放。
    
*   如果我们开启了内存泄露探测，那么创建流程的最后，Netty 会用一个  LeakAwareByteBuf 去包装新创建出来的 ByteBuf，当这个 ByteBuf 被 GC 的时候，Netty 会通过相关引用计数来判断是否存在忘记 release 的情况，从而确定出是否发生内存泄露。
    

## 总结

本文笔者从八个角度为大家详细的剖析了 ByteBuf 的整体设计，这八个角度分别是：内存区域分布的角度，内存管理的角度，内存访问的角度，内存回收的角度，内存统计 Metric 的角度，零拷贝的角度，引用计数的角度，扩容的角度。

到现在为止，我们只是扫清了 Netty 内存管理外围的一些障碍，那么下一篇文章，笔者将带大家深入到内存管理的核心，彻底让大家弄懂 Netty 的内存管理机制。好了，本文的内容就到这里，我们下篇文章见~~~
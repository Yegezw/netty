/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.util;

import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.internal.ObjectPool;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.netty.util.internal.MathUtil.safeFindNextPositivePowerOfTwo;
import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Light-weight object pool based on a thread-local stack.
 *
 * @param <T> the type of the pooled object
 */
public abstract class Recycler<T> {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(Recycler.class);

    /**
     * 空 Handler, 表示该对象不会被池化
     */
    @SuppressWarnings("rawtypes")
    private static final Handle NOOP_HANDLE = new Handle() {
        @Override
        public void recycle(Object object) {
            // NOOP
        }
    };

    /**
     * 池化对象的回收 Id, 标识对象被哪个线程回收
     */
    private static final AtomicInteger ID_GENERATOR = new AtomicInteger(Integer.MIN_VALUE);
    /**
     * 标识创建池化对象的线程 Id, 所有创建线程的 OWN_THREAD_ID 都是一样的
     * <br>
     * 主要用来区分创建线程与回收线程, 回收线程拥有各自不同的 Id
     */
    private static final int OWN_THREAD_ID = ID_GENERATOR.getAndIncrement();

    /**
     * 每个线程本地对象池的初始容量, 默认 256
     */
    private static final int INITIAL_CAPACITY;
    /**
     * 每个线程本地对象池的最大容量, 默认 4K
     */
    private static final int DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD = 4 * 1024; // Use 4k instances as default.
    /**
     * 每个线程本地对象池的最大容量, 默认 4K
     */
    private static final int DEFAULT_MAX_CAPACITY_PER_THREAD;
    /**
     * 创建线程回收对象时的回收比例, 默认 8
     */
    private static final int RATIO;
    /**
     * 回收线程回收对象时的回收比例, 默认 8
     */
    private static final int DELAYED_QUEUE_RATIO;

    /**
     * 默认 16
     */
    private static final int LINK_CAPACITY;
    /**
     * 针对创建线程中的 Stack, 回收线程可帮助回收的最大容量因子, 默认 2
     */
    private static final int MAX_SHARED_CAPACITY_FACTOR;
    /**
     * 每个回收线程可以创建的 WeakOrderQueue 最大个数, 默认 2 * CPU 核心数, 用于处理跨线程回收对象
     */
    private static final int MAX_DELAYED_QUEUES_PER_THREAD;

    static {
        // In the future, we might have different maxCapacity for different object types.
        // e.g. io.netty.recycler.maxCapacity.writeTask
        //      io.netty.recycler.maxCapacity.outboundBuffer
        int maxCapacityPerThread = SystemPropertyUtil.getInt("io.netty.recycler.maxCapacityPerThread",
                SystemPropertyUtil.getInt("io.netty.recycler.maxCapacity", DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD)
        );
        if (maxCapacityPerThread < 0) {
            maxCapacityPerThread = DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD;
        }

        DEFAULT_MAX_CAPACITY_PER_THREAD = maxCapacityPerThread;

        MAX_SHARED_CAPACITY_FACTOR = max(2,
                SystemPropertyUtil.getInt("io.netty.recycler.maxSharedCapacityFactor",
                        2));

        MAX_DELAYED_QUEUES_PER_THREAD = max(0,
                SystemPropertyUtil.getInt("io.netty.recycler.maxDelayedQueuesPerThread",
                        // We use the same value as default EventLoop number
                        NettyRuntime.availableProcessors() * 2));

        LINK_CAPACITY = safeFindNextPositivePowerOfTwo(
                max(SystemPropertyUtil.getInt("io.netty.recycler.linkCapacity", 16), 16));

        // By default we allow one push to a Recycler for each 8th try on handles that were never recycled before.
        // This should help to slowly increase the capacity of the recycler while not be too sensitive to allocation
        // bursts.
        RATIO = max(0, SystemPropertyUtil.getInt("io.netty.recycler.ratio", 8));
        DELAYED_QUEUE_RATIO = max(0, SystemPropertyUtil.getInt("io.netty.recycler.delayedQueue.ratio", RATIO));

        INITIAL_CAPACITY = min(DEFAULT_MAX_CAPACITY_PER_THREAD, 256);

        if (logger.isDebugEnabled()) {
            if (DEFAULT_MAX_CAPACITY_PER_THREAD == 0) {
                logger.debug("-Dio.netty.recycler.maxCapacityPerThread: disabled");
                logger.debug("-Dio.netty.recycler.maxSharedCapacityFactor: disabled");
                logger.debug("-Dio.netty.recycler.linkCapacity: disabled");
                logger.debug("-Dio.netty.recycler.ratio: disabled");
                logger.debug("-Dio.netty.recycler.delayedQueue.ratio: disabled");
            } else {
                logger.debug("-Dio.netty.recycler.maxCapacityPerThread: {}", DEFAULT_MAX_CAPACITY_PER_THREAD);
                logger.debug("-Dio.netty.recycler.maxSharedCapacityFactor: {}", MAX_SHARED_CAPACITY_FACTOR);
                logger.debug("-Dio.netty.recycler.linkCapacity: {}", LINK_CAPACITY);
                logger.debug("-Dio.netty.recycler.ratio: {}", RATIO);
                logger.debug("-Dio.netty.recycler.delayedQueue.ratio: {}", DELAYED_QUEUE_RATIO);
            }
        }
    }

    // =================================================================================================================

    /**
     * 创建线程本地对象池的最大容量, 默认 4K
     */
    private final int maxCapacityPerThread;
    /**
     * 针对创建线程中的 Stack, 回收线程可帮助回收的最大容量因子, 默认 2
     */
    private final int maxSharedCapacityFactor;
    /**
     * 创建线程回收对象时的回收比例, 默认 8
     */
    private final int interval;
    /**
     * 回收线程回收对象时的回收比例, 默认 8
     */
    private final int delayedQueueInterval;
    /**
     * 每个回收线程可以创建的 WeakOrderQueue 最大个数, 默认 2 * CPU 核心数, 用于处理跨线程回收对象
     */
    private final int maxDelayedQueuesPerThread;

    /**
     * 用于访问每个线程私有的对象栈池
     * <br>
     * (Stack) FastThreadLocalThread.InternalThreadLocalMap.indexedVariables[FastThreadLocal.index]
     */
    private final FastThreadLocal<Stack<T>> threadLocal = new FastThreadLocal<Stack<T>>() {
        @Override
        protected Stack<T> initialValue() {
            return new Stack<T>(
                    Recycler.this, Thread.currentThread(), 
                    maxCapacityPerThread, maxSharedCapacityFactor,
                    interval, maxDelayedQueuesPerThread, delayedQueueInterval
            );
        }

        @Override
        protected void onRemoval(Stack<T> value) {
            // Let us remove the WeakOrderQueue from the WeakHashMap directly if its safe to remove some overhead
            if (value.threadRef.get() == Thread.currentThread()) {
               if (DELAYED_RECYCLED.isSet()) {
                   DELAYED_RECYCLED.get().remove(value);
               }
            }
        }
    };

    protected Recycler() {
        this(DEFAULT_MAX_CAPACITY_PER_THREAD);
    }

    protected Recycler(int maxCapacityPerThread) {
        this(maxCapacityPerThread, MAX_SHARED_CAPACITY_FACTOR);
    }

    protected Recycler(int maxCapacityPerThread, int maxSharedCapacityFactor) {
        this(maxCapacityPerThread, maxSharedCapacityFactor, RATIO, MAX_DELAYED_QUEUES_PER_THREAD);
    }

    protected Recycler(int maxCapacityPerThread, int maxSharedCapacityFactor,
                       int ratio, int maxDelayedQueuesPerThread) {
        this(maxCapacityPerThread, maxSharedCapacityFactor, ratio, maxDelayedQueuesPerThread,
                DELAYED_QUEUE_RATIO);
    }

    protected Recycler(int maxCapacityPerThread, int maxSharedCapacityFactor,
                       int ratio, int maxDelayedQueuesPerThread, int delayedQueueRatio) {
        interval = max(0, ratio);
        delayedQueueInterval = max(0, delayedQueueRatio);
        if (maxCapacityPerThread <= 0) {
            this.maxCapacityPerThread = 0;
            this.maxSharedCapacityFactor = 1;
            this.maxDelayedQueuesPerThread = 0;
        } else {
            this.maxCapacityPerThread = maxCapacityPerThread;
            this.maxSharedCapacityFactor = max(1, maxSharedCapacityFactor);
            this.maxDelayedQueuesPerThread = max(0, maxDelayedQueuesPerThread);
        }
    }

    // =================================================================================================================

    /**
     * 入口一: 获取池化对象
     */
    @SuppressWarnings("unchecked")
    public final T get() {
        if (maxCapacityPerThread == 0) {
            return newObject((Handle<T>) NOOP_HANDLE);
        }
        Stack<T> stack = threadLocal.get();
        DefaultHandle<T> handle = stack.pop();
        if (handle == null) {
            handle = stack.newHandle();
            handle.value = newObject(handle);
        }
        return (T) handle.value;
    }

    /**
     * @deprecated use {@link Handle#recycle(Object)}.
     */
    @Deprecated
    public final boolean recycle(T o, Handle<T> handle) {
        if (handle == NOOP_HANDLE) {
            return false;
        }

        DefaultHandle<T> h = (DefaultHandle<T>) handle;
        if (h.stack.parent != this) {
            return false;
        }

        h.recycle(o);
        return true;
    }

    /**
     * 栈大小
     * <br>
     * FastThreadLocalThread.InternalThreadLocalMap.indexedVariables[FastThreadLocal.index].size
     */
    final int threadLocalSize() {
        return threadLocal.get().size;
    }

    /**
     * 栈容量
     * <br>
     * FastThreadLocalThread.InternalThreadLocalMap.indexedVariables[FastThreadLocal.index].elements.length
     */
    final int threadLocalCapacity() {
        return threadLocal.get().elements.length;
    }

    protected abstract T newObject(Handle<T> handle);

    public interface Handle<T> extends ObjectPool.Handle<T>  { }

    // =================================================================================================================

    private static final class DefaultHandle<T> implements Handle<T> {

        /*
         * 池化对象回收可以分为两种情况
         *
         * 1、由创建线程直接回收
         * 这种回收情况就是一步到位, 直接回收至创建线程对应的 Stack 中, 这种情况下是不分阶段的, recycleId = lastRecycledId = OWN_THREAD_ID
         *
         * 2、由回收线程帮助回收
         * 这种回收情况下就要分步进行了, 首先由回收线程将池化对象暂时存储在其创建线程对应 Stack 中的 WeakOrderQueue 链表中
         * 此时并没有完成真正的对象回收, recycleId = 0, lastRecycledId = 回收线程 Id (WeakOrderQueue#id)
         * 当创建线程将 WeakOrderQueue 链表中的待回收对象转移至 Stack 结构中的数组栈之后, 这时池化对象才算真正完成了回收动作
         * recycleId = lastRecycledId = 回收线程 Id (WeakOrderQueue#id)
         * lastRecycledId 和 recycleId 这两个字段主要是用来: 标记池化对象所处的回收阶段, 以及在这些回收阶段具体被哪个线程进行回收
         */

        /**
         * 标识最近被哪个线程回收, 被回收之前均是 0
         */
        int lastRecycledId;
        /**
         * 标识最终被哪个线程回收, 在没被回收前是 0
         */
        int recycleId;

        /**
         * 是否已经被回收
         */
        boolean hasBeenRecycled;

        /**
         * 对象所属的线程栈
         */
        Stack<?> stack;
        /**
         * 池化对象
         */
        Object value;

        DefaultHandle(Stack<?> stack) {
            this.stack = stack;
        }

        /**
         * 入口二: 回收对象 object
         */
        @Override
        public void recycle(Object object) {
            if (object != value) {
                throw new IllegalArgumentException("object does not belong to handle");
            }

            // 当池化对象对应的创建线程挂掉的时候, 对应的 Stack 随后也被 GC 回收掉, 这时就不需要再回收该池化对象了
            Stack<?> stack = this.stack;
            if (lastRecycledId != recycleId || stack == null) {
                // handler 初次创建 + 从对象池中获取到时, recycleId = lastRecycledId = 0
                // 创建线程回收对象后 recycleId = lastRecycledId = OWN_THREAD_ID
                // 回收线程回收对象后 lastRecycledId = 回收线程 Id, 当对象被转移到 stack 中后 recycleId = lastRecycledId = 回收线程 Id
                throw new IllegalStateException("recycled already");
            }

            stack.push(this);
        }
    }

    /**
     * Thread2 帮助 Thread1 回收对象
     */
    private static final FastThreadLocal<Map<Stack<?>, WeakOrderQueue>> DELAYED_RECYCLED =
            new FastThreadLocal<Map<Stack<?>, WeakOrderQueue>>() {
                // bug2 -> key 考虑使用弱引用 ? WeakHashMap<Stack<?>, WeakOrderQueue>
                @Override
                protected Map<Stack<?>, WeakOrderQueue> initialValue() {
                    return new WeakHashMap<Stack<?>, WeakOrderQueue>();
                }
            };

    // a queue that makes only moderate guarantees about visibility: items are seen in the correct order,
    // but we aren't absolutely guaranteed to ever see anything at all, thereby keeping the queue cheap to maintain
    private static final class WeakOrderQueue extends WeakReference<Thread> {

        /*
         * WeakOrderQueue 弱引用创建它的线程
         */

        /**
         * DUMMY 不能存储 DefaultHandle
         */
        static final WeakOrderQueue DUMMY = new WeakOrderQueue();

        // Let Link extend AtomicInteger for intrinsics. The Link itself will be used as writerIndex.
        /**
         * Link 由 Head 创建: 单链表, 继承 AtomicInteger 可以用来当做 writeIndex 使用
         */
        @SuppressWarnings("serial")
        static final class Link extends AtomicInteger {
            /**
             * 存储的任意 elements[i].stack = null
             */
            final DefaultHandle<?>[] elements = new DefaultHandle[LINK_CAPACITY];

            /*
             * elements [readIndex ... writeIndex)
             * 待读取 readIndex
             * 待写入 writeIndex
             *
             * 由于回收线程在向 Link 节点添加回收对象的时候需要修改 writeIndex
             * 与此同时创建线程在转移 Link 节点的时候需要读取 writeIndex
             * 所以 writeIndex 需要保证线程安全性, 故采用 AtomicInteger 类型存储
             */

            /**
             * 只会被创建线程使用
             */
            int readIndex;
            Link next;
        }

        // Its important this does not hold any reference to either Stack or WeakOrderQueue.
        private static final class Head {
            /**
             * stack.availableSharedCapacity<br>
             * 异线程回收对象时, 其它线程能保存的被回收对象的最大个数, 默认 2K, 最小 16<br>
             */
            private final AtomicInteger availableSharedCapacity;

            Link link;

            Head(AtomicInteger availableSharedCapacity) {
                this.availableSharedCapacity = availableSharedCapacity;
            }

            /**
             * 收回所有空间并解除链接
             * <br>
             * Reclaim all used space and also unlink the nodes to prevent GC nepotism.
             */
            void reclaimAllSpaceAndUnlink() {
                Link head = link;
                link = null;
                int reclaimSpace = 0;
                while (head != null) {
                    reclaimSpace += LINK_CAPACITY;
                    Link next = head.next;
                    // Unlink to help GC and guard against GC nepotism.
                    head.next = null;
                    head = next;
                }
                if (reclaimSpace > 0) {
                    reclaimSpace(reclaimSpace);
                }
            }

            /**
             * 回收空间
             */
            private void reclaimSpace(int space) {
                availableSharedCapacity.addAndGet(space);
            }

            /**
             * 参数 link 为新的 head 节点, 当前 head 指针指向的节点已经被回收完毕
             */
            void relink(Link link) {
                reclaimSpace(LINK_CAPACITY);
                this.link = link;
            }

            /**
             * Creates a new {@link} and returns it if we can reserve enough space for it, otherwise it
             * returns {@code null}.
             */
            Link newLink() {
                return reserveSpaceForLink(availableSharedCapacity) ? new Link() : null;
            }

            /**
             * 预留空间用于链接
             * <br>
             * 创建 Link 前就把 availableSharedCapacity -= LINK_CAPACITY
             * <br>
             * 回收时不用考虑 Link 中的数据量大小, 仅 availableSharedCapacity += LINK_CAPACITY 即可
             */
            static boolean reserveSpaceForLink(AtomicInteger availableSharedCapacity) {
                for (;;) {
                    int available = availableSharedCapacity.get();
                    if (available < LINK_CAPACITY) {
                        return false;
                    }
                    if (availableSharedCapacity.compareAndSet(available, available - LINK_CAPACITY)) {
                        return true;
                    }
                }
            }
        }

        /*
         * 避免 Link 并发: 头查法 + 尾插法
         */

        // chain of data items
        /**
         * head 指针始终指向第一个未被转移完毕的 Link 节点
         */
        private final Head head;
        private Link tail;
        // pointer to another queue of delayed items for the same stack
        private WeakOrderQueue next;

        /**
         * 回收线程回收 Id, 每个 WeakOrderQueue 分配一个
         * <br>
         * 同一 Stack 下的一个回收线程对应一个 WeakOrderQueue 节点
         */
        private final int id = ID_GENERATOR.getAndIncrement();
        /**
         * 回收线程回收对象时的回收比例, 默认 8
         */
        private final int interval;
        /**
         * 回收对象计数, 与 interval 配合, 控制回收速率, 新创建的对象为 interval
         */
        private int handleRecycleCount;

        private WeakOrderQueue() {
            super(null);
            head = new Head(null);
            interval = 0;
        }

        private WeakOrderQueue(Stack<?> stack, Thread thread) {
            super(thread);
            tail = new Link();

            // Its important that we not store the Stack itself in the WeakOrderQueue as the Stack also is used in
            // the WeakHashMap as key. So just store the enclosed AtomicInteger which should allow to have the
            // Stack itself GCed.
            head = new Head(stack.availableSharedCapacity);
            head.link = tail;
            interval = stack.delayedQueueInterval;
            handleRecycleCount = interval; // Start at interval so the first one will be recycled.
        }

        static WeakOrderQueue newQueue(Stack<?> stack, Thread thread) {
            // We allocated a Link so reserve the space
            if (!Head.reserveSpaceForLink(stack.availableSharedCapacity)) {
                return null;
            }
            final WeakOrderQueue queue = new WeakOrderQueue(stack, thread);
            // Done outside of the constructor to ensure WeakOrderQueue.this does not escape the constructor and so
            // may be accessed while its still constructed.
            stack.setHead(queue); // 头插法、加锁

            return queue;
        }

        WeakOrderQueue getNext() {
            return next;
        }

        void setNext(WeakOrderQueue next) {
            assert next != this;
            this.next = next;
        }

        /**
         * 收回所有空间并解除链接
         */
        void reclaimAllSpaceAndUnlink() {
            head.reclaimAllSpaceAndUnlink();
            this.next = null;
        }

        /*
         * Link 存储 DefaultHandle
         * 放入时将 handle.stack 置 null
         * 取出时将 handle.stack 置 dst
         * 如果 Stack 不再使用, 期望被 GC 回收, 发现 handle 中还持有 Stack 的引用, 那么就无法被 GC 回收, 造成内存泄漏
         */

        /**
         * 每 8 个只有 1 个被真正回收
         */
        void add(DefaultHandle<?> handle) {
            handle.lastRecycledId = id;

            // While we also enforce the recycling ratio when we transfer objects from the WeakOrderQueue to the Stack
            // we better should enforce it as well early. Missing to do so may let the WeakOrderQueue grow very fast
            // without control
            if (handleRecycleCount < interval) {
                handleRecycleCount++;
                // Drop the item to prevent recycling to aggressive.
                return;
            }
            handleRecycleCount = 0;

            // 如果 tailLink 已经写满, 新建一个 Link 追加到尾部
            Link tail = this.tail;
            int writeIndex;
            if ((writeIndex = tail.get()) == LINK_CAPACITY) {
                Link link = head.newLink();
                if (link == null) {
                    // Drop it.
                    return;
                }
                // We allocate a Link so reserve the space
                this.tail = tail = tail.next = link;

                writeIndex = tail.get();
            }
            tail.elements[writeIndex] = handle;
            handle.stack = null; // 注意: 放入时将 handle.stack 置 null
            // we lazy set to ensure that setting stack to null appears before we unnull it in the owning thread;
            // this also means we guarantee visibility of an element in the queue if we see the index updated
            // 并不需要保证线程之间的实时可见性, 只需要保证最终可见性即可
            // 因为只要创建线程 Stack 结构中的数组栈为空, 创建线程就会从 WeakOrderQueue 链表中转移对象
            // 以后会有很多次机会来 WeakOrderQueue 链表中转移对象, 什么时候看见了, 什么时候转移它, 并不需要实时性
            // 退一万步讲, 即使全部看不到, 大不了创建线程直接创建一个对象返回就行了
            tail.lazySet(writeIndex + 1);
        }

        /**
         * tail 已读完
         */
        boolean hasFinalData() {
            return tail.readIndex != tail.get();
        }

        // transfer as many items as we can from this queue to the stack, returning true if any were transferred
        /**
         * 每 8 个只有 1 个被真正转移
         */
        @SuppressWarnings("rawtypes")
        boolean transfer(Stack<?> dst) {
            Link head = this.head.link;
            // 还没有待回收对象
            if (head == null) {
                return false;
            }
            // 头结点中的待回收对象已经被转移完毕
            if (head.readIndex == LINK_CAPACITY) {
                if (head.next == null) {
                    return false;
                }
                head = head.next;
                this.head.relink(head);
            }

            // 评估是否应该对 Stack 进行扩容
            // head.elements[srcStart, srcEnd        ) 共 srcSize
            // dst .elements[dstSize,  actualCapacity) 共 srcSize
            final int srcStart = head.readIndex;
            int srcEnd = head.get();
            final int srcSize = srcEnd - srcStart;
            if (srcSize == 0) {
                return false;
            }

            final int dstSize = dst.size;
            final int expectedCapacity = dstSize + srcSize;

            if (expectedCapacity > dst.elements.length) {
                final int actualCapacity = dst.increaseCapacity(expectedCapacity);
                srcEnd = min(srcStart + actualCapacity - dstSize, srcEnd);
            }

            // 转移回收对象
            if (srcStart != srcEnd) {
                final DefaultHandle[] srcElems = head.elements;
                final DefaultHandle[] dstElems = dst.elements;
                int newDstSize = dstSize;
                for (int i = srcStart; i < srcEnd; i++) {
                    DefaultHandle<?> element = srcElems[i];
                    if (element.recycleId == 0) {
                        // recycleId = 0 表示对象还没有被真正的回收到 stack 中
                        // 设置 recycleId 表明是被哪个 weakOrderQueue 回收的
                        element.recycleId = element.lastRecycledId;
                    } else if (element.recycleId != element.lastRecycledId) {
                        throw new IllegalStateException("recycled already");
                    }
                    srcElems[i] = null;

                    // 从 weakOrderQueue 将待回收对象真正回收到所属 stack 之前, 需要进行回收频率控制
                    if (dst.dropHandle(element)) {
                        // Drop the object.
                        continue;
                    }
                    element.stack = dst; // 注意: 取出时将 handle.stack 置 dst
                    dstElems[newDstSize ++] = element;
                }

                if (srcEnd == LINK_CAPACITY && head.next != null) {
                    // Add capacity back as the Link is GCed.
                    this.head.relink(head.next);
                }

                head.readIndex = srcEnd;
                if (dst.size == newDstSize) {
                    return false;
                }
                dst.size = newDstSize;
                return true;
            } else {
                // The destination stack is full already.
                return false;
            }
        }
    }

    /**
     * 每个线程私有的对象栈池
     * <br>
     * 位于 FastThreadLocalThread.InternalThreadLocalMap.indexedVariables[FastThreadLocal.index]
     */
    private static final class Stack<T> {

        // we keep a queue of per-thread queues, which is appended to once only, each time a new thread other
        // than the stack owner recycles: when we run out of items in our stack we iterate this collection
        // to scavenge those that can be reused. this permits us to incur minimal thread synchronisation whilst
        // still recycling all items.
        /**
         * 所属的 Recycler
         */
        final Recycler<T> parent;

        // We store the Thread in a WeakReference as otherwise we may be the only ones that still hold a strong
        // Reference to the Thread itself after it died because DefaultHandle will hold a reference to the Stack.
        //
        // The biggest issue is if we do not use a WeakReference the Thread may not be able to be collected at all if
        // the user will store a reference to the DefaultHandle somewhere and never clear this reference (or not clear
        // it in a timely manner).
        /**
         * 所属线程的弱引用, 用于判断是否异线程回收
         * <br>
         * defaultHandler -> stack -> thread 避免 Thread 无法被回收
         */
        final WeakReference<Thread> threadRef;

        /**
         * 对象池的最大大小, 默认最大为 4K
         */
        private final int maxCapacity;
        /**
         * 创建线程回收对象时的回收比例, 默认 8
         */
        private final int interval;
        /**
         * 回收线程回收对象时的回收比例, 默认 8
         */
        private final int delayedQueueInterval;
        /**
         * 每个回收线程可以创建的 WeakOrderQueue 最大个数, 默认 2 * CPU 核心数, 用于处理跨线程回收对象
         */
        private final int maxDelayedQueues;
        /**
         * 异线程回收对象时, 其它线程能保存的被回收对象的最大个数, 默认 2K, 最小 16
         * <br>
         * 多个 WeakOrderQueue 共享该容量, 因此用 AtomicInteger 保证线程安全
         */
        final AtomicInteger availableSharedCapacity;

        /**
         * 栈, 初始容量 256, 最大容量 4K
         */
        DefaultHandle<?>[] elements;
        /**
         * 栈大小
         */
        int size;
        /**
         * 回收对象计数, 与 interval 配合, 控制回收速率
         */
        private int handleRecycleCount;
        private WeakOrderQueue cursor, prev;
        /**
         * 存储其它线程回收到当前线程所分配的对象 - 头插法、加锁
         */
        private volatile WeakOrderQueue head;

        Stack(Recycler<T> parent, Thread thread, int maxCapacity, int maxSharedCapacityFactor,
              int interval, int maxDelayedQueues, int delayedQueueInterval) {
            this.parent = parent;
            threadRef = new WeakReference<Thread>(thread);
            this.maxCapacity = maxCapacity;
            availableSharedCapacity = new AtomicInteger(max(maxCapacity / maxSharedCapacityFactor, LINK_CAPACITY));
            elements = new DefaultHandle[min(INITIAL_CAPACITY, maxCapacity)];
            this.interval = interval;
            this.delayedQueueInterval = delayedQueueInterval;
            handleRecycleCount = interval; // Start at interval so the first one will be recycled.
            this.maxDelayedQueues = maxDelayedQueues;
        }

        // Marked as synchronized to ensure this is serialized.
        /**
         * 头插法、加锁
         */
        synchronized void setHead(WeakOrderQueue queue) {
            queue.setNext(head);
            head = queue;
        }

        /**
         * elements 扩容
         */
        int increaseCapacity(int expectedCapacity) {
            int newCapacity = elements.length;
            int maxCapacity = this.maxCapacity;
            do {
                newCapacity <<= 1;
            } while (newCapacity < expectedCapacity && newCapacity < maxCapacity);

            newCapacity = min(newCapacity, maxCapacity);
            if (newCapacity != elements.length) {
                elements = Arrays.copyOf(elements, newCapacity);
            }

            return newCapacity;
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        DefaultHandle<T> pop() {
            int size = this.size;
            if (size == 0) {
                if (!scavenge()) {
                    return null;
                }
                size = this.size;
                if (size <= 0) {
                    // double check, avoid races
                    return null;
                }
            }
            size --;
            DefaultHandle ret = elements[size];
            elements[size] = null;
            // As we already set the element[size] to null we also need to store the updated size before we do
            // any validation. Otherwise we may see a null value when later try to pop again without a new element
            // added before.
            this.size = size;

            // 对象初次创建 + 回收对象再次使用时, recycleId = lastRecycleId = 0
            if (ret.lastRecycledId != ret.recycleId) {
                throw new IllegalStateException("recycled multiple times");
            }
            ret.recycleId = 0;
            ret.lastRecycledId = 0;
            return ret;
        }

        /**
         * 将其它线程 WeakOrderQueue 中的对象, 转移到自身的对象池中
         */
        private boolean scavenge() {
            // continue an existing scavenge, if any
            if (scavengeSome()) {
                return true;
            }

            // reset our scavenge cursor
            prev = null;
            cursor = head;
            return false;
        }

        /**
         * 将其它线程 WeakOrderQueue 中的对象, 转移到自身的对象池中
         */
        private boolean scavengeSome() {
            WeakOrderQueue prev;
            WeakOrderQueue cursor = this.cursor;
            // 第一次从 WeakOrderQueue 链表中获取对象
            if (cursor == null) {
                prev = null;
                cursor = head;
                if (cursor == null) {
                    return false;
                }
            } else {
                // 用于删除当前 cursor 节点
                prev = this.prev;
            }

            // 循环从 WeakOrderQueue 链表中找到一个可用的对象实例
            boolean success = false;
            do {
                // 尝试迁移 WeakOrderQueue 中部分对象实例到 Stack 中, 一次转移一个 link
                if (cursor.transfer(this)) {
                    success = true;
                    break;
                }
                // 如果当前 cursor 节点没有待回收对象可转移, 那么就继续遍历链表获取下一个 weakOrderQueue 节点
                WeakOrderQueue next = cursor.getNext();
                // cursor 所属的线程已退出
                if (cursor.get() == null) {
                    // If the thread associated with the queue is gone, unlink it, after
                    // performing a volatile read to confirm there is no data left to collect.
                    // We never unlink the first queue, as we don't want to synchronize on updating the head.
                    // 回收该 cursor.Link 所有对象
                    if (cursor.hasFinalData()) {
                        for (;;) {
                            if (cursor.transfer(this)) {
                                success = true;
                            } else {
                                break;
                            }
                        }
                    }

                    // cursor.Link 已回收完毕, 将当前节点从链表中删除, unlink 当前 cursor 节点
                    // 这里需要注意的是: 永远不会删除第一个节点, 因为更新头结点是一个同步方法, 避免更新头结点而导致的竞争开销
                    // prev == null 说明当前 cursor 节点是头结点, 不用 unlink
                    // 如果不是头结点, 就将其从链表中删除, 因为这个节点不会再有线程来收集池化对象了
                    if (prev != null) {
                        // Ensure we reclaim all space before dropping the WeakOrderQueue to be GC'ed.
                        cursor.reclaimAllSpaceAndUnlink();
                        prev.setNext(next);
                    }
                } else {
                    prev = cursor;
                }

                cursor = next;

            } while (cursor != null && !success);

            this.prev = prev;
            this.cursor = cursor;
            return success;
        }

        /**
         * 创建线程回收对象时, handle.stack != null
         * <br>
         * 其它线程回收对象时, handle.stack == null
         */
        void push(DefaultHandle<?> item) {
            Thread currentThread = Thread.currentThread();
            if (threadRef.get() == currentThread) {
                // The current Thread is the thread that belongs to the Stack, we can try to push the object now.
                pushNow(item);                  // item 是当前线程的对象, 直接入栈
            } else if (threadRef.get() == null) {
                // Bug
                // when the thread that belonged to the Stack was died or GC'ed, 
                // There is no need to add this item to WeakOrderQueue-linked-list which belonged to the Stack any more
                item.stack = null;
                // bug1 -> 清空所有 WeakOrderQueue ? WeakHashMap<Stack<?>, WeakOrderQueue>
            } else {
                // The current Thread is not the one that belongs to the Stack
                // (or the Thread that belonged to the Stack was collected already), we need to signal that the push
                // happens later.
                pushLater(item, currentThread); // item 是其它线程的对象, 为防止并发写入, 不能直接入栈
            }
        }

        private void pushNow(DefaultHandle<?> item) {
            // 池化对象被回收前 recycleId = lastRecycleId = 0
            // 如果其中之一不为 0 说明已经被回收了
            if ((item.recycleId | item.lastRecycledId) != 0) {
                throw new IllegalStateException("recycled already");
            }
            // 此处是由创建线程回收, 将池化对象的 recycleId 与 lastRecycleId 设置为创建线程 Id (OWN_THREAD_ID)
            // 注意这里的 OWN_THREAD_ID 是一个固定的值, 是因为这里的视角是池化对象的视角, 只需要区分创建线程和非创建线程即可
            // 对于一个池化对象来说创建线程只有一个, 所以用一个固定的 OWN_THREAD_ID 来表示创建线程 Id
            item.recycleId = item.lastRecycledId = OWN_THREAD_ID;

            int size = this.size;
            // 1、超出最大容量  2、控制回收速率
            if (size >= maxCapacity || dropHandle(item)) {
                // Hit the maximum capacity or should drop - drop the possibly youngest object.
                return;
            }
            if (size == elements.length) {
                elements = Arrays.copyOf(elements, min(size << 1, maxCapacity));
            }

            elements[size] = item;
            this.size = size + 1;
        }

        /**
         * item 是其它线程的对象、thread 是当前线程
         * <br>
         * 为防止并发写入, 不能直接入栈, this 就是 item 所属的栈
         */
        private void pushLater(DefaultHandle<?> item, Thread thread) {
            if (maxDelayedQueues == 0) {
                // We don't support recycling across threads and should just drop the item on the floor.
                return;
            }

            // we don't want to have a ref to the queue as the value in our weak map
            // so we null it out; to ensure there are no races with restoring it later
            // we impose a memory ordering here (no-op on x86)
            Map<Stack<?>, WeakOrderQueue> delayedRecycled = DELAYED_RECYCLED.get();
            // 虽然线程不是 item 所属的线程, 但是 this 是 item 所属的栈
            WeakOrderQueue queue = delayedRecycled.get(this);
            if (queue == null) {
                if (delayedRecycled.size() >= maxDelayedQueues) {
                    // Add a dummy queue so we know we should drop the object
                    delayedRecycled.put(this, WeakOrderQueue.DUMMY);
                    return;
                }
                // Check if we already reached the maximum number of delayed queues and if we can allocate at all.
                if ((queue = newWeakOrderQueue(thread)) == null) {
                    // drop object
                    return;
                }
                delayedRecycled.put(this, queue);
            } else if (queue == WeakOrderQueue.DUMMY) {
                // drop object
                return;
            }

            queue.add(item);
        }

        /**
         * Allocate a new {@link WeakOrderQueue} or return {@code null} if not possible.
         */
        private WeakOrderQueue newWeakOrderQueue(Thread thread) {
            // this 是 item 所属的栈、thread 是当前线程 (异线程)
            return WeakOrderQueue.newQueue(this, thread);
        }

        /**
         * 为了防止回收对象太多导致 Stack 的容量激增, 每次回收时调用 dropHandle 控制回收频率
         */
        boolean dropHandle(DefaultHandle<?> handle) {
            // 主要靠 hasBeenRecycled 和 handleRecycleCount 两个变量控制回收的频率
            // 从 8 个未被收回的对象中选取一个进行回收, 其它的都被丢弃掉
            if (!handle.hasBeenRecycled) {
                if (handleRecycleCount < interval) {
                    handleRecycleCount++;
                    // Drop the object.
                    return true;
                }
                handleRecycleCount = 0;
                handle.hasBeenRecycled = true;
            }
            return false;
        }

        DefaultHandle<T> newHandle() {
            return new DefaultHandle<T>(this);
        }
    }
}

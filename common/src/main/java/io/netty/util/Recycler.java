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
import io.netty.util.concurrent.FastThreadLocalThread;
import io.netty.util.internal.ObjectPool;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.UnstableApi;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import org.jctools.queues.MessagePassingQueue;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static io.netty.util.internal.PlatformDependent.newMpscQueue;
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
    private static final EnhancedHandle<?> NOOP_HANDLE = new EnhancedHandle<Object>() {
        @Override
        public void recycle(Object object) {
            // NOOP
        }

        @Override
        public void unguardedRecycle(final Object object) {
            // NOOP
        }

        @Override
        public String toString() {
            return "NOOP_HANDLE";
        }
    };
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
     * 每个线程队列块大小, 默认 32
     */
    private static final int DEFAULT_QUEUE_CHUNK_SIZE_PER_THREAD;
    /**
     * 是否使用阻塞队列, 默认 false
     */
    private static final boolean BLOCKING_POOL;
    /**
     * 只在 FastThreadLocalThread 中使用批量处理, 默认 true
     */
    private static final boolean BATCH_FAST_TL_ONLY;

    static {
        // In the future, we might have different maxCapacity for different object types.
        // e.g. io.netty.recycler.maxCapacity.writeTask
        //      io.netty.recycler.maxCapacity.outboundBuffer
        int maxCapacityPerThread = SystemPropertyUtil.getInt("io.netty.recycler.maxCapacityPerThread",
                SystemPropertyUtil.getInt("io.netty.recycler.maxCapacity", DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD));
        if (maxCapacityPerThread < 0) {
            maxCapacityPerThread = DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD;
        }

        DEFAULT_MAX_CAPACITY_PER_THREAD = maxCapacityPerThread;
        DEFAULT_QUEUE_CHUNK_SIZE_PER_THREAD = SystemPropertyUtil.getInt("io.netty.recycler.chunkSize", 32);

        // By default, we allow one push to a Recycler for each 8th try on handles that were never recycled before.
        // This should help to slowly increase the capacity of the recycler while not be too sensitive to allocation
        // bursts.
        RATIO = max(0, SystemPropertyUtil.getInt("io.netty.recycler.ratio", 8));

        BLOCKING_POOL = SystemPropertyUtil.getBoolean("io.netty.recycler.blocking", false);
        BATCH_FAST_TL_ONLY = SystemPropertyUtil.getBoolean("io.netty.recycler.batchFastThreadLocalOnly", true);

        if (logger.isDebugEnabled()) {
            if (DEFAULT_MAX_CAPACITY_PER_THREAD == 0) {
                logger.debug("-Dio.netty.recycler.maxCapacityPerThread: disabled");
                logger.debug("-Dio.netty.recycler.ratio: disabled");
                logger.debug("-Dio.netty.recycler.chunkSize: disabled");
                logger.debug("-Dio.netty.recycler.blocking: disabled");
                logger.debug("-Dio.netty.recycler.batchFastThreadLocalOnly: disabled");
            } else {
                logger.debug("-Dio.netty.recycler.maxCapacityPerThread: {}", DEFAULT_MAX_CAPACITY_PER_THREAD);
                logger.debug("-Dio.netty.recycler.ratio: {}", RATIO);
                logger.debug("-Dio.netty.recycler.chunkSize: {}", DEFAULT_QUEUE_CHUNK_SIZE_PER_THREAD);
                logger.debug("-Dio.netty.recycler.blocking: {}", BLOCKING_POOL);
                logger.debug("-Dio.netty.recycler.batchFastThreadLocalOnly: {}", BATCH_FAST_TL_ONLY);
            }
        }
    }

    // =================================================================================================================

    /**
     * 创建线程本地对象池的最大容量, 默认 4K
     */
    private final int maxCapacityPerThread;
    /**
     * 创建线程回收对象时的回收比例, 默认 8
     */
    private final int interval;
    /**
     * 每个线程队列块大小, 默认 32
     */
    private final int chunkSize;

    /**
     * 用于访问每个线程私有的本地对象池
     * <br>
     * (LocalPool) FastThreadLocalThread.InternalThreadLocalMap.indexedVariables[FastThreadLocal.index]
     */
    private final FastThreadLocal<LocalPool<T>> threadLocal = new FastThreadLocal<LocalPool<T>>() {
        @Override
        protected LocalPool<T> initialValue() {
            return new LocalPool<T>(maxCapacityPerThread, interval, chunkSize);
        }

        @Override
        protected void onRemoval(LocalPool<T> value) throws Exception {
            super.onRemoval(value);
            MessagePassingQueue<DefaultHandle<T>> handles = value.pooledHandles;
            value.pooledHandles = null;
            value.owner = null;
            handles.clear();
        }
    };

    protected Recycler() {
        this(DEFAULT_MAX_CAPACITY_PER_THREAD);
    }

    protected Recycler(int maxCapacityPerThread) {
        this(maxCapacityPerThread, RATIO, DEFAULT_QUEUE_CHUNK_SIZE_PER_THREAD);
    }

    /**
     * @deprecated Use one of the following instead:
     * {@link #Recycler()}, {@link #Recycler(int)}, {@link #Recycler(int, int, int)}.
     */
    @Deprecated
    @SuppressWarnings("unused") // Parameters we can't remove due to compatibility.
    protected Recycler(int maxCapacityPerThread, int maxSharedCapacityFactor) {
        this(maxCapacityPerThread, RATIO, DEFAULT_QUEUE_CHUNK_SIZE_PER_THREAD);
    }

    /**
     * @deprecated Use one of the following instead:
     * {@link #Recycler()}, {@link #Recycler(int)}, {@link #Recycler(int, int, int)}.
     */
    @Deprecated
    @SuppressWarnings("unused") // Parameters we can't remove due to compatibility.
    protected Recycler(int maxCapacityPerThread, int maxSharedCapacityFactor,
                       int ratio, int maxDelayedQueuesPerThread) {
        this(maxCapacityPerThread, ratio, DEFAULT_QUEUE_CHUNK_SIZE_PER_THREAD);
    }

    /**
     * @deprecated Use one of the following instead:
     * {@link #Recycler()}, {@link #Recycler(int)}, {@link #Recycler(int, int, int)}.
     */
    @Deprecated
    @SuppressWarnings("unused") // Parameters we can't remove due to compatibility.
    protected Recycler(int maxCapacityPerThread, int maxSharedCapacityFactor,
                       int ratio, int maxDelayedQueuesPerThread, int delayedQueueRatio) {
        this(maxCapacityPerThread, ratio, DEFAULT_QUEUE_CHUNK_SIZE_PER_THREAD);
    }

    protected Recycler(int maxCapacityPerThread, int ratio, int chunkSize) {
        interval = max(0, ratio);
        if (maxCapacityPerThread <= 0) {
            this.maxCapacityPerThread = 0;
            this.chunkSize = 0;
        } else {
            this.maxCapacityPerThread = max(4, maxCapacityPerThread);
            this.chunkSize = max(2, min(chunkSize, this.maxCapacityPerThread >> 1));
        }
    }

    // =================================================================================================================

    /**
     * 入口一: 获取池化对象
     */
    @SuppressWarnings("unchecked")
    public final T get() {
        // 禁用池化或虚拟线程, 直接创建对象
        if (maxCapacityPerThread == 0 || PlatformDependent.isVirtualThread(Thread.currentThread())) {
            return newObject((Handle<T>) NOOP_HANDLE);
        }
        // 取得线程本地对象池
        LocalPool<T> localPool = threadLocal.get();
        DefaultHandle<T> handle = localPool.claim();
        T obj;
        if (handle == null) {
            // 按比例决定是否创建新 Handle (该对象是否参与池化)
            handle = localPool.newHandle();
            if (handle != null) {
                // 创建新对象并绑定到 Handle
                obj = newObject(handle);
                handle.set(obj);
            } else {
                // 触发比例限制, 使用 NOOP_HANDLE
                obj = newObject((Handle<T>) NOOP_HANDLE);
            }
        } else {
            // 直接复用已有对象
            obj = handle.get();
        }

        return obj;
    }

    /**
     * @deprecated use {@link Handle#recycle(Object)}.
     */
    @Deprecated
    public final boolean recycle(T o, Handle<T> handle) {
        if (handle == NOOP_HANDLE) {
            return false;
        }

        handle.recycle(o);
        return true;
    }

    /**
     * 本地对象池大小
     * <br>
     * FastThreadLocalThread.InternalThreadLocalMap.indexedVariables[FastThreadLocal.index].size
     */
    @VisibleForTesting
    final int threadLocalSize() {
        // 虚拟线程不使用池化
        if (PlatformDependent.isVirtualThread(Thread.currentThread())) {
            return 0;
        }
        // 只在已初始化时返回统计值
        LocalPool<T> localPool = threadLocal.getIfExists();
        return localPool == null ? 0 : localPool.pooledHandles.size() + localPool.batch.size();
    }

    /**
     * @param handle can NOT be null.
     */
    protected abstract T newObject(Handle<T> handle);

    @SuppressWarnings("ClassNameSameAsAncestorName") // Can't change this due to compatibility.
    public interface Handle<T> extends ObjectPool.Handle<T>  { }

    // =================================================================================================================

    @UnstableApi
    public abstract static class EnhancedHandle<T> implements Handle<T> {

        /**
         * 允许跳过并发防护的回收
         */
        public abstract void unguardedRecycle(Object object);

        private EnhancedHandle() {
        }
    }

    private static final class DefaultHandle<T> extends EnhancedHandle<T> {
        /**
         * 已使用
         */
        private static final int STATE_CLAIMED = 0;
        /**
         * 已回收
         */
        private static final int STATE_AVAILABLE = 1;
        /**
         * 对象状态更新器
         */
        private static final AtomicIntegerFieldUpdater<DefaultHandle<?>> STATE_UPDATER;
        static {
            AtomicIntegerFieldUpdater<?> updater = AtomicIntegerFieldUpdater.newUpdater(DefaultHandle.class, "state");
            //noinspection unchecked
            STATE_UPDATER = (AtomicIntegerFieldUpdater<DefaultHandle<?>>) updater;
        }

        /**
         * 对象状态
         */
        private volatile int state; // State is initialised to STATE_CLAIMED (aka. 0) so they can be released.
        /**
         * 对象所属的对象池
         */
        private final LocalPool<T> localPool;
        /**
         * 池化对象
         */
        private T value;

        DefaultHandle(LocalPool<T> localPool) {
            this.localPool = localPool;
        }

        /**
         * 入口二: 回收对象 object
         */
        @Override
        public void recycle(Object object) {
            if (object != value) {
                throw new IllegalArgumentException("object does not belong to handle");
            }
            localPool.release(this, true);
        }

        /**
         * 入口二: 回收对象 object
         */
        @Override
        public void unguardedRecycle(Object object) {
            if (object != value) {
                throw new IllegalArgumentException("object does not belong to handle");
            }
            localPool.release(this, false);
        }

        T get() {
            return value;
        }

        void set(T value) {
            this.value = value;
        }

        /**
         * 已回收 -> 已使用
         */
        void toClaimed() {
            // 保证有序性
            assert state == STATE_AVAILABLE;
            STATE_UPDATER.lazySet(this, STATE_CLAIMED);
        }

        /**
         * 已使用 -> 已回收
         */
        void toAvailable() {
            // volatile 语义写入
            int prev = STATE_UPDATER.getAndSet(this, STATE_AVAILABLE);
            if (prev == STATE_AVAILABLE) {
                throw new IllegalStateException("Object has been recycled already.");
            }
        }

        /**
         * 已使用 -> 已回收
         */
        void unguardedToAvailable() {
            // 保证有序性
            int prev = state;
            if (prev == STATE_AVAILABLE) {
                throw new IllegalStateException("Object has been recycled already.");
            }
            STATE_UPDATER.lazySet(this, STATE_AVAILABLE);
        }
    }

    private static final class LocalPool<T> implements MessagePassingQueue.Consumer<DefaultHandle<T>> {
        /**
         * 创建线程回收对象时的回收比例, 默认 8
         */
        private final int ratioInterval;
        /**
         * 回收对象计数, 与 ratioInterval 配合, 控制回收速率, 新创建的对象为 ratioInterval
         */
        private int ratioCounter;

        /**
         * 每个线程队列块大小, 默认 32
         */
        private final int chunkSize;
        /**
         * 双端队列
         */
        private final ArrayDeque<DefaultHandle<T>> batch;

        /**
         * 对象池所属线程
         */
        private volatile Thread owner;
        /**
         * 对象池
         */
        private volatile MessagePassingQueue<DefaultHandle<T>> pooledHandles;

        @SuppressWarnings("unchecked")
        LocalPool(int maxCapacity, int ratioInterval, int chunkSize) {
            this.ratioInterval = ratioInterval;
            this.chunkSize = chunkSize;
            batch = new ArrayDeque<DefaultHandle<T>>(chunkSize);
            /*
             * 如果开启了 BATCH_FAST_TL_ONLY + 当前线程不是 FastThreadLocalThread, 那么 owner = null
             * 1. get()     不受影响, 仍然可以 batch
             * 2. recycle() 时不允许批量处理, 直接放入共享队列
             */
            Thread currentThread = Thread.currentThread();
            owner = !BATCH_FAST_TL_ONLY || currentThread instanceof FastThreadLocalThread ? currentThread : null;
            // 选择队列实现
            if (BLOCKING_POOL) {
                pooledHandles = new BlockingMessageQueue<DefaultHandle<T>>(maxCapacity);
            } else {
                pooledHandles = (MessagePassingQueue<DefaultHandle<T>>) newMpscQueue(chunkSize, maxCapacity);
            }
            ratioCounter = ratioInterval; // Start at interval so the first one will be recycled.
        }

        /**
         * 获取对象
         */
        DefaultHandle<T> claim() {
            MessagePassingQueue<DefaultHandle<T>> handles = pooledHandles;
            if (handles == null) {
                return null;
            }
            // 本地批量为空时, 从共享队列拉取
            if (batch.isEmpty()) {
                handles.drain(this, chunkSize); // 批处理
            }
            DefaultHandle<T> handle = batch.pollLast();
            if (null != handle) {
                handle.toClaimed(); // 领取后切换为已使用
            }
            return handle;
        }

        /**
         * 回收对象
         */
        void release(DefaultHandle<T> handle, boolean guarded) {
            // 更新状态, guarded 决定是否强保护
            if (guarded) {
                handle.toAvailable();
            } else {
                handle.unguardedToAvailable();
            }
            Thread owner = this.owner;
            if (owner != null && Thread.currentThread() == owner && batch.size() < chunkSize) {
                // 创建线程回收, 且 batch 未超过 chunkSize
                accept(handle);
            } else if (owner != null && isTerminated(owner)) {
                // 创建线程已终止
                this.owner = null;
                pooledHandles = null;
            } else {
                // owner == null || 创建线程回收但 batch 已满 || 跨线程回收
                MessagePassingQueue<DefaultHandle<T>> handles = pooledHandles;
                if (handles != null) {
                    handles.relaxedOffer(handle);
                }
            }
        }

        private static boolean isTerminated(Thread owner) {
            // Do not use `Thread.getState()` in J9 JVM because it's known to have a performance issue.
            // See: https://github.com/netty/netty/issues/13347#issuecomment-1518537895
            return PlatformDependent.isJ9Jvm() ? !owner.isAlive() : owner.getState() == Thread.State.TERMINATED;
        }

        /**
         * 每 8 次调用才真正创建 1 个 Handle 对象
         */
        DefaultHandle<T> newHandle() {
            /*
             * ratioInterval  ratioCounter  new
             * 8              8             ✅  first time
             * 8              0             ❌
             * 8              1             ❌
             * 8              2             ❌
             * 8              3             ❌
             * 8              4             ❌
             * 8              5             ❌
             * 8              6             ❌
             * 8              7             ✅  second time
             */
            if (++ratioCounter >= ratioInterval) {
                ratioCounter = 0;
                return new DefaultHandle<T>(this);
            }
            return null;
        }

        @Override
        public void accept(DefaultHandle<T> e) {
            batch.addLast(e); // 加入本地批量
        }
    }

    /**
     * This is an implementation of {@link MessagePassingQueue}, similar to what might be returned from
     * {@link PlatformDependent#newMpscQueue(int)}, but intended to be used for debugging purpose.
     * The implementation relies on synchronised monitor locks for thread-safety.
     * The {@code fill} bulk operation is not supported by this implementation.
     */
    private static final class BlockingMessageQueue<T> implements MessagePassingQueue<T> {
        /**
         * 双端队列
         */
        private final Queue<T> deque;
        /**
         * 创建线程本地对象池的最大容量, 默认 4K
         */
        private final int maxCapacity;

        BlockingMessageQueue(int maxCapacity) {
            this.maxCapacity = maxCapacity;
            // This message passing queue is backed by an ArrayDeque instance,
            // made thread-safe by synchronising on `this` BlockingMessageQueue instance.
            // Why ArrayDeque?
            // We use ArrayDeque instead of LinkedList or LinkedBlockingQueue because it's more space efficient.
            // We use ArrayDeque instead of ArrayList because we need the queue APIs.
            // We use ArrayDeque instead of ConcurrentLinkedQueue because CLQ is unbounded and has O(n) size().
            // We use ArrayDeque instead of ArrayBlockingQueue because ABQ allocates its max capacity up-front,
            // and these queues will usually have large capacities, in potentially great numbers (one per thread),
            // but often only have comparatively few items in them.
            deque = new ArrayDeque<T>();
        }

        @Override
        public synchronized boolean offer(T e) {
            if (deque.size() == maxCapacity) {
                return false;
            }
            return deque.offer(e);
        }

        @Override
        public synchronized T poll() {
            return deque.poll();
        }

        @Override
        public synchronized T peek() {
            return deque.peek();
        }

        @Override
        public synchronized int size() {
            return deque.size();
        }

        @Override
        public synchronized void clear() {
            deque.clear();
        }

        @Override
        public synchronized boolean isEmpty() {
            return deque.isEmpty();
        }

        @Override
        public int capacity() {
            return maxCapacity;
        }

        @Override
        public boolean relaxedOffer(T e) {
            return offer(e);
        }

        @Override
        public T relaxedPoll() {
            return poll();
        }

        @Override
        public T relaxedPeek() {
            return peek();
        }

        @Override
        public int drain(Consumer<T> c, int limit) {
            T obj;
            int i = 0;
            for (; i < limit && (obj = poll()) != null; i++) {
                c.accept(obj);
            }
            return i;
        }

        @Override
        public int fill(Supplier<T> s, int limit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int drain(Consumer<T> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int fill(Supplier<T> s) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void drain(Consumer<T> c, WaitStrategy wait, ExitCondition exit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void fill(Supplier<T> s, WaitStrategy wait, ExitCondition exit) {
            throw new UnsupportedOperationException();
        }
    }
}

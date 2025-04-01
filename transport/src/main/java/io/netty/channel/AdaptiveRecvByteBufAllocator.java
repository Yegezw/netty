/*
 * Copyright 2012 The Netty Project
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
package io.netty.channel;

import java.util.ArrayList;
import java.util.List;

import static io.netty.util.internal.ObjectUtil.checkPositive;
import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * The {@link RecvByteBufAllocator} that automatically increases and
 * decreases the predicted buffer size on feed back.
 * <p>
 * It gradually increases the expected number of readable bytes if the previous
 * read fully filled the allocated buffer.  It gradually decreases the expected
 * number of readable bytes if the read operation was not able to fill a certain
 * amount of the allocated buffer two times consecutively.  Otherwise, it keeps
 * returning the same prediction.
 */
public class AdaptiveRecvByteBufAllocator extends DefaultMaxMessagesRecvByteBufAllocator {
    /**
     * ByteBuf 最小容量
     */
    static final int DEFAULT_MINIMUM = 64;
    // Use an initial value that is bigger than the common MTU of 1500
    /**
     * ByteBuf 初始容量
     */
    static final int DEFAULT_INITIAL = 2048; // 使用比常用的 MTU(IP 层 1500) 更大的初始值
    /**
     * ByteBuf 最大容量
     */
    static final int DEFAULT_MAXIMUM = 65536;

    /**
     * 扩容步长
     */
    private static final int INDEX_INCREMENT = 4;
    /**
     * 缩容步长
     */
    private static final int INDEX_DECREMENT = 1;

    /**
     * RecvBuf 分配容量表(扩缩容索引表), 按照表中记录的容量大小进行扩缩容
     */
    private static final int[] SIZE_TABLE;

    static {
        // 初始化 RecvBuf 容量分配表
        List<Integer> sizeTable = new ArrayList<Integer>();
        // 当分配容量小于 512 时, 扩容单位为 16 递增
        for (int i = 16; i < 512; i += 16) {
            sizeTable.add(i);
        }

        // 当分配容量大于 512 时, 扩容单位为一倍
        // Suppress a warning since i becomes negative when an integer overflow happens
        for (int i = 512; i > 0; i <<= 1) { // lgtm[java/constant-comparison]
            sizeTable.add(i);
        }

        // 初始化 RecvBuf 扩缩容索引表
        SIZE_TABLE = new int[sizeTable.size()];
        for (int i = 0; i < SIZE_TABLE.length; i ++) {
            SIZE_TABLE[i] = sizeTable.get(i);
        }
    }

    /**
     * @deprecated There is state for {@link #maxMessagesPerRead()} which is typically based upon channel type.
     */
    @Deprecated
    public static final AdaptiveRecvByteBufAllocator DEFAULT = new AdaptiveRecvByteBufAllocator();

    private static int getSizeTableIndex(final int size) {
        for (int low = 0, high = SIZE_TABLE.length - 1;;) {
            if (high < low) {
                return low;
            }
            if (high == low) {
                return high;
            }

            int mid = low + high >>> 1; // 无符号右移, 高位始终补 0
            int a = SIZE_TABLE[mid];
            int b = SIZE_TABLE[mid + 1];
            if (size > b) {
                low = mid + 1;
            } else if (size < a) {
                high = mid - 1;
            } else if (size == a) {
                return mid;
            } else {
                return mid + 1;
            }
        }
    }

    private final class HandleImpl extends MaxMessageHandle {
        /**
         * ByteBuf 最小容量在 SIZE_TABLE 中的 index
         */
        private final int minIndex;
        /**
         * ByteBuf 最大容量在 SIZE_TABLE 中的 index
         */
        private final int maxIndex;

        /**
         * ByteBuf 当前容量在 SIZE_TABLE 中的 index
         */
        private int index;

        /**
         * 预计下一次分配 buffer 的容量, 初始 2048
         */
        private int nextReceiveBufferSize;
        /**
         * 是否缩容, 初始 false
         */
        private boolean decreaseNow;

        HandleImpl(int minIndex, int maxIndex, int initial) {
            this.minIndex = minIndex;
            this.maxIndex = maxIndex;

            // 在 SIZE_TABLE 中二分查找 >= initial 的最小容量索引 33
            index = getSizeTableIndex(initial);
            nextReceiveBufferSize = SIZE_TABLE[index]; // 2048
        }

        @Override
        public void lastBytesRead(int bytes) {
            // If we read as much as we asked for we should check if we need to ramp up the size of our next guess.
            // This helps adjust more quickly when large amounts of data is pending and can avoid going back to
            // the selector to check for more data. Going back to the selector can add significant latency for large
            // data transfers.
            if (bytes == attemptedBytesRead()) {
                record(bytes);
            }
            super.lastBytesRead(bytes);
        }

        @Override
        public int guess() {
            return nextReceiveBufferSize;
        }

        private void record(int actualReadBytes) {
            // 缩容
            if (actualReadBytes <= SIZE_TABLE[max(0, index - INDEX_DECREMENT)]) {
                // 需要满足两次缩容条件才会进行缩容, 且缩容步长为 1, 比较谨慎
                if (decreaseNow) {
                    index = max(index - INDEX_DECREMENT, minIndex);
                    nextReceiveBufferSize = SIZE_TABLE[index];
                    decreaseNow = false;
                } else {
                    decreaseNow = true;
                }
            }
            // 扩容
            else if (actualReadBytes >= nextReceiveBufferSize) {
                // 满足一次扩容条件就进行扩容, 且扩容步长为 4, 比较奔放
                index = min(index + INDEX_INCREMENT, maxIndex);
                nextReceiveBufferSize = SIZE_TABLE[index];
                decreaseNow = false;
            }
        }

        @Override
        public void readComplete() {
            // 是否对 RecvBuf 进行扩容缩容
            record(totalBytesRead());
        }
    }

    /**
     * ByteBuf 最小容量在 SIZE_TABLE 中的 index
     */
    private final int minIndex;
    /**
     * ByteBuf 最大容量在 SIZE_TABLE 中的 index
     */
    private final int maxIndex;
    /**
     * ByteBuf 初始容量
     */
    private final int initial;

    /**
     * Creates a new predictor with the default parameters.  With the default
     * parameters, the expected buffer size starts from {@code 1024}, does not
     * go down below {@code 64}, and does not go up above {@code 65536}.
     */
    public AdaptiveRecvByteBufAllocator() {
        this(DEFAULT_MINIMUM, DEFAULT_INITIAL, DEFAULT_MAXIMUM);
    }

    /**
     * Creates a new predictor with the specified parameters.
     *
     * @param minimum  the inclusive lower bound of the expected buffer size
     * @param initial  the initial buffer size when no feed back was received
     * @param maximum  the inclusive upper bound of the expected buffer size
     */
    public AdaptiveRecvByteBufAllocator(int minimum, int initial, int maximum) {
        checkPositive(minimum, "minimum");
        if (initial < minimum) {
            throw new IllegalArgumentException("initial: " + initial);
        }
        if (maximum < initial) {
            throw new IllegalArgumentException("maximum: " + maximum);
        }

        // 在 SIZE_TABLE 中二分查找 >= minimum 的最小容量索引 3
        int minIndex = getSizeTableIndex(minimum);
        if (SIZE_TABLE[minIndex] < minimum) {
            this.minIndex = minIndex + 1;
        } else {
            this.minIndex = minIndex;
        }

        // 在 SIZE_TABLE 中二分查找 <= maximum 的最大容量索引 38
        int maxIndex = getSizeTableIndex(maximum);
        if (SIZE_TABLE[maxIndex] > maximum) {
            this.maxIndex = maxIndex - 1;
        } else {
            this.maxIndex = maxIndex;
        }

        this.initial = initial;
    }

    @SuppressWarnings("deprecation")
    @Override
    public Handle newHandle() {
        return new HandleImpl(minIndex, maxIndex, initial);
    }

    @Override
    public AdaptiveRecvByteBufAllocator respectMaybeMoreData(boolean respectMaybeMoreData) {
        super.respectMaybeMoreData(respectMaybeMoreData);
        return this;
    }
}

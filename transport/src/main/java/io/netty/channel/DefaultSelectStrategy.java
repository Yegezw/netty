/*
 * Copyright 2016 The Netty Project
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

import io.netty.util.IntSupplier;

/**
 * Default select strategy.
 */
final class DefaultSelectStrategy implements SelectStrategy {
    static final SelectStrategy INSTANCE = new DefaultSelectStrategy();

    private DefaultSelectStrategy() { }

    @Override
    public int calculateStrategy(IntSupplier selectSupplier, boolean hasTasks) throws Exception {
        /*
         * Reactor 线程要保证及时的执行异步任务
         * 1、有异步任务, 马上执行 selectNow() 非阻塞轮询一次 IO 就绪事件
         * 2、无异步任务, 跳到 switch select 分支
         *
         * 结果
         * 1、无异步任务 -1
         * 2、有异步任务 + 有 IO 就绪事件 = 大于 0
         * 3、有异步任务 + 无 IO 就绪事件 = 等于 0
         */
        return hasTasks ? selectSupplier.get() : SelectStrategy.SELECT;
    }
}

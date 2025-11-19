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
package io.netty.channel.nio;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.FileRegion;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.internal.ChannelUtils;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.channel.socket.ChannelInputShutdownReadComplete;
import io.netty.channel.socket.SocketChannelConfig;
import io.netty.util.internal.StringUtil;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;

import static io.netty.channel.internal.ChannelUtils.WRITE_STATUS_SNDBUF_FULL;

/**
 * {@link AbstractNioChannel} base class for {@link Channel}s that operate on bytes.
 */
public abstract class AbstractNioByteChannel extends AbstractNioChannel {
    private static final ChannelMetadata METADATA = new ChannelMetadata(false, 16);
    private static final String EXPECTED_TYPES =
            " (expected: " + StringUtil.simpleClassName(ByteBuf.class) + ", " +
            StringUtil.simpleClassName(FileRegion.class) + ')';

    private final Runnable flushTask = new Runnable() {
        @Override
        public void run() {
            // Calling flush0 directly to ensure we not try to flush messages that were added via write(...) in the
            // meantime.
            ((AbstractNioUnsafe) unsafe()).flush0();
        }
    };
    /**
     * 表示 Input 已经 shutdown 了, 再次对 channel 进行读取返回 -1 设置该标志
     */
    private boolean inputClosedSeenErrorOnRead;

    /**
     * Create a new instance
     *
     * @param parent            the parent {@link Channel} by which this instance was created. May be {@code null}
     * @param ch                the underlying {@link SelectableChannel} on which it operates
     */
    protected AbstractNioByteChannel(Channel parent, SelectableChannel ch) {
        // 父类 AbstractNioChannel 中保存
        // JDK NIO 原生 SocketChannel 以及要监听的事件 OP_READ
        super(parent, ch, SelectionKey.OP_READ);
    }

    /**
     * Shutdown the input side of the channel.
     */
    protected abstract ChannelFuture shutdownInput();

    protected boolean isInputShutdown0() {
        return false;
    }

    @Override
    protected AbstractNioUnsafe newUnsafe() {
        return new NioByteUnsafe();
    }

    @Override
    public ChannelMetadata metadata() {
        return METADATA;
    }

    final boolean shouldBreakReadReady(ChannelConfig config) {
        return isInputShutdown0() && (inputClosedSeenErrorOnRead || !isAllowHalfClosure(config));
    }

    private static boolean isAllowHalfClosure(ChannelConfig config) {
        return config instanceof SocketChannelConfig &&
                ((SocketChannelConfig) config).isAllowHalfClosure();
    }

    protected class NioByteUnsafe extends AbstractNioUnsafe {

        private void closeOnRead(ChannelPipeline pipeline) {
            // 判断服务端 channel 接收方向是否关闭, 这里肯定是没有关闭的
            if (!isInputShutdown0()) {
                // 可通过 ServerBootstrap.childOption(ChannelOption.ALLOW_HALF_CLOSURE, true) 开启半关闭的支持
                if (isAllowHalfClosure(config())) {
                    // 半关闭处理流程
                    // 1、关闭服务端 Channel 的读通道
                    // 如果此时 Socket 接收缓冲区还有数据, 则会将这些数据统统丢弃
                    // 注意: 关闭读通道并不会向对端发送 FIN, 此时服务端连接依然处于 CLOSE_WAIT 状态
                    shutdownInput();
                    // 2、触发 UserEventTriggered(ChannelInputShutdownEvent) 事件
                    // 我们可以在 ChannelInputShutdownEvent 事件的回调方法中, 向客户端发送遗留的数据, 做到真正的优雅关闭
                    // 这里就是服务端处于 CLOSE_WAIT 状态, 在半关闭场景下, 可以继续向处于 FIN_WAIT2 状态下的客户端发送数据的地方
                    pipeline.fireUserEventTriggered(ChannelInputShutdownEvent.INSTANCE);
                } else {
                    // 如果不支持半关闭, 则服务端直接调用 close 向客户端发送 Fin, 结束 close_wait 进入 last_ack
                    close(voidPromise());
                }
            } else {
                // 在连接半关闭的情况下, JDK NIO Selector 会不停的通知 OP_READ 事件活跃, 所以 read loop 会一直不停的执行
                // 当 Reactor 处理完 ChannelInputShutdownEvent 之后, 由于 Selector 又会通知 OP_READ 事件活跃, 所以半关闭流程再一次来到了 closeOnRead 方法
                // 那么此时服务端的读通道已经关闭了 isInputShutdown0 == true, 所以流程来到 else 分支
                // 1、设置 inputClosedSeenErrorOnRead = true 表示此时 Channel 的读通道已经关闭了, 不能再继续响应 OP_READ 事件
                // 因为半关闭状态下, Selector 会不停的通知 OP_READ 事件, 如果不停无脑响应的话, 会造成极大的 CPU 资源浪费
                inputClosedSeenErrorOnRead = true;
                // 2、触发 UserEventTriggered(ChannelInputShutdownReadComplete) 事件
                // 此事件的触发标志着: 服务端在 CLOSE_WAIT 状态下, 已经将所有遗留的数据发送给了客户端
                // 服务端可以在该事件的回调中关闭 Channel, 结束 CLOSE_WAIT 进入 LAST_ACK 状态
                pipeline.fireUserEventTriggered(ChannelInputShutdownReadComplete.INSTANCE);
            }
        }

        private void handleReadException(ChannelPipeline pipeline, ByteBuf byteBuf, Throwable cause, boolean close,
                RecvByteBufAllocator.Handle allocHandle) {
            if (byteBuf != null) {
                if (byteBuf.isReadable()) {
                    readPending = false;
                    // 如果发生异常时, 已经读取到了部分数据, 则触发 ChannelRead 事件
                    pipeline.fireChannelRead(byteBuf);
                } else {
                    byteBuf.release();
                }
            }
            allocHandle.readComplete();
            // 随后触发 ChannelReadComplete 事件和 ExceptionCaught 事件
            pipeline.fireChannelReadComplete();
            pipeline.fireExceptionCaught(cause);

            // If oom will close the read event, release connection.
            // See https://github.com/netty/netty/issues/10434
            if (close || cause instanceof OutOfMemoryError || cause instanceof IOException) {
                closeOnRead(pipeline); // 关闭连接 + 取消 Channel 注册 + 触发 ChannelInactive 事件和 ChannelUnregistered 事件
            }
        }

        @Override
        public final void read() {
            // config 和 pipeline 都是 NioSocketChannel 的
            final ChannelConfig config = config();
            // 半关闭的状态下
            // 在没有调用 close 方法关闭 Channel 之前, JDK NIO Selector 会一直不停的通知 OP_READ 事件, 所以流程马上又会回到 OP_READ 事件的处理方法中
            // 那么这次我们就不能再响应 OP_READ 事件了, 需要调用 clearReadPending 方法将读事件从 Reactor 中取消掉, 停止对 OP_READ 事件的监听
            // 否则 Reactor 线程就会 "在半关闭期间内" 一直在这里空转, 导致 CPU 100%, shouldBreakReadReady() 判断在半关闭期间是否取消 OP_READ 事件的监听
            if (shouldBreakReadReady(config)) {
                clearReadPending();
                return;
            }
            final ChannelPipeline pipeline = pipeline();

            /*
             * AdaptiveRecvByteBufAllocator 只是负责动态调整 ByteBuf 的容量
             * 而具体为 ByteBuf 申请内存空间的由 PooledByteBufAllocator 负责
             */

            final ByteBufAllocator allocator = config.getAllocator();             // 用于分配 ByteBuf 的分配器 PooledByteBufAllocator
            final RecvByteBufAllocator.Handle allocHandle = recvBufAllocHandle(); // AdaptiveRecvByteBufAllocator#HandleImpl->MaxMessageHandle
            allocHandle.reset(config); // 重置清除上次的统计指标

            ByteBuf byteBuf = null;
            boolean close = false;
            try {
                do {
                    // 利用 PooledByteBufAllocator 分配合适大小的 ByteBuf, 初始大小为 2048
                    byteBuf = allocHandle.allocate(allocator);       // 装饰模式: 增强行为
                    // 当对方 TCP 异常关闭, 这里会接收到 RST 报文, 在读取 channel 中的数据时就会抛出 IOException 异常  
                    // 1、此时 Socket 接收缓冲区中只有 RST 报文, 并没有其它正常数据
                    // 2、Socket 接收缓冲区有正常的数据 + RST 报文
                    allocHandle.lastBytesRead(doReadBytes(byteBuf)); // 记录本次: 尝试读取字节数(ByteBuf 剩余可写字节数) + 实际读取字节数
                    // 如果本次没有读取到任何字节: 退出循环, 进行下一轮事件轮询
                    if (allocHandle.lastBytesRead() <= 0) {
                        // nothing was read. release the buffer.
                        byteBuf.release(); // 释放
                        byteBuf = null;
                        // 客户端主动关闭连接 close() 或者 shutdownOutput()
                        // 当客户端主动关闭连接时 (客户端发送 Fin), 会触发 read 就绪事件, 这里从 channel 读取的数据会是 -1
                        close = allocHandle.lastBytesRead() < 0;
                        if (close) {
                            // There is nothing left to read as we received an EOF.
                            readPending = false;
                        }
                        // 注意: 只会触发 ChannelReadComplete 事件而不会触发 ChannelRead 事件
                        break;
                    }

                    // read loop 读取数据次数 + 1
                    allocHandle.incMessagesRead(1);
                    readPending = false;
                    // 客户端 NioSocketChannel 的 pipeline 中触发 ChannelRead 事件
                    pipeline.fireChannelRead(byteBuf);
                    byteBuf = null; // 解除本次读取数据分配的 ByteBuffer 引用, 方便下一轮 read loop 分配
                } while (allocHandle.continueReading()); // 判断是否应该继续 read loop(16 && ByteBuf 是否满载而归)

                // 根据本次 read loop 总共读取的字节数, 决定下次是否扩容或者缩容
                allocHandle.readComplete();
                // 客户端 NioSocketChannel 的 pipeline 中触发 ChannelReadComplete 事件, 表示一次 OP_READ 事件处理完毕
                // 但这并不表示客户端发送来的数据已经全部读完, 因为如果数据太多的话, 这里只会读取 16 次, 剩下的会等到下次 OP_READ 事件到来后再处理
                pipeline.fireChannelReadComplete();

                // 此时客户端发送 Fin(Fin_wait_1) 主动关闭连接, 服务端接收到 Fin 并回复 ack 进入 close_wait
                // 在服务端进入 close_wait 状态后, 需要调用 close 方法向客户端发送 Fin, 服务端才能结束 close_wait 状态
                if (close) {
                    closeOnRead(pipeline); // 正常关闭
                }
            } catch (Throwable t) {
                // 接收到 RST 报文
                // 在调用 doReadBytes 方法从 Channel 中读取数据的时候会抛出 IOException 异常, 这里会有两种情况抛出异常
                // 1. 此时 Socket 接收缓冲区中只有 RST 包, 并没有其它正常数据
                // 2. Socket 接收缓冲区有正常的数据, OP_READ 事件活跃
                //    当调用 doReadBytes 方法从 Channel 中读取数据的过程中, 对端发送 RST 强制关闭连接, 这时会在读取的过程中抛出 IOException 异常
                handleReadException(pipeline, byteBuf, t, close, allocHandle); // 异常关闭
            } finally {
                // Check if there is a readPending which was not processed yet.
                // This could be for two reasons:
                // * The user called Channel.read() or ChannelHandlerContext.read() in channelRead(...) method
                // * The user called Channel.read() or ChannelHandlerContext.read() in channelReadComplete(...) method
                //
                // See https://github.com/netty/netty/issues/2254
                if (!readPending && !config.isAutoRead()) {
                    removeReadOp();
                }
            }
        }
    }

    /**
     * Write objects to the OS.
     * @param in the collection which contains objects to write.
     * @return The value that should be decremented from the write quantum which starts at
     * {@link ChannelConfig#getWriteSpinCount()}. The typical use cases are as follows:
     * <ul>
     *     <li>0 - if no write was attempted. This is appropriate if an empty {@link ByteBuf} (or other empty content)
     *     is encountered</li>
     *     <li>1 - if a single call to write data was made to the OS</li>
     *     <li>{@link ChannelUtils#WRITE_STATUS_SNDBUF_FULL} - if an attempt to write data was made to the OS, but no
     *     data was accepted</li>
     * </ul>
     * @throws Exception if an I/O exception occurs during write.
     */
    protected final int doWrite0(ChannelOutboundBuffer in) throws Exception {
        Object msg = in.current();
        if (msg == null) {
            // Directly return here so incompleteWrite(...) is not called.
            return 0;
        }
        return doWriteInternal(in, in.current()); // 注意返回值
    }

    private int doWriteInternal(ChannelOutboundBuffer in, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) msg;
            if (!buf.isReadable()) {
                in.remove();
                return 0;
            }

            final int localFlushedAmount = doWriteBytes(buf);
            if (localFlushedAmount > 0) {
                in.progress(localFlushedAmount);
                if (!buf.isReadable()) {
                    in.remove();
                }
                return 1;
            }
        } else if (msg instanceof FileRegion) {
            FileRegion region = (FileRegion) msg;
            // 文件已经传输完毕
            if (region.transferred() >= region.count()) {
                in.remove();
                return 0;
            }

            // 零拷贝的方式传输文件
            long localFlushedAmount = doWriteFileRegion(region);
            if (localFlushedAmount > 0) {
                in.progress(localFlushedAmount);
                if (region.transferred() >= region.count()) {
                    in.remove();
                }
                return 1;
            }
        } else {
            // Should not reach here.
            throw new Error();
        }
        // 走到这里表示: 此时 Socket 已经写不进去了, 退出 write Loop, 注册 OP_WRITE 事件
        return WRITE_STATUS_SNDBUF_FULL;
    }

    @Override
    protected void doWrite(ChannelOutboundBuffer in) throws Exception {
        int writeSpinCount = config().getWriteSpinCount();
        do {
            Object msg = in.current();
            if (msg == null) {
                // Wrote all messages.
                clearOpWrite();
                // Directly return here so incompleteWrite(...) is not called.
                return;
            }
            writeSpinCount -= doWriteInternal(in, msg);
        } while (writeSpinCount > 0);

        incompleteWrite(writeSpinCount < 0);
    }

    @Override
    protected final Object filterOutboundMessage(Object msg) {
        if (msg instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) msg;
            if (buf.isDirect()) {
                return msg;
            }

            return newDirectBuffer(buf);
        }

        if (msg instanceof FileRegion) {
            return msg;
        }

        throw new UnsupportedOperationException(
                "unsupported message type: " + StringUtil.simpleClassName(msg) + EXPECTED_TYPES);
    }

    protected final void incompleteWrite(boolean setOpWrite) {
        // Did not write completely.
        if (setOpWrite) {
            // 这里处理还没写满 16 次, 但是 socket 缓冲区已满写不进去的情况
            // 注册 OP_WRITE 事件, 什么时候 socket 可写了, epoll 会通知 Reactor 线程继续写
            setOpWrite(); // 向 Reactor 注册 OP_WRITE 事件
        } else {
            // It is possible that we have set the write OP, woken up by NIO because the socket is writable, and then
            // use our write quantum. In this case we no longer want to set the write OP because the socket is still
            // writable (as far as we know). We will find out next time we attempt to write if the socket is writable
            // and set the write OP if necessary.
            // 这里处理的是 socket 缓冲区依然可写, 但是写了 16 次还没写完, 这时就不能在写了, Reactor 线程需要处理其它 channel 上的 io 事件
            // 因为此时 socket 是可写的, 必须清除 OP_WRITE 事件, 否则会一直不停地被通知
            clearOpWrite();

            // Schedule flush again later so other tasks can be picked up in the meantime
            // 如果本次 write Loop 还没写完, 则提交 flushTask 到 Reactor
            // 释放 Sub Reactor 让其可以继续处理其它 Channel 上的 IO 事件
            eventLoop().execute(flushTask);
        }
    }

    /**
     * Write a {@link FileRegion}
     *
     * @param region        the {@link FileRegion} from which the bytes should be written
     * @return amount       the amount of written bytes
     */
    protected abstract long doWriteFileRegion(FileRegion region) throws Exception;

    /**
     * Read bytes into the given {@link ByteBuf} and return the amount.
     */
    protected abstract int doReadBytes(ByteBuf buf) throws Exception;

    /**
     * Write bytes form the given {@link ByteBuf} to the underlying {@link java.nio.channels.Channel}.
     * @param buf           the {@link ByteBuf} from which the bytes should be written
     * @return amount       the amount of written bytes
     */
    protected abstract int doWriteBytes(ByteBuf buf) throws Exception;

    protected final void setOpWrite() {
        final SelectionKey key = selectionKey();
        // Check first if the key is still valid as it may be canceled as part of the deregistration
        // from the EventLoop
        // See https://github.com/netty/netty/issues/2104
        if (!key.isValid()) {
            return;
        }
        final int interestOps = key.interestOps();
        if ((interestOps & SelectionKey.OP_WRITE) == 0) {
            key.interestOps(interestOps | SelectionKey.OP_WRITE);
        }
    }

    protected final void clearOpWrite() {
        final SelectionKey key = selectionKey();
        // Check first if the key is still valid as it may be canceled as part of the deregistration
        // from the EventLoop
        // See https://github.com/netty/netty/issues/2104
        if (!key.isValid()) {
            return;
        }
        final int interestOps = key.interestOps();
        if ((interestOps & SelectionKey.OP_WRITE) != 0) {
            key.interestOps(interestOps & ~SelectionKey.OP_WRITE);
        }
    }
}

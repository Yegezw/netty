/*
 * Copyright 2019 The Netty Project
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

import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.internal.PlatformDependent;

import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.WeakHashMap;

final class ChannelHandlerMask {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ChannelHandlerMask.class);

    /*
     * 当 netty 内核处理连接的接收, 以及数据的读取过程中, 如果发生异常: 会在整个 pipeline 中触发 exceptionCaught 事件的传播
     * 为什么要单独强调在 inbound 事件传播的过程中发生异常, 才会回调 exceptionCaught 呢 ?
     *
     * 因为 inbound 事件一般都是由 netty 内核触发传播的, 而 outbound 事件一般都是由用户选择触发的
     * 比如用户在处理完业务逻辑触发的 write 事件或者 flush 事件(AbstractChannelHandlerContext 内)
     * 在用户触发 outbound 事件后, 一般都会得到一个 ChannelPromise, 用户可以向 ChannelPromise 添加各种 listener
     * 当 outbound 事件在传播的过程中发生异常时, netty 会通知用户持有的这个 ChannelPromise, 但不会触发 exceptionCaught 的回调
     *
     * outbound 事件中只有 flush 事件的传播是个例外
     * 当 flush 事件在 pipeline 传播的过程中发生异常时, 会触发对应异常 ChannelHandler 的 exceptionCaught 事件回调
     * 因为 flush 方法的签名中不会给用户返回 ChannelPromise
     *
     * ExceptionCaught 事件和 Inbound 类事件一样都是在 pipeline 中从前往后开始传播
     * ExceptionCaught 事件的触发有两种情况
     * [1] netty 框架内部产生的异常, 这时 netty 会直接在 pipeline 中触发 ExceptionCaught 事件的传播
     *     异常事件会在 pipeline 中从 HeadContext 开始一直向后传播直到 TailContext
     *     比如 netty 在 NioByteUnsafe#read 中 read loop 读取数据时发生异常
     * [2] 当 Inbound 类事件或者 flush 事件在 pipeline 中传播的过程中
     *     在某个 ChannelHandler 中的事件回调方法处理中发生异常, 这时该 ChannelHandler 的 exceptionCaught 方法会被回调
     *     用户可以在这里处理异常事件, 并通过 "是否调用 ctx.fireExceptionCaught(cause)" 来决定 "是否继续向后" 传播异常事件
     *
     * ExceptionCaught 事件和 Inbound 类事件的区别
     * 在 Inbound 类事件传播过程中, 会查找下一个具有事件响应资格的 ChannelInboundHandler, 遇到 ChannelOutboundHandler 会直接跳过
     * 而 ExceptionCaught 事件无论是在哪种类型的 channelHandler 中触发的
     * 都会从当前异常 ChannelHandler 开始一直向后传播, ChannelInboundHandler And ChannelOutboundHandler 都可以响应该异常事件
     * 由于无论异常是在 ChannelInboundHandler 中产生的还是在 ChannelOutboundHandler 中产生的
     * exceptionCaught 事件都会在 pipeline 中从前向后传播, 并且不关心 ChannelHandler 的类型
     * 所以我们一般将负责统一异常处理的 ChannelHandler 放在 pipeline 的最后, 这样它对于 inbound 类异常和 outbound 类异常均可以捕获得到
     */
    // Using to mask which methods must be called for a ChannelHandler.
    static final int MASK_EXCEPTION_CAUGHT = 1;

    /*
     * 新建连接: handlerAdded -> ChannelRegistered -> ChannelActive
     * 读写数据: ChannelRead -> write -> ChannelReadComplete -> flush
     * 关闭连接: ChannelInactive -> ChannelUnregistered -> handlerRemoved
     * 异常关闭: ChannelRead (读到部分数据后, 读到 RST 异常) -> ChannelReadComplete -> ExceptionCaught -> ChannelInactive -> ChannelUnregistered -> handlerRemoved
     *
     * handlerAdded and handlerRemoved 可以做资源的初始化和释放工作
     * handlerAdded 事件会在 ChannelHandler 被添加到 pipeline 中时触发
     * handlerRemoved 事件会在 ChannelHandler 从 pipeline 中移除时触发
     *
     * channelActive and channelInActive 可以做连接个数的统计工作
     * channelActive 事件会在 Channel 处于活跃状态时触发, 比如客户端成功连接到服务端
     * channelInActive 事件会在 Channel 处于非活跃状态时触发, 比如客户端断开连接
     *
     * 可以在 ChannelRead 中调用 write 事件, 在 ChannelReadComplete 中调用 flush 事件
     */

    // -------------------------------------------------------------------------------------------------------------------------------------------------

    // ChannelRegistered -> ChannelActive -> ChannelRead -> ChannelReadComplete
    // ChannelWritabilityChanged -> UserEventTriggered -> ChannelInactive -> ChannelUnregistered

    /*
     * AbstractChannel#register0
     * NioServerSocketChannel 在向 Main Reactor 注册完成后
     * NioSocketChannel       在向 Sub  Reactor 注册完成后
     * 会触发 ChannelRegistered 事件, 从 HeadContext 开始, 依次在 pipeline 中向后传播
     */
    static final int MASK_CHANNEL_REGISTERED = 1 << 1;
    /*
     * 当 Channel 被关闭之后
     * 会在 pipeline 中先触发 ChannelInactive 事件的传, 再触发 ChannelUnregistered 事件的传播
     * 实现 ChannelInboundHandlerAdapter#channelInactive     响应 ChannelInactive     事件
     * 实现 ChannelInboundHandlerAdapter#channelUnregistered 响应 ChannelUnregistered 事件
     */
    static final int MASK_CHANNEL_UNREGISTERED = 1 << 2;
    /*
     * 1、NioServerSocketChannel 在与端口绑定成功后(AbstractBootstrap#doBind0 -> AbstractChannel#bind)
     * 2、NioSocketChannel 在向 Sub Reactor 注册完成后(AbstractChannel#register0)
     * 会触发 ChannelActive 事件, 从 HeadContext 开始, 依次在 pipeline 中向后传播
     * 并在 HeadContext 中通过 unsafe.beginRead()
     * NioServerSocketChannel 注册 OP_ACCEPT 事件到 Main Reactor 中
     * NioSocketChannel       注册 OP_READ   事件到 Sub  Reactor 中
     */
    static final int MASK_CHANNEL_ACTIVE = 1 << 3;
    /*
     * 当 Channel 被关闭之后
     * 会在 pipeline 中先触发 ChannelInactive 事件的传, 再触发 ChannelUnregistered 事件的传播
     * 实现 ChannelInboundHandlerAdapter#channelInactive     响应 ChannelInactive     事件
     * 实现 ChannelInboundHandlerAdapter#channelUnregistered 响应 ChannelUnregistered 事件
     */
    static final int MASK_CHANNEL_INACTIVE = 1 << 4;
    /*
     * NioEventLoop#processSelectedKey -> NioUnsafe#read
     * 1、NioMessageUnsafe#read -> HeadContext#channelRead -> ServerBootstrapAcceptor#channelRead
     * 2、NioByteUnsafe   #read -> UserDefined#channelRead
     */
    static final int MASK_CHANNEL_READ = 1 << 5;
    /*
     * NioEventLoop#processSelectedKey -> NioUnsafe#read
     * 1、NioMessageUnsafe#read
     * 2、NioByteUnsafe   #read
     * ChannelRead 事件中往往需要调用 ctx.write、可以在 ChannelReadComplete 事件中调用 ctx.flush 聚合刷出到 socket
     */
    static final int MASK_CHANNEL_READ_COMPLETE = 1 << 6;
    /*
     * Netty 提供了一种事件扩展机制, 可以允许用户自定义异步事件, 可以灵活实现各种复杂场景的处理机制
     * 从 currCtx 向后传播 ctx.fireUserEventTriggered(new Event())
     * 从 headCtx 向后传播 ctx.channel().pipeline().fireUserEventTriggered(new Event())
     * 实现 ChannelInboundHandlerAdapter#userEventTriggered 自定义事件的响应和处理
     */
    static final int MASK_USER_EVENT_TRIGGERED = 1 << 7;
    /*
     * 当处理完业务逻辑得到结果后, 会调用 ctx.write(msg) 触发 write 事件在 pipeline 中的传播, 直到 HeadContext#write
     * 最终 AbstractChannel#write 会将发送数据 msg 写入 NioSocketChannel 中的待发送缓冲队列 ChannelOutboundBuffer 中
     * 并等待用户调用 flush 操作从 ChannelOutboundBuffer 中将待发送数据 msg, 写入到底层 Socket 的发送缓冲区中
     *
     * 当用户端的接收处理速度非常慢 OR 网络状况极度拥塞时, 会导致 TCP 滑动窗口不断缩小, 进而导致发送端的发送速度也变得越来越小
     * 而此时用户还在不断的调用 ctx.write(msg), 就会导致 ChannelOutboundBuffer 会急剧增大, 从而可能导致 OOM
     *
     * ctx#write -> HeadContext#write -> AbstractUnsafe#write  -> ChannelOutboundBuffer#incrementPendingOutboundBytes
     * ctx#flush -> HeadContext#flush -> AbstractUnsafe#flush0 -> ChannelOutboundBuffer#decrementPendingOutboundBytes
     * Netty 引入了高低水位线来控制 ChannelOutboundBuffer 的内存占用 (WriteBufferWaterMark 32K ~ 64K)
     * 当 ChannelOutboundBuffer 中的内存占用量超过高水位线时, Netty 会将对应的 NioSocketChannel 置为不可写状态, 并在 pipeline 中触发 ChannelWritabilityChanged 事件
     * 当 ChannelOutboundBuffer 中的内存占用量低于低水位线时, Netty 会将对应的 NioSocketChannel 设置为可写状态, 并再次触发 ChannelWritabilityChanged 事件
     * 用户可在自定义的 ChannelHandler 中通过 ctx.channel().isWritable() 判断当前 channel 是否可写
     */
    static final int MASK_CHANNEL_WRITABILITY_CHANGED = 1 << 8;

    // -------------------------------------------------------------------------------------------------------------------------------------------------

    // ChannelRegistered -> [bind / connect] -> ChannelActive -> [read] -> ChannelRead -> ChannelReadComplete
    // [write] -> [flush] -> ChannelWritabilityChanged
    // UserEventTriggered -> [close / disconnect] -> ChannelInactive -> [deregister] -> ChannelUnregistered

    static final int MASK_BIND = 1 << 9;
    /*
     * ctx.connect(remoteAddress)
     * ctx.channel().connect(remoteAddress)
     * 实现 ChannelOutboundHandlerAdapter#connect 自定义事件的响应和处理
     * 最终 connect 事件会在 pipeline 中的头结点 headContext 中触发底层的连接建立请求
     * 当客户端成功连接到服务端之后, 会在客户端 NioSocketChannel 的 pipeline 中传播 channelActive 事件
     */
    static final int MASK_CONNECT = 1 << 10;
    /*
     * ctx.disconnect()
     * ctx.channel().disconnect()
     * 实现 ChannelOutboundHandlerAdapter#disconnect 自定义事件的响应和处理
     * 最终 disconnect 事件会传播到 HeadContext 中, 并在 HeadContext 中完成底层的断开连接操作
     * 当客户端断开连接成功关闭之后, 会在 pipeline 中先后触发 ChannelInactive 事件和 ChannelUnregistered 事件
     */
    static final int MASK_DISCONNECT = 1 << 11;
    /*
     * ctx.close()
     * ctx.channel().close()
     * 实现 ChannelOutboundHandlerAdapter#close 自定义事件的响应和处理
     * 最终 close 事件会在 pipeline 中一直向前传播直到头结点 HeadConnect 中, 并在 HeadContext 中完成连接关闭的操作
     * 当连接完成关闭之后, 会在 pipeline 中先后触发 ChannelInactive 事件和 ChannelUnregistered 事件
     */
    static final int MASK_CLOSE = 1 << 12;
    /*
     * ctx.deregister()
     * ctx.channel().deregister()
     * 实现 ChannelOutboundHandlerAdapter#deregister 自定义事件的响应和处理
     * 最终 deRegister 事件会传播至 pipeline 中的头结点 HeadContext 中, 并在 HeadContext 中完成底层 channel 取消注册的操作
     * 当 Channel 从 Reactor 上注销之后, 从此 Reactor 将不会再监听该 Channel 上的 IO 事件, 并触发 ChannelUnregistered 事件在 pipeline 中传播
     */
    static final int MASK_DEREGISTER = 1 << 13;
    /*
     * read 事件: 使 Channel 具备感知 IO 事件的能力
     * read 事件的触发: 当 Channel 需要向其对应的 Reactor 注册读类型事件时(OP_ACCEPT OR OP_READ)才会触发
     * read 事件的响应: 将 Channel 感兴趣的 IO 事件注册到对应的 Reactor 上
     * 1、NioServerSocketChannel -> OP_ACCEPT
     * 2、NioSocketChannel       -> OP_READ
     * 当 Channel 处于 Active 状态后, 会在 pipeline 中传播 ChannelActive 事件, HeadContext#readIfIsAutoRead 会触发 Read 事件的传播
     *
     * 当客户端发送数据量很大且频繁时, 为防止服务器 OOM, ctx.channel().config().setAutoRead(false) 将 autoRead 属性设置为 false
     * Netty 就会将 Channel 中感兴趣的读类型事件从 Reactor 中注销, 从此 Reactor 不会再对相应事件进行监听, 这样 Channel 就不会再读取数据了
     *
     * 当服务端的处理速度恢复正常, ctx.channel().config().setAutoRead(true) 将 autoRead 属性设置为 true
     * Netty 会在 pipeline 中触发 read 事件, 并在 HeadContext 中通过 unsafe.beginRead() 将 Channel 感兴趣的读类型事件重新注册到对应的 Reactor 中
     */
    static final int MASK_READ = 1 << 14;
    /*
     * write 事件和 flush 事件: 由用户在处理完业务请求, 得到业务结果后, 在业务线程中主动触发
     * 触发 writeAndFlush 后, write 事件首先会在 pipeline 中传播, 最后 flush 事件在 pipeline 中传播
     * 通过 Channel               触发: 从 pipeline 尾部节点 TailContext    开始一直向前传播直到 HeadContext
     * 通过 ChannelHandlerContext 触发: 在 pipeline 中从当前 ChannelHandler 开始一直向前传播直到 HeadContext
     *
     * Netty 对 write 事件的处理, 最终会将发送数据写入 Channel 对应的写缓冲队列 ChannelOutboundBuffer 中
     * 此时数据并没有发送出去, 而是在写缓冲队列中缓存，这也是 Netty 实现异步写的核心设计
     * 最终通过 flush 操作从 Channel 中的写缓冲队列 ChannelOutboundBuffer 中获取到待发送数据, 并写入到 Socket 的发送缓冲区中
     */
    static final int MASK_WRITE = 1 << 15;
    static final int MASK_FLUSH = 1 << 16;

    // -------------------------------------------------------------------------------------------------------------------------------------------------

    /**
     * inbound 事件的掩码集合
     */
    static final int MASK_ONLY_INBOUND =  MASK_CHANNEL_REGISTERED |
            MASK_CHANNEL_UNREGISTERED | MASK_CHANNEL_ACTIVE | MASK_CHANNEL_INACTIVE | MASK_CHANNEL_READ |
            MASK_CHANNEL_READ_COMPLETE | MASK_USER_EVENT_TRIGGERED | MASK_CHANNEL_WRITABILITY_CHANGED;
    private static final int MASK_ALL_INBOUND = MASK_EXCEPTION_CAUGHT | MASK_ONLY_INBOUND;

    /**
     * outbound 事件的掩码集合
     */
    static final int MASK_ONLY_OUTBOUND =  MASK_BIND | MASK_CONNECT | MASK_DISCONNECT |
            MASK_CLOSE | MASK_DEREGISTER | MASK_READ | MASK_WRITE | MASK_FLUSH;
    private static final int MASK_ALL_OUTBOUND = MASK_EXCEPTION_CAUGHT | MASK_ONLY_OUTBOUND;

    /**
     * ChannelHandler 类一旦被定义出来它的执行掩码就固定了, 可以缓存起来
     */
    private static final FastThreadLocal<Map<Class<? extends ChannelHandler>, Integer>> MASKS =
            new FastThreadLocal<Map<Class<? extends ChannelHandler>, Integer>>() {
                @Override
                protected Map<Class<? extends ChannelHandler>, Integer> initialValue() {
                    return new WeakHashMap<Class<? extends ChannelHandler>, Integer>(32);
                }
            };

    /**
     * Return the {@code executionMask}.
     */
    static int mask(Class<? extends ChannelHandler> clazz) {
        // Try to obtain the mask from the cache first. If this fails calculate it and put it in the cache for fast
        // lookup in the future.
        Map<Class<? extends ChannelHandler>, Integer> cache = MASKS.get();
        Integer mask = cache.get(clazz);
        if (mask == null) {
            mask = mask0(clazz);
            cache.put(clazz, mask);
        }
        return mask;
    }

    /**
     * Calculate the {@code executionMask}.
     */
    private static int mask0(Class<? extends ChannelHandler> handlerType) {
        int mask = MASK_EXCEPTION_CAUGHT;
        try {
            // ChannelHandler 如果标注了 @Skip 注解, 则认为 handler 对该事件不感兴趣

            // Inbound
            if (ChannelInboundHandler.class.isAssignableFrom(handlerType)) {
                mask |= MASK_ALL_INBOUND;

                if (isSkippable(handlerType, "channelRegistered", ChannelHandlerContext.class)) {
                    mask &= ~MASK_CHANNEL_REGISTERED;
                }
                if (isSkippable(handlerType, "channelUnregistered", ChannelHandlerContext.class)) {
                    mask &= ~MASK_CHANNEL_UNREGISTERED;
                }
                if (isSkippable(handlerType, "channelActive", ChannelHandlerContext.class)) {
                    mask &= ~MASK_CHANNEL_ACTIVE;
                }
                if (isSkippable(handlerType, "channelInactive", ChannelHandlerContext.class)) {
                    mask &= ~MASK_CHANNEL_INACTIVE;
                }
                if (isSkippable(handlerType, "channelRead", ChannelHandlerContext.class, Object.class)) {
                    mask &= ~MASK_CHANNEL_READ;
                }
                if (isSkippable(handlerType, "channelReadComplete", ChannelHandlerContext.class)) {
                    mask &= ~MASK_CHANNEL_READ_COMPLETE;
                }
                if (isSkippable(handlerType, "channelWritabilityChanged", ChannelHandlerContext.class)) {
                    mask &= ~MASK_CHANNEL_WRITABILITY_CHANGED;
                }
                if (isSkippable(handlerType, "userEventTriggered", ChannelHandlerContext.class, Object.class)) {
                    mask &= ~MASK_USER_EVENT_TRIGGERED;
                }
            }

            // Outbound
            if (ChannelOutboundHandler.class.isAssignableFrom(handlerType)) {
                mask |= MASK_ALL_OUTBOUND;

                if (isSkippable(handlerType, "bind", ChannelHandlerContext.class,
                        SocketAddress.class, ChannelPromise.class)) {
                    mask &= ~MASK_BIND;
                }
                if (isSkippable(handlerType, "connect", ChannelHandlerContext.class, SocketAddress.class,
                        SocketAddress.class, ChannelPromise.class)) {
                    mask &= ~MASK_CONNECT;
                }
                if (isSkippable(handlerType, "disconnect", ChannelHandlerContext.class, ChannelPromise.class)) {
                    mask &= ~MASK_DISCONNECT;
                }
                if (isSkippable(handlerType, "close", ChannelHandlerContext.class, ChannelPromise.class)) {
                    mask &= ~MASK_CLOSE;
                }
                if (isSkippable(handlerType, "deregister", ChannelHandlerContext.class, ChannelPromise.class)) {
                    mask &= ~MASK_DEREGISTER;
                }
                if (isSkippable(handlerType, "read", ChannelHandlerContext.class)) {
                    mask &= ~MASK_READ;
                }
                if (isSkippable(handlerType, "write", ChannelHandlerContext.class,
                        Object.class, ChannelPromise.class)) {
                    mask &= ~MASK_WRITE;
                }
                if (isSkippable(handlerType, "flush", ChannelHandlerContext.class)) {
                    mask &= ~MASK_FLUSH;
                }
            }

            if (isSkippable(handlerType, "exceptionCaught", ChannelHandlerContext.class, Throwable.class)) {
                mask &= ~MASK_EXCEPTION_CAUGHT;
            }
        } catch (Exception e) {
            // Should never reach here.
            PlatformDependent.throwException(e);
        }

        return mask;
    }

    @SuppressWarnings("rawtypes")
    private static boolean isSkippable(
            final Class<?> handlerType, final String methodName, final Class<?>... paramTypes) throws Exception {
        return AccessController.doPrivileged(new PrivilegedExceptionAction<Boolean>() {
            @Override
            public Boolean run() throws Exception {
                Method m;
                try {
                    m = handlerType.getMethod(methodName, paramTypes);
                } catch (NoSuchMethodException e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                            "Class {} missing method {}, assume we can not skip execution", handlerType, methodName, e);
                    }
                    return false;
                }
                return m != null && m.isAnnotationPresent(Skip.class);
            }
        });
    }

    private ChannelHandlerMask() { }

    /**
     * Indicates that the annotated event handler method in {@link ChannelHandler} will not be invoked by
     * {@link ChannelPipeline} and so <strong>MUST</strong> only be used when the {@link ChannelHandler}
     * method does nothing except forward to the next {@link ChannelHandler} in the pipeline.
     * <p>
     * Note that this annotation is not {@linkplain Inherited inherited}. If a user overrides a method annotated with
     * {@link Skip}, it will not be skipped anymore. Similarly, the user can override a method not annotated with
     * {@link Skip} and simply pass the event through to the next handler, which reverses the behavior of the
     * supertype.
     * </p>
     */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @interface Skip {
        // no value
    }
}

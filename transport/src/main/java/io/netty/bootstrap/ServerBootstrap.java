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
package io.netty.bootstrap;

import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.util.AttributeKey;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * {@link Bootstrap} sub-class which allows easy bootstrap of {@link ServerChannel}
 *
 */
public class ServerBootstrap extends AbstractBootstrap<ServerBootstrap, ServerChannel> {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ServerBootstrap.class);

    // The order in which child ChannelOptions are applied is important they may depend on each other for validation
    // purposes.
    /**
     * SocketChannel 中的 ChannelOption 配置
     */
    private final Map<ChannelOption<?>, Object> childOptions = new LinkedHashMap<ChannelOption<?>, Object>();
    /**
     * SocketChannel 中的 attributes 配置
     */
    private final Map<AttributeKey<?>, Object> childAttrs = new ConcurrentHashMap<AttributeKey<?>, Object>();

    /**
     * Sub Reactor 线程组
     */
    private volatile EventLoopGroup childGroup;
    /**
     * SocketChannel 中 ChannelPipeline 里的 ChannelHandler<br>
     * 如需添加多个, 使用 ChannelInitializer#initChannel() 方法
     */
    private volatile ChannelHandler childHandler;

    /**
     * 父 AbstractBootstrapConfig<br>
     * 子 ServerBootstrapConfig
     */
    private final ServerBootstrapConfig config = new ServerBootstrapConfig(this);

    public ServerBootstrap() { }

    private ServerBootstrap(ServerBootstrap bootstrap) {
        super(bootstrap);
        childGroup = bootstrap.childGroup;
        childHandler = bootstrap.childHandler;
        synchronized (bootstrap.childOptions) {
            childOptions.putAll(bootstrap.childOptions);
        }
        childAttrs.putAll(bootstrap.childAttrs);
    }

    /**
     * Specify the {@link EventLoopGroup} which is used for the parent (acceptor) and the child (client).
     */
    @Override
    public ServerBootstrap group(EventLoopGroup group) {
        return group(group, group);
    }

    /**
     * Set the {@link EventLoopGroup} for the parent (acceptor) and the child (client). These
     * {@link EventLoopGroup}'s are used to handle all the events and IO for {@link ServerChannel} and
     * {@link Channel}'s.
     */
    public ServerBootstrap group(EventLoopGroup parentGroup, EventLoopGroup childGroup) {
        super.group(parentGroup);
        if (this.childGroup != null) {
            throw new IllegalStateException("childGroup set already");
        }
        this.childGroup = ObjectUtil.checkNotNull(childGroup, "childGroup");
        return this;
    }

    /**
     * Allow to specify a {@link ChannelOption} which is used for the {@link Channel} instances once they get created
     * (after the acceptor accepted the {@link Channel}). Use a value of {@code null} to remove a previous set
     * {@link ChannelOption}.
     */
    public <T> ServerBootstrap childOption(ChannelOption<T> childOption, T value) {
        ObjectUtil.checkNotNull(childOption, "childOption");
        synchronized (childOptions) {
            if (value == null) {
                childOptions.remove(childOption);
            } else {
                childOptions.put(childOption, value);
            }
        }
        return this;
    }

    /**
     * Set the specific {@link AttributeKey} with the given value on every child {@link Channel}. If the value is
     * {@code null} the {@link AttributeKey} is removed
     */
    public <T> ServerBootstrap childAttr(AttributeKey<T> childKey, T value) {
        ObjectUtil.checkNotNull(childKey, "childKey");
        if (value == null) {
            childAttrs.remove(childKey);
        } else {
            childAttrs.put(childKey, value);
        }
        return this;
    }

    /**
     * Set the {@link ChannelHandler} which is used to serve the request for the {@link Channel}'s.
     */
    public ServerBootstrap childHandler(ChannelHandler childHandler) {
        this.childHandler = ObjectUtil.checkNotNull(childHandler, "childHandler");
        return this;
    }

    @Override
    void init(Channel channel) {
        // 向 NioServerSocketChannel 设置 NioServerSocketChannelConfig
        setChannelOptions(channel, newOptionsArray(), logger);
        // 向 NioServerSocketChannel 设置 attributes
        setAttributes(channel, attrs0().entrySet().toArray(EMPTY_ATTRIBUTE_ARRAY));

        // NioServerSocketChannel.pipeline
        ChannelPipeline p = channel.pipeline();

        // 获取 Sub Reactor 线程组
        final EventLoopGroup currentChildGroup = childGroup;
        // 获取用于初始化客户端 NioSocketChannel 的 ChannelInitializer
        final ChannelHandler currentChildHandler = childHandler;
        // 获取用户配置的客户端 NioSocketChannel 的 NioSocketChannelConfig
        final Entry<ChannelOption<?>, Object>[] currentChildOptions;
        synchronized (childOptions) {
            currentChildOptions = childOptions.entrySet().toArray(EMPTY_OPTION_ARRAY);
        }
        // 获取用户配置的客户端 NioSocketChannel 的 attributes
        final Entry<AttributeKey<?>, Object>[] currentChildAttrs = childAttrs.entrySet().toArray(EMPTY_ATTRIBUTE_ARRAY);

        // 向 NioServerSocketChannel 中的 pipeline 添加初始化 ChannelHandler 逻辑
        // 当 NioServerSocketChannel 注册到 Main Reactor 线程组时, 由 Main Reactor 负责初始化 AbstractUnsafe#register0
        // 由于此时 Channel 还未注册到 Main Reactor, ChannelInitializer 添加到 pipeline 后
        // 会把 ChannelInitializer#handlerAdded 包装成 PendingHandlerAddedTask 任务, 存储在 pipeline 的任务列表中
        p.addLast(
                // Netty ChannelInitializer#handlerAdded 会被包装成一个 PendingHandlerAddedTask 任务
                new ChannelInitializer<Channel>() {
                    @Override
                    public void initChannel(final Channel ch) {
                        // NioServerSocketChannel.pipeline
                        final ChannelPipeline pipeline = ch.pipeline();
                        // 用户指定的 AbstractBootstrap.handler, 它可能也是 ChannelInitializer
                        // 但此时 Channel 已经注册到 Reactor, 添加到 pipeline 后不会包装 PendingHandlerAddedTask 任务
                        // 直接调用用户自定义的 ChannelInitializer#handlerAdded -> ChannelInitializer#initChannel
                        ChannelHandler handler = config.handler();
                        if (handler != null) {
                            pipeline.addLast(handler);
                        }
        
                        // NioServerSocketChannel.pipeline 异步添加用于接收客户端连接的 ServerBootstrapAcceptor
                        // ServerBootstrapAcceptor#channelRead 会初始化客户端 NioSocketChannel
                        ch.eventLoop().execute(new Runnable() {
                            @Override
                            public void run() {
                                pipeline.addLast(
                                        new ServerBootstrapAcceptor(
                                                ch, 
                                                currentChildGroup, 
                                                currentChildHandler, currentChildOptions, currentChildAttrs
                                        )
                                );
                            }
                        });
                    }
                }
        );
    }

    @Override
    public ServerBootstrap validate() {
        super.validate();
        if (childHandler == null) {
            throw new IllegalStateException("childHandler not set");
        }
        if (childGroup == null) {
            logger.warn("childGroup is not set. Using parentGroup instead.");
            childGroup = config.group();
        }
        return this;
    }

    private static class ServerBootstrapAcceptor extends ChannelInboundHandlerAdapter {

        private final EventLoopGroup childGroup;
        private final ChannelHandler childHandler;
        private final Entry<ChannelOption<?>, Object>[] childOptions;
        private final Entry<AttributeKey<?>, Object>[] childAttrs;
        private final Runnable enableAutoReadTask;

        /**
         * 在 ServerBootstrap#init 方法创建
         */
        ServerBootstrapAcceptor(
                final Channel channel, EventLoopGroup childGroup, ChannelHandler childHandler,
                Entry<ChannelOption<?>, Object>[] childOptions, Entry<AttributeKey<?>, Object>[] childAttrs) {
            this.childGroup = childGroup;
            this.childHandler = childHandler;
            this.childOptions = childOptions;
            this.childAttrs = childAttrs;

            // Task which is scheduled to re-enable auto-read.
            // It's important to create this Runnable before we try to submit it as otherwise the URLClassLoader may
            // not be able to load the class because of the file limit it already reached.
            //
            // See https://github.com/netty/netty/issues/1328
            enableAutoReadTask = new Runnable() {
                @Override
                public void run() {
                    channel.config().setAutoRead(true);
                }
            };
        }

        @Override
        @SuppressWarnings("unchecked")
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            final Channel child = (Channel) msg;

            // 向客户端 NioSocketChannel 的 pipeline 中
            // 添加在启动配置类 ServerBootstrap 中配置的 ChannelHandler (它可能是 ChannelInitializer)
            // 由于此时 Channel 还未注册到 Sub Reactor, ChannelInitializer 添加到 pipeline 后
            // 会把 ChannelInitializer#handlerAdded 包装成 PendingHandlerAddedTask 任务, 存储在 pipeline 的任务列表中
            child.pipeline().addLast(childHandler);

            // 向 NioSocketChannel 设置 NioSocketChannelConfig
            setChannelOptions(child, childOptions, logger);
            // 向 NioSocketChannel 设置 attributes
            setAttributes(child, childAttrs);

            try {
                // 执行这段代码的是 Main Reactor 线程
                // 在 Sub Reactor 线程组中选择一个 Reactor, 注册到 Reactor#selector 上, 监听 OP_READ 事件
                childGroup.register(child).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (!future.isSuccess()) {
                            forceClose(child, future.cause());
                        }
                    }
                });
            } catch (Throwable t) {
                forceClose(child, t);
            }
        }

        private static void forceClose(Channel child, Throwable t) {
            child.unsafe().closeForcibly();
            logger.warn("Failed to register an accepted channel: {}", child, t);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final ChannelConfig config = ctx.channel().config();
            if (config.isAutoRead()) {
                // stop accept new connections for 1 second to allow the channel to recover
                // See https://github.com/netty/netty/issues/1328
                config.setAutoRead(false);
                ctx.channel().eventLoop().schedule(enableAutoReadTask, 1, TimeUnit.SECONDS);
            }
            // still let the exceptionCaught event flow through the pipeline to give the user
            // a chance to do something with it
            ctx.fireExceptionCaught(cause);
        }
    }

    @Override
    @SuppressWarnings("CloneDoesntCallSuperClone")
    public ServerBootstrap clone() {
        return new ServerBootstrap(this);
    }

    /**
     * Return the configured {@link EventLoopGroup} which will be used for the child channels or {@code null}
     * if non is configured yet.
     *
     * @deprecated Use {@link #config()} instead.
     */
    @Deprecated
    public EventLoopGroup childGroup() {
        return childGroup;
    }

    final ChannelHandler childHandler() {
        return childHandler;
    }

    final Map<ChannelOption<?>, Object> childOptions() {
        synchronized (childOptions) {
            return copiedMap(childOptions);
        }
    }

    final Map<AttributeKey<?>, Object> childAttrs() {
        return copiedMap(childAttrs);
    }

    @Override
    public final ServerBootstrapConfig config() {
        return config;
    }
}

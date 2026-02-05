> 本文基于 Netty 4.1.112.Final , Kafka 3.9.0 版本进行讨论

在业务开发的场景中，我们经常会遇到很多定时任务的需求。比如，生成业务报表，周期性对账，同步数据，订单支付超时处理等。针对业务场景中定时任务逻辑复杂，执行时间长的特点，市面上已经有很多成熟的任务调度中间件可供我们选择。比如：ElasticJob , XXL-JOB , PowerJob 等等。

而在中间件的场景中，同样也存在很多定时任务的需求。比如，网络连接的心跳检测，网络请求超时或失败的重试机制，网络连接断开之后的重连机制。和业务场景不同的是，这些中间件场景的定时任务特点是逻辑简单，执行时间非常短，而且对时间精度的要求比较低。比如，心跳检测以及失败重试这些定时任务，其实晚执行个几十毫秒或者 100 毫秒也无所谓。

于是针对中间件场景中的这些定时任务特点：

1.  海量任务
    
2.  任务逻辑简单
    
3.  执行时间短
    
4.  对任务调度的及时性没有那么高的要求
    

各大中间件设计了时间轮来调度具有上述 4 种特征的定时任务，而本文主要讨论的就是时间轮的设计与实现。但在这之前我们需要搞清楚时间轮这个设计需求是怎么产生的，我们先从 JDK 中的任务调度组件开始聊起，看看 JDK 中的这些任务调度组件为什么不能满足中间件的场景。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2g0X1jXOG90H28vdN5gb3RSFl4VToWumLf6DdMDyDnoz6icsGdaiazjNA/640?wx_fmt=other&from=appmsg#imgIndex=0)

本文概要.png

## 1\. JDK 中的任务调度组件

说到定时任务，我们第一时间就能想到的调度组件就是 JDK 中的 Timer，为什么这么说呢，因为笔者刚参加工作时的第一个任务就是用 Timer 实现的，当时对 Java 一无所知，完全零基础。主管交给我一个定时任务的需求，两眼抹黑。于是带着清澈而又稚嫩的眼神到网上一顿搜索，找到了这个 Timer，如获至宝。

### 1.1 Timer

```
public class Timer {
    // 优先队列，按照任务的 ExecutionTime，由近到远组织
    private final TaskQueue queue = new TaskQueue();
    // 延时任务的调度线程
    private final TimerThread thread = new TimerThread(queue);
}
```

Timer 中有两个核心组件，一个是用于调度延时任务的 TimerThread ，另一个是 TaskQueue，用于组织延时任务。它是一个优先级队列，其底层是一个数组实现的小根堆。

```
class TaskQueue {
    // 数组实现的小根堆
    private TimerTask[] queue = new TimerTask[128];

    // 向小根堆的堆底添加TimerTask 
    void add(TimerTask task) {
        if (size + 1 == queue.length)
            queue = Arrays.copyOf(queue, 2*queue.length);

        queue[++size] = task;
        // 向上调整
        fixUp(size);
    }

    // 获取堆顶任务
    TimerTask getMin() {
        return queue[1];
    }
    
    // 删除堆顶任务
    void removeMin() {
        queue[1] = queue[size];
        queue[size--] = null;
        //向下调整堆
        fixDown(1);
    }
}
```

TaskQueue 会将所有延时任务按照它们的 ExecutionTime ，由近到远的组织在小根堆中，堆顶永远存放的是 ExecutionTime 最近的延时任务。

TimerThread 会不断的从 TaskQueue 中获取堆顶任务，如果堆顶任务的 ExecutionTime 已经达到 —— `executionTime <= currentTime` , 则执行任务。如果该任务是一个周期性任务，则将任务重新放入到 TaskQueue 中。

如果堆顶任务的 ExecutionTime 还没有到达，那么 TimerThread 就会等待 `executionTime - currentTime` 的时间，一直到堆顶任务的执行时间到达，TimerThread 被重新唤醒执行堆顶任务。

```
    private void mainLoop() {
        while (true) {
            try {
                TimerTask task;
                // 堆顶任务的执行时间是否到达
                boolean taskFired;
                synchronized(queue) {
                    long currentTime, executionTime;
                    // 获取堆顶延时任务
                    task = queue.getMin();
                    synchronized(task.lock) {
                        // 当前时间
                        currentTime = System.currentTimeMillis();
                        // 堆顶任务的执行时间
                        executionTime = task.nextExecutionTime;
                        // 是否到达堆顶任务的执行时间
                        if (taskFired = (executionTime<=currentTime)) {
                            if (task.period == 0) { // Non-repeating, remove
                                queue.removeMin();
                                task.state = TimerTask.EXECUTED;
                            } else { // Repeating task, reschedule
                                queue.rescheduleMin(
                                  task.period<0 ? currentTime   - task.period
                                                : executionTime + task.period);
                            }
                        }
                    }
                    // 如果堆顶任务的执行时间还未到达，那么 TimerThread 就会在这里等待
                    if (!taskFired)
                        queue.wait(executionTime - currentTime);
                }
                // 如果堆顶任务的执行时间已经到达，则立即执行
                if (taskFired)  // Task fired; run it, holding no locks
                    task.run();
            } catch(InterruptedException e) {
            }
        }
    }
```

根据以上 Timer 的核心实现，我们可以总结出 Timer 在应对中间件场景的延时任务时，有以下四种不足：

1.  首先用于组织延时任务的 TaskQueue 本质上是一个小根堆。对于堆这种数据结构来说，添加，删除一个延时任务时，堆都要向上，向下调整以便满足小根堆的特性。单次操作的时间复杂度为 `O(logn)`。显然在面对海量定时任务的添加，删除时，性能上还是差点意思。
    
2.  Timer 调度框架中只有一个 TimerThread 线程来负责延时任务的调度，执行。在面对海量任务的时候，通常会显得力不从心。
    
3.  另外一个严重问题是，当延时任务在执行的过程中出现异常时， Timer 并不会捕获，会导致 TimerThread 终止。这样一来，TaskQueue 中的其他延时任务将永远不会得到执行。
    
4.  Timer 依赖于系统的绝对时间，如果系统时间本身不准确，那么延时任务的调度就可能会出问题。
    

### 1.2 DelayQueue

DelayQueue 是 JDK 提供的一个延时队列，我们可以利用它来延时获取队列中的元素，它的实现其实和 Timer 中的 TaskQueue 很类似，其底层都是一个优先级队列。

```
public class DelayQueue<E extends Delayed> extends AbstractQueue<E>
    implements BlockingQueue<E> {
    
    //基于小根堆实现的优先级队列
    private final PriorityQueue<E> q = new PriorityQueue<E>();
}
```

本质上还是一个数组实现的小根堆。添加，删除任务的时间复杂度仍然是 `O(logn)`。

```
public class PriorityQueue<E> {
    // 数组实现的小根堆
    transient Object[] queue; 
}
```

DelayQueue 中的元素必须实现 `Delayed` 接口中的 `getDelay` , `compareTo` 方法。

```
public interface Delayed extends Comparable<Delayed> {
    long getDelay(TimeUnit unit);
}

public interface Comparable<T> {
   public int compareTo(T o);
}
```

其中 `getDelay` 方法用于获取任务还有多久到期。返回值如果小于等于 0 ，则表示该任务已经到期了。

`compareTo` 方法用于调整任务在 DelayQueue 中的位置，DelayQueue 是一个小根堆，每次向 DelayQueue 添加新的任务时，先是把任务放到 DelayQueue 的末尾，然后依次向上调整，直到任务的过期时间大于等于其 parent 。 这样就可以保证 DelayQueue 的小根堆特性 —— 堆顶元素永远是过期时间最近的任务。

我们可以通过 `take()` 方法从 DelayQueue 获取到期的堆顶任务，如果堆顶任务还没到期，那么就会在 DelayQueue 上阻塞等待，直到堆顶任务到期为止。

```
    public E take() throws InterruptedException {
        try {
            for (;;) {
                // 获取 DelayQueue 堆顶任务
                E first = q.peek();
                if (first == null)
                    available.await();
                else {
                    // 获取堆顶任务还有多久到期
                    long delay = first.getDelay(NANOSECONDS);
                    if (delay <= 0L)
                        // 堆顶任务到期，则从 DelayQueue 中取出
                        return q.poll();
                    else {
                        try {
                            // 等待堆顶任务到期
                            available.awaitNanos(delay);
                        }
                    }
                }
            }
        } finally {
                    ...........
        }
    }
```

从 DelayQueue 的实现上可以看出，相比于 Timer ，DelayQueue 只是一个延时任务的管理队列，而 Timer 却是一个完整的任务调度组件。我们需要在 DelayQueue 的基础之上，额外地实现任务调度功能。

但其底层的核心数据结构仍然是一个小根堆。和 Timer 一样，添加删除延时任务的时间复杂度都是 `O(logn)`。同样无法满足海量延时任务的调度。

### 1.3 ScheduledThreadPoolExecutor

ScheduledThreadPoolExecutor 是多线程版本的 Timer ，它是在 DelayQueue 的基础上增加了多线程调度延时任务的能力。ScheduledThreadPoolExecutor 中负责组织管理延时任务的是 DelayedWorkQueue，它也是一个小根堆实现的优先级队列，延时任务 ScheduledFutureTask 按照到期时间由近及远的组织在 DelayedWorkQueue 中。DelayedWorkQueue 的第一个元素是到期时间最近的  ScheduledFutureTask。

业务线程可以通过 schedule , scheduleAtFixedRate , scheduleWithFixedDelay 方法将延时任务 ScheduledFutureTask 添加到 DelayedWorkQueue 中。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2kHtJE9PNKcfRAefwic1GEhBzOwR38XVaaa9al7NPyXSicq1NVnfibmMWw/640?wx_fmt=other&from=appmsg#imgIndex=1)

image.png

ScheduledThreadPoolExecutor 负责调度延时任务的是一个线程池，里边包含了多个 worker 调度线程，每个 worker 线程负责从 DelayedWorkQueue 中获取已经到期的 ScheduledFutureTask，然后执行。如果 DelayedWorkQueue 中没有任务到期，那么 worker 线程就会在 DelayedWorkQueue 上阻塞等待，直到有到期的任务出现。

虽然 ScheduledThreadPoolExecutor 提供了多线程的调度能力，在一定程度上保证了延时任务调度的及时性，但是其底层仍然是依赖 DelayedWorkQueue 来管理延时任务，在面对海量延时任务的添加，删除时，时间复杂度依然还是 `O(logn)` 。那么有没有一种数据结构可以将这个时间复杂度降低为 `O(1)` 呢 ？ 这就是本文我们要讨论的重点内容 —— 时间轮的设计与实现。

## 2\. Netty 时间轮的设计原理

时间轮的设计灵感来自于我们日常生活中用的钟表，钟表有秒针，分针，时针，共三个指针，60 个刻度。秒针每走一个刻度就是一秒，秒针走完一个时钟周期（60s），分针走一个刻度就是一分钟，当分针走完一个时钟周期（60m），时针走一个刻度就是一个小时。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2hd8oyhMsEEylkf2KRWghAZqIibMf2Mw0oWmSSv9icAc2bawBDNEz10Yg/640?wx_fmt=other&from=appmsg#imgIndex=2)

image.png

比如我们要在今天的 10 点 10 分 0 秒这个时刻去开一个重要的会议，那么当钟表的秒针指向 0 这个刻度，分针指向 10 这个刻度，时针指向 10 这个刻度的时候，闹钟就会响起，提醒我们去执行开会这个延时任务。

如果我们能把钟表里的刻度抽象成一个数据结构，用这个数据结构来存放对应刻度的延时任务的话，那么当钟表的时针，分针，秒针指向某个刻度的时候，我们就去执行这个刻度对应的延时任务，这样一来，一种新的延时任务调度思路就出来了，这也是时间轮的设计理念。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2dXO0dwGj18CIuYibmUL0r8Y1qTczhI8auxGR4zuS7ald3k1ic68gX9iaQ/640?wx_fmt=other&from=appmsg#imgIndex=3)

image.png

如上图所示，Netty 将钟表的刻度抽象成了一个 HashedWheelBucket 的数据结构，钟表的表盘被抽象成一个 HashedWheelBucket 类型的环形数组，钟表中有 60 个刻度，而 Netty 的时间轮 HashedWheelTimer 一共有 512 个刻度。

```
public class HashedWheelTimer implements Timer {
    // 数组大小默认为 512
    private final HashedWheelBucket[] wheel;
    // HashedWheelTimer 的时钟精度，也就是时钟间隔，多久转动一次，默认 100ms, 最小值为 1ms
    private final long tickDuration;
}
```

钟表中一共有三个指针，分别是秒针，分针，时针。而 HashedWheelTimer 中只有一个 tick 指针，tick 每隔 tickDuration (100ms) 走一个刻度，也就是说 Netty 时间轮的时钟精度就是 100 ms , 定时任务的调度延时有时会在 100ms 左右。如果你接受不了这么大的调度误差，那么可以将  tickDuration 适当调小一些，但最小不能低于 1ms 。

什么意思呢 ？比如现在我们需要在 5ms 之后执行一个延时任务，那么时间轮可能在 8ms 之后才会调度这个任务，也可能在 65ms 之后调度，也有可能在 108ms  之后调度，这就使得定时任务的执行有了大约 100ms 左右的延时。

具体延时多少，取决于我们在什么时刻将这个定时任务添加到时间轮中。关于这一点，笔者后面会在介绍时间轮具体实现细节的时候详细讨论，这里点到为止，本小节我们还是主要聚焦于时间轮的设计原理。

对于钟表的秒针来说，它的 tickDuration 就是 1s , 走完一个时钟周期就是 60s 。 对于分针来说，它的 tickDuration 就是 1m , 走完一个时钟周期就是 60m。对于时针来说，它的 tickDuration 就是 1h , 走完一个时钟周期就是 12h。

由于 HashedWheelTimer 中的 tickDuration 是 100ms , 有 512 个刻度 (HashedWheelBucket) , 所以时间轮中的 tick 指针走完一个时钟周期需要 51200ms 。

HashedWheelBucket 是一个具有头尾指针的双向链表，链表中存储的元素类型为 HashedWheelTimeout  用于封装定时任务。HashedWheelBucket 中的 head 指向双向链表中的第一个 HashedWheelTimeout ， tail 指向双向链表中的最后一个 HashedWheelTimeout。

```
    private static final class HashedWheelBucket {
        // Used for the linked-list datastructure
        private HashedWheelTimeout head;// 指向双向链表中的第一个 timeout
        private HashedWheelTimeout tail;// 指向双向链表中的最后一个 timeout
    }
```

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2YwWcEsA0cT745d2mluFDE3nsq7viaMVQxs7wibfKyWN4Gmzd1icSF5ibjw/640?wx_fmt=other&from=appmsg#imgIndex=4)

image.png

HashedWheelTimeout 用于封装时间轮中的延时任务，提交到时间轮中的延时任务必须实现 TimerTask 接口。

```
// 延时任务
public interface TimerTask {
    void run(Timeout timeout) throws Exception;
}

private static final class HashedWheelTimeout implements Timeout, Runnable {
        // 延时任务所属的时间轮
        private final HashedWheelTimer timer;
        // 延时任务
        private final TimerTask task;
        // 延时任务的 deadline ，该时间是一个绝对时间，以时间轮的启动时间 startTime 为起点
        private final long deadline;
        // 延时任务所属的 bucket
        HashedWheelBucket bucket;
        // 指向其在 bucket 的下一个延时任务
        HashedWheelTimeout next;
        // 指向其在 bucket 的前一个延时任务
        HashedWheelTimeout prev;


        HashedWheelTimeout(HashedWheelTimer timer, TimerTask task, long deadline) {
            this.timer = timer;
            this.task = task;
            this.deadline = deadline;
        }
}
```

HashedWheelTimeout 中有一个重要的属性 deadline ，它规定了延时任务 TimerTask 的到期时间。deadline 是一个绝对时间值，它以时间轮的启动时间 startTime 为起点，表示从 startTime 这个时间点开始，到 deadline 这个时间点到期。

```
// 计算延时任务到期的绝对时间戳
// 时间轮中的时间戳均以时间轮的启动时间 startTime 为起点
long deadline = System.nanoTime() + unit.toNanos(delay) - startTime;
```

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2B3j3q2UBIoGeib4bo9uSrLKVRoxDuvLiag9BKjRTyKV41UQgK1icfhDqg/640?wx_fmt=other&from=appmsg#imgIndex=5)

image.png

Netty 时间轮中的时间坐标系全部是以时间轮的启动时间点 startTime 为基准的，当时间轮启动之后，会将那一刻的时间戳设置到 startTime 中。

```
public class HashedWheelTimer implements Timer {
    // 时间轮的启动时间戳（纳秒）
    private volatile long startTime;
}
```

时间轮中的 tick 指针也是一个绝对值，当时间轮启动之后，tick 指向 0 ，每隔 100ms (tickDuration)，tick 向前转动一下。但需要注意的是 tick 的值是只增不减的，只要时间轮在运行，那么 tick 的值就会一直递增上去。比如，当 tick 转动完一个时钟周期（51200ms）之后，tick 的值是 512 而不是重新指向 0 。

tick 与 HashedWheelBucket 之间的映射关系通过 `ticks & mask` 计算得出。mask 为 HashedWheelBucket 的个数减 1 ，所以这就要求时间轮中 HashedWheelBucket 的个数必须是 2 的次幂。

在时间轮中，属于同一个 HashedWheelBucket 中的延时任务 HashedWheelTimeouts ，它们的到期时间 deadline 都在同一时间范围内 —— `[ tick , tick + 1) * tickDuration` 。

比如，在时间轮刚刚启动之后，tick 指向 0 ，那么 wheel\[0\] 指向的 HashedWheelBucket 里存放的 HashedWheelTimeouts，它们的到期时间均在 `[ 0 , 100) ms` 之内。

假如我们在 `tick = 0` 这个时刻，向时间轮中添加了一个延时 `5ms` 执行的  HashedWheelTimeout，那么它就会被放入 wheel\[0\]  中。如果添加的是一个延时 `101ms` 执行的 HashedWheelTimeout，那么它就会被放入 wheel\[1\]  中。同样的道理，如果添加的是一个延时 `360ms` 执行的 HashedWheelTimeout，那么它就会被放入 wheel\[3\]  中。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2lryOB0iarTXhLWyOctMicibZKqS2Q9QSH4BricxXbUbBUnc7FpfpP6ia2Yw/640?wx_fmt=other&from=appmsg#imgIndex=6)

image.png

当时间过了 100ms 之后，Netty 就会将 `HashedWheelBucket0` 中的延时任务拉出来执行，执行完之后，tick 的值加 1 ，从 0 转动到 1 。在经过 100 ms 之后，Netty 就会将 `HashedWheelBucket1` 中的延时任务拉出来执行，执行完之后，tick 的值加 1 ，从 1 转动到 2 ，如此往复循环。这就是整个时间轮的运转逻辑。

但从这个过程中我们可以看出，延时任务的调度存在 tickDuration（100ms）左右的延迟。比如，在 `tick = 0` 这个时刻，添加到  `HashedWheelBucket0` 中的延时任务，我们本来是期望这些延时任务分别在 5ms , 10ms , 95ms 之后执行，但时间轮的真正调度时间却在 100ms 之后。这就导致了任务调度产生了 100ms 左右的延迟。

如果你接受不了 100ms 的延迟，那么可以在创建时间轮的时候，将 tickDuration 的值调低，但不能低于 1ms 。tickDuration 的值越小，时间轮的精度越高，性能开销也就越大。tickDuration 的值越大，时间轮的精度也就越低，性能开销越小。

```
    public HashedWheelTimer(long tickDuration, TimeUnit unit) {
        
    }
```

但在中间件的场景中，往往对延时任务调度的及时性没有那么高的要求，同时为了兼顾时间轮的精度与性能，tickDuration 默认设置为100ms 是刚好合适的，通常不需要调整。

另外在默认情况下，只有一个线程 workerThread 负责推动时间轮的转动，以及延时任务的执行。

```
public class HashedWheelTimer implements Timer {
    // HashedWheelTimer 的 worker 线程，由它来驱动时间轮的转动，延时任务的执行
    private final Thread workerThread;
}
```

从上面的过程可以看出，只有当前 tick 对应的 HashedWheelBucket 中的延时任务全部被执行完毕的时候，tick 才会向前推动。所以为了保证任务调度的及时性，时间轮中的延时任务执行时间不能太长，只适合逻辑简单，执行时间短的延时任务。

但毕竟在默认情况下就只有这一个 workerThread，既负责延时任务的调度，又负责延时任务的执行，对于有海量并发延时任务的场景，还是显得力不从心。为了应对这种情况，我们可以在创建时间轮的时候，指定一个专门用于执行延时任务的 Executor。

这样一来，时间轮中的延时任务调度还是由单线程 workerThread 负责，到期的延时任务由线程池 Executor 来负责执行，近一步提升延时任务调度的及时性。但事实上，在大部分场景中，有一个 workerThread 就够了，并不需要额外的指定 Executor。大家可以根据实际情况，自由裁定。

```
public class HashedWheelTimer implements Timer {
    // 负责执行延时任务，用于应对大量的并发延时任务场景
    // 默认为单线程 workerThread
    private final Executor taskExecutor;
    // 在构造函数中可以设置 taskExecutor
    public HashedWheelTimer(
            ThreadFactory threadFactory,
            long tickDuration, TimeUnit unit, int ticksPerWheel, boolean leakDetection,
            long maxPendingTimeouts, Executor taskExecutor) 
}
```

另外还有一个问题就是，上图时间轮中的延时任务，它们的延时时间都在同一时钟周期内。Netty 时间轮中的一个时钟周期是 51200ms 。

也就是说，在 `tick = 0` 这个时刻，只要延时任务的延时时间在 51200ms 之内，那么当 tick 转动完 512 个刻度之后（一个时钟周期），这 512 个刻度对应的 HashedWheelBucket 中的延时任务全部会被执行到。

如果我们在 `tick = 0` 这个时刻，添加一个延时任务，但它的延时时间超过了一个时钟周期，比如在 `51250ms` 之后执行。  那么这个延时任务也会被添加到 `HashedWheelBucket0` 中。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2wibvIC8XCndIpB4Q86FHBSWuFZnBTyDI4OEicVez2no4z5icFlfRcrY5Q/640?wx_fmt=other&from=appmsg#imgIndex=7)

image.png

当时间过了 100ms 之后，workerThread 就会执行 `HashedWheelBucket0` 中的延时任务。但此时只能执行延时 5ms , 10ms 的任务，不能执行延时 `51250ms` 的任务，因为它需要等到下一个时钟周期才能执行。

那么 workerThread 在执行延时任务的时候如何才能知道，哪些任务是本次时钟周期内可以执行的，哪些任务是要等到下一次或者下下次时钟周期才能执行的呢 ？

在延时任务模型 HashedWheelTimeout 中有一个字段 —— remainingRounds，用于记录延时任务还剩多少时钟周期可以执行。

```
private static final class HashedWheelTimeout implements Timeout, Runnable {
    // 执行该延时任务需要经过多少时钟周期
    long remainingRounds;
}
```

本次时钟周期内可以执行的延时任务，它的 remainingRounds = 0 ，workerThread 在遇到 `remainingRounds = 0` 的 HashedWheelTimeout 就会执行。

下一个时钟周期才能执行的延时任务，它的  remainingRounds = 1 ，依次类推。当 workerThread 遇到 `remainingRounds > 0` 的 HashedWheelTimeout 就会直接跳过，并将 remainingRounds 减 1 。

比如，上图中 HashedWheelBucket0 中的这几个延时任务，其中延时 5ms , 10ms 的 HashedWheelTimeout 它们的 `remainingRounds = 0`, 表示在本次时钟周期内就可以马上执行。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2MoicJmpHFR2ib0lrk9aC6XKyLoKzXk3Szia3fIEaftm5G1IVFSULczmibQ/640?wx_fmt=other&from=appmsg#imgIndex=8)

image.png

延时 51250ms 的 HashedWheelTimeout 它的 `remainingRounds = 1` ， 表示在下一个时钟周期才能执行。

好了，现在整个时间轮的设计原理笔者就为大家介绍完了，那么让我们再次回到本小节开头的问题，在面对海量延时任务的添加与取消时，时间轮如何将这个时间复杂度降低为 `O(1)` 呢 ？

首先，时间轮的核心数据结构就是一个 HashedWheelBucket 类型的环形数组 wheel ， 数组长度默认为 512 。wheel 数组用于组织管理时间轮中的所有延时任务。

```
    // 数组大小默认为 512
    private final HashedWheelBucket[] wheel;
```

与之前介绍的延时队列 DelayedWorkQueue 不同的是，环形数组 wheel 会按照延时时间的不同，将延时任务分散到 512 个 HashedWheelBucket 中管理。每个 HashedWheelBucket 负责管理到期时间范围在 `[ tick , tick + 1) * tickDuration` 之间的任务。

而 DelayedWorkQueue 则是用一个优先级队列来管理所有的延时任务，为了维护小根堆的特性，每次在向 DelayedWorkQueue 添加或者删除延时任务的时间复杂度为 `O(logn)`。

当我们向时间轮添加一个延时任务时，Netty 首先会根据延时任务的 deadline 以及 tickDuration 计算出该延时任务最终会停留在哪一个 tick 上。注意，延时任务中的  deadline 是一个绝对值而不是相对值，是以时间轮启动时间 startTime 为基准的一个绝对时间戳。tick 也是一个绝对值而不是相对值，是以时间轮刚刚启动时 `tick = 0` 为基准的绝对值，只增不减。

比如，前面这个延时 51250ms 的任务，它最终会停留在 `tick = 512` 上。但由于时间轮是一个环形数组，所以 tick 512 与数组长度 512 取余得到所属 HashedWheelBucket 在 wheel 数组中的 index = 0。

```
// 计算延时任务，最终会停留在哪一个 tick 上
long calculated = timeout.deadline / tickDuration;

// 获取 calculated 对应的 HashedWheelBucket
int stopIndex = (int) (calculated & mask);
HashedWheelBucket bucket = wheel[stopIndex];

// 将延时任务添加到对应的 HashedWheelBucket 中
bucket.addTimeout(timeout);
```

然后将延时任务添加到 HashedWheelBucket 的末尾，前面我们已经提过，HashedWheelBucket 是一个双向链表，向链表末尾添加一个元素的时间复杂度为 `O(1)` 。

```
private static final class HashedWheelBucket {

        public void addTimeout(HashedWheelTimeout timeout) {
            assert timeout.bucket == null;
            timeout.bucket = this;
            if (head == null) {
                head = tail = timeout;
            } else {
                tail.next = timeout;
                timeout.prev = tail;
                tail = timeout;
            }
        }      
}
```

延时任务的取消逻辑也很简单，就是将这个延时任务从其所属的 HashedWheelBucket 中删除即可。从一个双向链表中删除某个指定的元素时间复杂度也是 `O(1)`。

```
  public HashedWheelTimeout remove(HashedWheelTimeout timeout) {
            HashedWheelTimeout next = timeout.next;
            // timeout 不是第一个元素
            if (timeout.prev != null) { 
                timeout.prev.next = next;
            }
            // timeout 不是最后一个元素
            if (timeout.next != null) {
                timeout.next.prev = timeout.prev;
            }

            if (timeout == head) {
                // Bucket 中只有一个任务，直接将头尾指针置空
                if (timeout == tail) {
                    tail = null;
                    head = null;
                } else {
                    // 待删除的任务是第一个任务，head 指针向后移动
                    head = next;
                }
            } else if (timeout == tail) {
                // 待删除的任务是最后一个任务，tail 指针向前移动
                tail = timeout.prev;
            }
            // null out prev, next and bucket to allow for GC.
            timeout.prev = null;
            timeout.next = null;
            timeout.bucket = null;
            return next;
        }
```

从以上过程我们可以看出，时间轮在面对海量延时任务的添加，取消的时候，所需的时间复杂度都是 `O(1)`，聊完了延时任务的管理，现在我们在来看一下延时任务的调度与执行。

Netty 只靠一个单线程 workThread 来推动时间轮一个 tick 一个 tick 地向前转动，当时间轮转动到某一个 tick 时，workThread 会等待一个 tickDuration （默认 100ms）的时间，随后 workThread 会将该 tick 对应的 HashedWheelBucket 中 `remainingRounds = 0` 的延时任务全都拉取下来挨个执行。

当执行完 HashedWheelBucket 中的延时任务之后，tick 向前推进一格（tick++），workThread 继续睡眠等待一个 tickDuration，然后重复上述过程。

这里我们可以看出，延时任务的调度与执行在默认情况下全部都是由一个单线程 workThread 来执行。如果时间轮中的延时任务逻辑复杂，执行时间长，那么就会影响整个时间轮的调度，tick 的转动就会出现延时，所以**时间轮虽然可以处理海量的延时任务，但是这些延时任务的逻辑必须要简单，执行时间要短**。当然了，我们也可以在创建时间轮的时候指定一个专门执行延时任务的线程池来加快任务的执行。

由于延时任务的调度通常会有一个 tickDuration 左右的延时。比如，任务的调度可能会晚几毫秒或者几十毫秒，也有可能晚一个 tickDuration 左右。**所以时间轮只能处理那些对任务调度的及时性要求没那么高的场景**。

## 3\. Netty 时间轮相关设计模型的实现

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2dXO0dwGj18CIuYibmUL0r8Y1qTczhI8auxGR4zuS7ald3k1ic68gX9iaQ/640?wx_fmt=other&from=appmsg#imgIndex=9)

image.png

### 3.1 HashedWheelTimer

Netty 使用一个叫做 HashedWheelTimer 的结构来描述时间轮，其中包含了第二小节中介绍的所有重要属性以及核心结构。其中最核心的就是 wheel 环形数组，它相当于钟表的表盘，表盘中的每一个刻度用 HashedWheelBucket 结构描述。

```
private final HashedWheelBucket[] wheel;
```

时间轮中究竟包含多少个刻度，是由构造参数 `ticksPerWheel` 决定的，默认为 512 。Netty 会根据延时时间的不同将所有提交到时间轮的延时任务分散到 512 个 HashedWheelBucket 中组织管理。定位延时任务所在的 HashedWheelBucket 以及向 HashedWheelBucket 中添加，取消延时任务的时间复杂度均为 `O(1)`。

```
    public HashedWheelTimer(
            ThreadFactory threadFactory,
            long tickDuration, TimeUnit unit, int ticksPerWheel, boolean leakDetection,
            long maxPendingTimeouts, Executor taskExecutor) 
```

如果时间轮中需要调度的延时任务非常多，那么每个 HashedWheelBucket 中就可能包含大量的延时任务，这就导致时间轮的调度发生延迟。针对这种情况，我们可以适当增加 ticksPerWheel 的个数，让更多的 HashedWheelBucket 来分摊延时任务。但 ticksPerWheel 必须是 2 的次幂。

```
    private static HashedWheelBucket[] createWheel(int ticksPerWheel) {
        // ticksPerWheel 必须是 2 的次幂，默认为 512
        ticksPerWheel = MathUtil.findNextPositivePowerOfTwo(ticksPerWheel);
        // 创建时间轮中的 hash 槽
        HashedWheelBucket[] wheel = new HashedWheelBucket[ticksPerWheel];
        for (int i = 0; i < wheel.length; i ++) {
            wheel[i] = new HashedWheelBucket();
        }
        return wheel;
    }
```

这样一来，当我们向时间轮添加延时的任务的时候，就可以通过 `&` 运算来代替 `%` 运算去寻找延时任务对应的 HashedWheelBucket。

```
mask = wheel.length - 1;
```

第二小节我们已经介绍过了，在向时间轮添加延时任务时，我们需要首先定位到这个延时任务最终会停留在哪一个 tick 上，时间轮中的 tick 是一个绝对值，它不会按照时钟周期的结束而自动归 0 ，而是一直会往上递增。

calculated 也是一个绝对值，表示延时任务最终会停留在哪一个 tick 上，随后通过 `calculated & mask` 定位到对应的 HashedWheelBucket，时间复杂度为 `O(1)`。

```
// 计算延时任务，最终会停留在哪一个 tick 上
long calculated = timeout.deadline / tickDuration;

// 获取 calculated 对应的 HashedWheelBucket
int stopIndex = (int) (calculated & mask);
HashedWheelBucket bucket = wheel[stopIndex];
```

`tickDuration` 表示时间轮中的时钟精度，也就是 tick 指针多久转动一次，默认为 100ms，我们可以通过构造参数 tickDuration 进行指定，但最小不能低于 1ms。

```
private final long tickDuration;
```

tickDuration 的值越小，时间轮的精度越高，性能开销也就越大。tickDuration 的值越大，时间轮的精度也就越低，性能开销越小。

现在时间轮的基本骨架就有了，而时间轮的运转靠的就是 workerThread ，由它来驱动时钟 tick 一下一下的转动，并执行对应 HashedWheelBucket 中的延时任务。

```
private final Worker worker = new Worker();
private final Thread workerThread;
```

由于在默认情况下，Netty 时间轮中就只有这一个单线程 workerThread 来负责延时任务的调度与执行，在面对海量并发任务的时候，难免显得有点力不从心。执行任务的时间过长，就会导致 tick 的转动产生很大的延时。于是 Netty 又在 4.1.69.Final 中引入了一个 taskExecutor，来专门负责执行延时任务。

```
 private final Executor taskExecutor;
```

我们可以通过构造参数 `taskExecutor` 来指定自定义的线程池，默认情况下为 ImmediateExecutor ，其本质还是由 workerThread 来执行延时任务。

```
public final class ImmediateExecutor implements Executor {
    @Override
    public void execute(Runnable command) {
        ObjectUtil.checkNotNull(command, "command").run();
    }
}
```

workerThread 负责从对应 tick 的 HashedWheelBucket 中拉取延时任务，然后将延时任务丢给 taskExecutor 来执行。这在一定程度上提高了延时任务的消费速度，不至于拖慢 workerThread 从而影响到整个时间轮的运转。

时间轮中待执行延时任务的最大个数受到参数 `maxPendingTimeouts` 限制，默认为 -1 。当 maxPendingTimeouts 的值小于等于 0 的时候，表示 Netty 不会对时间轮中的延时任务个数进行限制。

```
// 时间轮中待执行延时任务的最大个数
private final long maxPendingTimeouts;
// 时间轮当前待执行的延时任务个数
private final AtomicLong pendingTimeouts = new AtomicLong(0);
```

当时间轮中的延时任务个数超过了 maxPendingTimeouts 的限制时，再向时间轮添加延时任务就会得到 `RejectedExecutionException` 异常。

```
    @Override
    public Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {

        // 待执行的延时任务计数 + 1
        long pendingTimeoutsCount = pendingTimeouts.incrementAndGet();
        // maxPendingTimeouts 默认为 -1 。表示不对时间轮的延时任务个数进行限制
        // 如果达到限制，则不能继续向时间轮添加任务
        if (maxPendingTimeouts > 0 && pendingTimeoutsCount > maxPendingTimeouts) {
            pendingTimeouts.decrementAndGet();
            throw new RejectedExecutionException("Number of pending timeouts ("
                + pendingTimeoutsCount + ") is greater than or equal to maximum allowed pending "
                + "timeouts (" + maxPendingTimeouts + ")");
        }
                    ，，，，，，，，
    }
```

另外时间轮 HashedWheelTimer 在 JVM 进程中的实例个数会受到 `INSTANCE_COUNT_LIMIT` 的限制，默认为 64 。

```
    // 系统当前时间轮实例的个数
    private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger();
    // 默认允许的 HashedWheelTimer 最大实例个数
    private static final int INSTANCE_COUNT_LIMIT = 64;
```

如果当前 JVM 进程中的 HashedWheelTimer 实例个数超过了 64 ，那么 Netty 就会打印 `Error` 日志进行警告。

```
    private static void reportTooManyInstances() {
        if (logger.isErrorEnabled()) {
            String resourceType = simpleClassName(HashedWheelTimer.class);
            logger.error("You are creating too many " + resourceType + " instances. " +
                    resourceType + " is a shared resource that must be reused across the JVM, " +
                    "so that only a few instances are created.");
        }
    }
```

从上面的警告信息我们可以看出，时间轮是一种共享的资源，既然是一种系统资源，那么就和内存资源一样（ByteBuf）都存在资源泄露的风险。当我们使用完时间轮但忘记调用它的 `stop` 方法进行关闭的时候，就发生了资源泄露。

和 ByteBuf 一样，在 HashedWheelTimer 中也有一个 `ResourceLeakTracker` 用于跟踪探测资源泄露的发生，如果发生资源泄露，Netty 就会以 `Error` 日志的形式打印出泄露的位置。

```
class SimpleLeakAwareByteBuf extends WrappedByteBuf {
   final ResourceLeakTracker<ByteBuf> leak;
}

public class HashedWheelTimer implements Timer {
    private final ResourceLeakTracker<HashedWheelTimer> leak;
}
```

> 关于 ResourceLeakTracker 的实现原理，感兴趣的读者可以回看下笔者之前的文章 [《Netty 如何自动探测内存泄露的发生》](https://mp.weixin.qq.com/s?__biz=Mzg2MzU3Mjc3Ng==&mid=2247490390&idx=1&sn=617e9304df9c2c9c9d9da882f0759dcc&scene=21#wechat_redirect)

我们在创建 HashedWheelTimer 的时候可以通过构造参数 `leakDetection` 来开启，关闭时间轮的资源泄露探测。leakDetection 默认为 true , 表示无条件开启资源泄露的探测。如果设置为 false , 那么只有当 workerThread 不是守护线程的时候才会开启资源泄露探测。

> workerThread 默认情况下并不是一个守护线程。

```
leak = leakDetection || !workerThread.isDaemon() ? leakDetector.track(this) : null;
```

### 3.2 延时任务的添加

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2TUsUclw4h0ZYJG8TdJJiaFyl603YfLKjvstpnw04jqsLedPibuoOAI4A/640?wx_fmt=other&from=appmsg#imgIndex=10)

image.png

如上图所示，当我们向时间轮添加一个延时任务时，并不是大家想象的那样，直接将延时任务添加到时间轮中，而是首先添加到一个叫做 timeouts 的 MpscQueue 中。

```
    // 多线程在向 HashedWheelTimer 添加延时任务的时候，首先会将任务添加到 timeouts 中，而不是直接添加到时间轮里
    private final Queue<HashedWheelTimeout> timeouts = PlatformDependent.newMpscQueue();
```

为什么会这么设计呢 ？ 时间轮是一个单线程驱动的模型，内部只有一个 workerThread 来推动 tick 的转动，并从对应 HashedWheelBucket 中拉取延时任务。所以时间轮采用的是无锁化的设计，workerThread 在访问内部任何数据结构的时候都不会加锁。

而向时间轮添加延时任务的操作却是多线程执行的，如果任务被直接添加到时间轮中，那么就破坏了无锁化的设计，workerThread 在访问内部相关数据结构的时候就必须加锁了。

所以为了避免加锁的开销，Netty 引入了一个 MpscQueue 作为中转，业务多线程首先会将延时任务添加到 MpscQueue 中。等到下一个 tick , workerThread 调度延时任务的时候，会统一将 MpscQueue 中的延时任务转移到时间轮中。保证了 workerThread 单线程的无锁化运行。

另外 Netty 时间轮采用了懒启动的设计，只有第一次向时间轮添加延时任务的时候才会启动。因为时间轮一旦启动之后，workerThread 就开始运行，推动 tick 一下一下的向前推进。如果时间轮刚被创建出来就启动，此时内部又没有任何延时任务，这就导致了 tick 不必要的空转。

当时间轮启动之后，就会根据延时任务 TimerTask 的延时时间 delay 计算到期时间 deadline ， 然后将 TimerTask 封装成 HashedWheelTimeout 添加到 MpscQueue 中。

```
private static final class HashedWheelTimeout implements Timeout, Runnable {
        // 延时任务所属的时间轮
        private final HashedWheelTimer timer;
        // 延时任务
        private final TimerTask task;
        // 延时任务的 deadline ，该时间是一个绝对时间，以时间轮的启动时间 startTime 为起点
        private final long deadline;

        HashedWheelTimeout(HashedWheelTimer timer, TimerTask task, long deadline) {
            this.timer = timer;
            this.task = task;
            this.deadline = deadline;
        }
}
```

之前我们提到过，HashedWheelTimeout 中最重要的一个属性就是延时任务的到期时间 deadline ， deadline 是一个绝对时间戳，Netty 时间轮中的时间坐标系全部是以时间轮的启动时间点 startTime 为基准的，deadline 表示从 startTime 这个时间点开始，到 deadline 这个时间点到期。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2B3j3q2UBIoGeib4bo9uSrLKVRoxDuvLiag9BKjRTyKV41UQgK1icfhDqg/640?wx_fmt=other&from=appmsg#imgIndex=11)

image.png

为什么这么设计呢 ？ 这是因为我们需要把时间轮的启动时间也考虑进延时时间的计算当中。比如，我们向时间轮中添加一个延时 5ms 执行的任务，其中时间轮启动花了 2ms , 那么这个延时任务就应该在时间轮启动后 3ms 开始执行。所以在计算延时任务到期时间戳 deadline 的时候需要减去时间轮的启动时间。后续时间轮的时间坐标轴均以 startTime 为基准。

```
long deadline = System.nanoTime() + unit.toNanos(delay) - startTime;
```

下面是延时任务完整的添加逻辑，整个时间复杂度为 `O(1)`  ：

```
public class HashedWheelTimer implements Timer {
    // 时间轮的启动时间戳（纳秒）
    private volatile long startTime;

    @Override
    public Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {
        // 懒启动时间轮，worker 线程会等待 100ms 后执行
        start();
        // 计算延时任务到期的时间戳
        // 时间戳的参考坐标系均以时间轮的启动时间 startTime 为起点
        long deadline = System.nanoTime() + unit.toNanos(delay) - startTime;
        HashedWheelTimeout timeout = new HashedWheelTimeout(this, task, deadline);
        timeouts.add(timeout);
        return timeout;
    }
}
```

### 3.3 延时任务的取消

延时任务的取消和添加一样，它们都是在 workerThread 之外进行操作的，所以当业务线程取消一个延时任务时，也是先将这个被取消的延时任务放到一个 MpscQueue 中暂存。

```
private final Queue<HashedWheelTimeout> cancelledTimeouts = PlatformDependent.newMpscQueue();
```

等到下一个 tick 到来的时候，workerThread 会统一处理 cancelledTimeouts 集合中已经被取消的延时任务。

```
        private void processCancelledTasks() {
            for (;;) {
                // workerThread 不断的从 cancelledTimeouts 中拉取被取消的延时任务
                HashedWheelTimeout timeout = cancelledTimeouts.poll();
                if (timeout == null) {
                    // all processed
                    break;
                }
                try {
                    // 将延时任务从 HashedWheelBucket 中删除，时间复杂度 O(1)
                    timeout.remove();
                } catch (Throwable t) {

                }
            }
        }
```

延时任务 HashedWheelTimeout 的状态一共有三个，初始为 ST\_INIT，任务被取消之后会更新为 ST\_CANCELLED，任务准备执行的时候会更新为 ST\_EXPIRED。

```
private static final class HashedWheelTimeout implements Timeout, Runnable {
        private static final int ST_INIT = 0;
        private static final int ST_CANCELLED = 1;
        private static final int ST_EXPIRED = 2;

        private volatile int state = ST_INIT;
        // 延时任务所属的时间轮
        private final HashedWheelTimer timer;

        @Override
        public boolean cancel() {
            // 更新任务状态为 ST_CANCELLED
            if (!compareAndSetState(ST_INIT, ST_CANCELLED)) {
                return false;
            }

            timer.cancelledTimeouts.add(this);
            return true;
        }
}
```

### 3.4 时间轮的启动

之前我们提过，Netty 时间轮采用了懒启动的设计，当我们首次向时间轮添加延时任务的时候才会启动。时间轮有三种状态，刚被创建出来的时候状态为 WORKER\_STATE\_INIT，启动之后状态为 WORKER\_STATE\_STARTED，关闭之后状态为 WORKER\_STATE\_SHUTDOWN。

```
public class HashedWheelTimer implements Timer {
    // 初始状态
    public static final int WORKER_STATE_INIT = 0;
    // 启动状态
    public static final int WORKER_STATE_STARTED = 1;
    // 关闭状态
    public static final int WORKER_STATE_SHUTDOWN = 2;
    // 时间轮的状态，初始为 WORKER_STATE_INIT
    private volatile int workerState; 
    // 原子更新时间轮状态的 Updater
    private static final AtomicIntegerFieldUpdater<HashedWheelTimer> WORKER_STATE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(HashedWheelTimer.class, "workerState");
    // 监听时间轮的启动事件
    private final CountDownLatch startTimeInitialized = new CountDownLatch(1);
    // 时间轮的启动时间戳（纳秒）
    private volatile long startTime;

    public void start() {
        switch (WORKER_STATE_UPDATER.get(this)) {
            case WORKER_STATE_INIT:
                if (WORKER_STATE_UPDATER.compareAndSet(this, WORKER_STATE_INIT, WORKER_STATE_STARTED)) {
                    // 启动 workerThread ， 它是一个非守护线程
                    workerThread.start();
                }
                break;
            case WORKER_STATE_STARTED:
                break;
            case WORKER_STATE_SHUTDOWN:
                throw new IllegalStateException("cannot be started once stopped");
            default:
                throw new Error("Invalid WorkerState");
        }

        // 等待 workerThread 的启动，workerThread 启动之后会设置 startTime，并执行 startTimeInitialized.countdown
        while (startTime == 0) {
            try {
                // 等待时间轮的启动
                startTimeInitialized.await();
            } catch (InterruptedException ignore) {
              
            }
        }
    }
}
```

时间轮中有一个重要的属性 startTime，初始状态下为 0 ，启动之后，workerThread 会将启动那一刻的时间戳设置到 startTime 中，这个 startTime 非常重要，因为后续时间轮中的时间坐标系均是以 startTime 为基准的。时间轮启动的一项重要工作就是设置这个 startTime。

```
private final class Worker implements Runnable {
       @Override
        public void run() {
              // 设置时间轮的启动时间戳
              startTime = System.nanoTime();
              // 通知正在等待时间轮启动的业务线程
              startTimeInitialized.countDown();

                        ..........
        }
}
```

### 3.5 时间轮的运转

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2iaSqgEY2iaKUMy1l5sfyvpicV302scSxV7poxC8dgSWUy13DBDUeUPuFg/640?wx_fmt=other&from=appmsg#imgIndex=12)

image.png

时间轮会按照一定的到期 deadline 时间范围将所有的延时任务分别打散到 512 个 HashedWheelBucket 中，比如，**我们在 `tick = 0` 这个时刻**向时间轮添加延时任务，如果这个任务的 deadline 在 `[ 0 , 100 )ms` 内，那么它将会被添加到 HashedWheelBucket0，中，如果 deadline 在  `[ 100 , 200 )ms` 内，那么就会被添加到 HashedWheelBucket1 中。同样的道理，如果 deadline 在 `[ 200, 300 )ms` 内，它将会被添加到 HashedWheelBucket2 中，以此类推。

假设当前 `tick = 2` , 那么就表示 HashedWheelBucket2 中的延时任务马上要被 workerThread 调度执行，那么具体在什么时间执行呢 ？

时间轮中的时间纪元是 `tick = 0` ，也就是从 0ms 开始， HashedWheelBucket2 中所有的延时任务，它们的 deadline 都在 `[ 200, 300 )ms` 以内。那么当 tick 从 0 转动到 2 的时候，就表示时间已经过去了 200ms。

但此时还不能马上就开始执行 HashedWheelBucket2 中的任务，因为它们的延时时间可能是 210ms , 250ms 也可能是 299ms ，如果在 tick = 2 也就是 200ms 的这个时间点就马上执行，那么这些任务就被提前执行了。

所以我们需要等到 300ms 也就是 tick = 3 这个时刻才能执行 HashedWheelBucket2 中的延时任务，**注意这里 `tick = 3` 指的是具体真实的时间已经到了 300ms 这个时间点，而时间轮中的 tick 还是指向 2 ，并没有向前推进**。

也就是说，延时 210ms , 250ms , 299ms 的任务，需要等到 300ms 之后才能得到执行，这里我们也可以看出，时间轮的精度是 tickDuration （默认 100ms），延时任务的调度通常会晚一个 100ms 左右。

> 这里提到 "100ms 左右" 的意思是，时间轮中的延时任务可能会被晚调度 5ms ,也可能晚调度 9ms ,也可能是几十毫秒，也有可能是 105ms ， 108ms , 111ms 。具体这个调度延迟的不确定性是如何产生的，我们放在下一个小节在讨论，这里大家有这个概念就可以了。

因此 workerThread 在调度延时任务的时候，通常会首先等到 next tick 的时间点来临才会开始执行当前 tick 对应的 HashedWheelBucket。

```
        private long waitForNextTick() {
            // 获取 tick + 1 对应的时间戳 deadline，后续 workerThread 会 sleep 直到 deadline 到达
            long deadline = tickDuration * (tick + 1);

            for (;;) {
                // 时间轮时间轴中的当前时间戳
                final long currentTime = System.nanoTime() - startTime;
                // 这里需要保证 workerThread 至少要 sleep 1ms ，防止其被频繁的唤醒
                long sleepTimeMs = (deadline - currentTime + 999999) / 1000000;
                // 如果 deadline 已过期，那么直接返回 currentTime
                // tick bucket 中延时任务的 deadline 小于等于 currentTime 的就会被执行
                if (sleepTimeMs <= 0) {
                    // currentTime 溢出
                    if (currentTime == Long.MIN_VALUE) {
                        return -Long.MAX_VALUE;
                    } else {
                        return currentTime;
                    }
                }

                try {
                    // sleep 等待到 deadline 时间点，然后执行当前 tick bucket 中的延时任务（timeout.deadline <= deadline）
                    Thread.sleep(sleepTimeMs);
                } catch (InterruptedException ignored) {
                    // 时间轮被其他线程关闭，中断 worker 线程
                    if (WORKER_STATE_UPDATER.get(HashedWheelTimer.this) == WORKER_STATE_SHUTDOWN) {
                        return Long.MIN_VALUE;
                    }
                }
            }
        }
```

时间轮的精度是由 tickDuration 决定的，这个值我们可以调节，默认为 100ms , 但最小不能低于 1ms 。tickDuration 越小，时间轮的精度越高，同时 workerThread 的繁忙程度也越高 。如果 tickDuration 设置的过小，那么 workerThread 在这里就会被频繁的唤醒。

所以为了防止 workerThread 被频繁的唤醒，我们需要保证至少要 sleep 1ms 。

```
long sleepTimeMs = (deadline - currentTime + 999999) / 1000000;
```

如果 `sleepTimeMs <= 0` 则说明当前时间点 currentTime 已经过了  tick + 1 对应的时间戳 deadline ， 这样就不用在这里等待了，直接返回 currentTime。

只要当前 tick 对应的 HashedWheelBucket 中的延时任务到期时间小于等于 currentTime （延时任务以过期），workerThread 会将会执行它们。

如果 `sleepTimeMs > 0` 则说明当前时间还没有到达 tick + 1 这个时间点，那么  workerThread 就会在这里睡眠等待。

当时间到达 tick + 1 这个时间点之后，workerThread 就会从这里唤醒，转去执行当前 tick 对应的 HashedWheelBucket 里的延时任务。

```
int idx = (int) (tick & mask);
HashedWheelBucket bucket = wheel[idx];
```

但 HashedWheelBucket 里面此时可能还是空的，没有任何延时任务。因为当业务线程在向时间轮添加延时任务的时候，首先是要将任务添加到一个叫做 timeouts 的 MpscQueue 中。也就是说延时任务首先会在 timeouts 中缓存，并不会直接添加到对应的 HashedWheelBucket 中，

那么 workerThread 在被唤醒之后，首先要做的就是从 timeouts 中将延时任务转移到时间轮对应的 HashedWheelBucket 中。

```
        private void transferTimeoutsToBuckets() {
            // 每个 tick 最多只能从 timeouts 中转移 10 万个延时任务到时间轮中
            // 防止极端情况下 worker 线程在这里不停地拉取任务，执行任务
            // 剩下的任务等到下一个 tick 在进行转移
            for (int i = 0; i < 100000; i++) {
                // 拉取待执行的延时任务
                HashedWheelTimeout timeout = timeouts.poll();
                // 跳过已被取消的任务
                if (timeout.state() == HashedWheelTimeout.ST_CANCELLED) {
                    continue;
                }
                // 计算延时任务最终会停留在哪个 tick 上
                // 这里的 tick 和 calculated 是一个绝对值，从 0 开始增加，只增不减
                long calculated = timeout.deadline / tickDuration;
                // 时间轮从当前 tick 开始转动到 calculated 需要经过多少个时钟周期
                timeout.remainingRounds = (calculated - tick) / wheel.length;
                // calculated < 当前 tick , 则表示延时任务 timeout 已经过期了
                // 那么就将过期的 timeout 放在当前 tick 中执行
                final long ticks = Math.max(calculated, tick);
                int stopIndex = (int) (ticks & mask);

                HashedWheelBucket bucket = wheel[stopIndex];
                bucket.addTimeout(timeout);
            }
        }
```

当任务转移完成之后，workerThread 开始处理当前 tick 对应的HashedWheelBucket，将 HashedWheelBucket 中的延时任务挨个拉取出来执行。当所有到期的延时任务被执行完之后，tick 向前推进一格，开启新一轮的循环。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2MoicJmpHFR2ib0lrk9aC6XKyLoKzXk3Szia3fIEaftm5G1IVFSULczmibQ/640?wx_fmt=other&from=appmsg#imgIndex=13)

image.png

```
private static final class HashedWheelBucket {

       public void expireTimeouts(long deadline) {
            HashedWheelTimeout timeout = head;
            // process all timeouts
            while (timeout != null) { // bucket 不为空
                HashedWheelTimeout next = timeout.next;
                if (timeout.remainingRounds <= 0) { // 属于当前时钟周期
                    next = remove(timeout); // 从 bucket 中删除
                    if (timeout.deadline <= deadline) {
                        // 执行延时任务
                        timeout.expire();
                    } else {
                        // The timeout was placed into a wrong slot. This should never happen.
                        throw new IllegalStateException(String.format(
                                "timeout.deadline (%d) > deadline (%d)", timeout.deadline, deadline));
                    }
                } else if (timeout.isCancelled()) {
                    next = remove(timeout);
                } else {
                    timeout.remainingRounds --;
                }
                timeout = next;
            }
        }
}
```

时间轮中的延时任务默认情况下是由 workerThread 执行的，但如果我们在创建时间轮的时候指定了专门的 taskExecutor ， 那么延时任务就会由这个 taskExecutor 负责执行，workerThread 只负责调度，大大减轻了 workerThread 的负荷。

```
private static final class HashedWheelTimeout implements Timeout, Runnable { 
        // 时间轮
        private final HashedWheelTimer timer;

        public void expire() {
            if (!compareAndSetState(ST_INIT, ST_EXPIRED)) {
                return;
            }

            try {
                // 时间轮的 taskExecutor 负责执行延时任务，默认为 workerThread
                timer.taskExecutor.execute(this);
            } catch (Throwable t) {
                if (logger.isWarnEnabled()) {
                    logger.warn("An exception was thrown while submit " + TimerTask.class.getSimpleName()
                            + " for execution.", t);
                }
            }
        }
}
```

下面是时间轮运转的完整逻辑流程：

```
    private final class Worker implements Runnable {
        @Override
        public void run() {
          
            do {
                // workerThread 这里会等待到下一个 tick 的时间点
                final long deadline = waitForNextTick();
                // deadline < 0 表示 currentTime 溢出
                if (deadline > 0) {
                    int idx = (int) (tick & mask);
                    // 将 cancelledTimeouts 中已经取消的 task 从对应 bucket 中删除
                    processCancelledTasks();
                    HashedWheelBucket bucket =
                            wheel[idx];
                    // 将 timeouts 中收集的延时任务添加到时间轮中
                    transferTimeoutsToBuckets();
                    // 执行当前 tick 对应 bucket 中的所有延时任务
                    bucket.expireTimeouts(deadline);
                    tick++;
                }
            } while (WORKER_STATE_UPDATER.get(HashedWheelTimer.this) == WORKER_STATE_STARTED);

                      ..............

    }
```

### 3.6 调度延迟的不确定性是如何产生的

前面我们提到过，时间轮只适合那种对延时任务的调度及时性要求没那么高的场景，Netty 时间轮的精度为一个 tickDuration，默认为 100ms 。延时任务的调度通常会晚 100ms 左右。

比如，现在我们向时间轮添加一个延时 5ms 之后执行的任务，那么这个延时任务可能会在 8ms 之后执行，也可能是  65ms  之后执行，也有可能在 108ms 之后执行。总之，时间轮调度的延迟范围会在 100ms 左右。那为什么会出现这种不确定性呢 ？

这其中主要有两个原因，首先第一个原因就是时间轮的延时任务太多，延时任务的逻辑比较复杂，执行时间略长，导致了 workerThread 的阻塞，从而造成了任务调度的延迟。减缓这种情况的一个措施就是在创建时间轮的时候，我们可以指定一个自定义的 taskExecutor 来专门负责延时任务的执行，减轻 workerThread 的负荷。或者增大 HashedWheelBucket 的个数，尽量的分散延时任务，不要让它们集中在某一个 HashedWheelBucket 中。

第二个原因是要看我们究竟在哪一个时间点向时间轮添加延时任务。不同的添加时机，也会造成调度的不确定性。这可能有点难以理解，我们来看一个具体的例子。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2lryOB0iarTXhLWyOctMicibZKqS2Q9QSH4BricxXbUbBUnc7FpfpP6ia2Yw/640?wx_fmt=other&from=appmsg#imgIndex=14)

image.png

比如，我们在下图时间轴中 1ms 这个时刻向时间轮添加一个延时 5ms 执行的任务。当前时间轮如上图所示，tick 指向 0 。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2ibduzbMMQ3EZYaQDIBia9eOgXoBJ0F2CRr5iamPM5OFzE6TibibGViash4Cg/640?wx_fmt=other&from=appmsg#imgIndex=15)

image.png

延时 5ms 的任务会被添加到 HashedWheelBucket0 中，此时 workerThread 还在 sleep 等待 next tick 也就是 100ms 这个时间点的到来。

我们在  1ms 这个时刻添加的这个延时任务本来应该在时间轴中的 6ms 这个时间点执行，但是现在 workerThread 还在睡眠，需要等到 100ms 这个时间点才能被唤醒去执行 HashedWheelBucket0 中的延时任务。这就产生了 90 ms 的调度延时。

但如果我们在时间轴的 94ms 位置处添加这个 5ms 的延时任务，那么这个延时任务本应该在时间轴的 99ms 这个时间点被执行，但由于 workerThread 在 100ms 这个时间点才会被唤醒，所以产生了 1ms 的调度延时。

如果非常不幸，我们恰好卡在了时间轴 95ms 这个时间点添加这个 5ms 的延时任务，此时要注意，这个延时任务会被放在 HashedWheelBucket1 中而不是 HashedWheelBucket0。

而 HashedWheelBucket1 中的延时任务，workerThread 需要等到时间轴 200ms 这个时间点才会去执行，这样一来，本应该在 100ms 这个时间点执行的延时任务，时间轮却在 200ms 这个时间点来调度，这就产生了 100ms 的调度延时。如果在算上 CPU 调度 workerThread 的时间，那么这个延迟可能就在 105ms 或者 108ms 左右。这里大家可以对照上小节的内容，仔细想想是不是这么回事。

### 3.7 时间轮的关闭

时间轮定义了三种状态，在刚被创建出来的时候状态为 WORKER\_STATE\_INIT，启动之后状态为 WORKER\_STATE\_STARTED。

```
public class HashedWheelTimer implements Timer {
    // 初始状态
    public static final int WORKER_STATE_INIT = 0;
    // 启动状态
    public static final int WORKER_STATE_STARTED = 1;
    // 关闭状态
    public static final int WORKER_STATE_SHUTDOWN = 2;
    // 时间轮的状态，初始为 WORKER_STATE_INIT
    private volatile int workerState; 
}
```

当时间轮要关闭的时候，我们就需要将 workerState 更新为 WORKER\_STATE\_SHUTDOWN。当 workerThread 检测到时间轮的状态不是 WORKER\_STATE\_STARTED 的时候，就会退出 `do ... while` 循环，停止时间轮的转动。

随后 workerThread 会将当前时间轮中所有 HashedWheelBuckets 中遗留的（未来得及执行，未取消）的延时任务以及 timeouts 队列中缓存的待执行延时任务（未取消）统统转移到 **unprocessedTimeouts 集合**中。并将已经取消的延时任务从对应 HashedWheelBucket 中删除。

```
private final class Worker implements Runnable {

        private final Set<Timeout> unprocessedTimeouts = new HashSet<Timeout>();

        @Override
        public void run() {
            do {

                      ........ 时间轮运转逻辑 .......

            } while (WORKER_STATE_UPDATER.get(HashedWheelTimer.this) == WORKER_STATE_STARTED);


            for (HashedWheelBucket bucket: wheel) {
                // 将 bucket 中还没来得及执行并且没有被取消的任务转移到 unprocessedTimeouts
                bucket.clearTimeouts(unprocessedTimeouts);
            }
            // 将 timeouts 中缓存的待执行任务（没有被取消）转移到 unprocessedTimeouts
            for (;;) {
                HashedWheelTimeout timeout = timeouts.poll();
                if (timeout == null) {
                    break;
                }
                if (!timeout.isCancelled()) {
                    unprocessedTimeouts.add(timeout);
                }
            }
            // 将 cancelledTimeouts 中的延时任务从对应 bucket 中删除
            processCancelledTasks();
        }
}
```

最后 Netty 会将 unprocessedTimeouts 集合中收集到的那些还未来得及执行的延时任务全部取消，然后将这些延时任务返回给业务线程，由业务线程自行处理。

时间轮关闭的完整流程总结如下：

1.  在延时任务中不能执行时间轮关闭的操作。
    
2.  原子更新时间轮的状态为 WORKER\_STATE\_SHUTDOWN
    
3.  中断 workerThread，并等待 workerThread 结束。
    
4.  取消所有还未来得及调度的延时任务，并返回给业务线程。
    

```
public class HashedWheelTimer implements Timer {
    @Override
    public Set<Timeout> stop() {
        // 在延时任务中不能执行停止时间轮的操作
        if (Thread.currentThread() == workerThread) {
            throw new IllegalStateException(
                    HashedWheelTimer.class.getSimpleName() +
                            ".stop() cannot be called from " +
                            TimerTask.class.getSimpleName());
        }
        // 停止时间轮
        if (!WORKER_STATE_UPDATER.compareAndSet(this, WORKER_STATE_STARTED, WORKER_STATE_SHUTDOWN)) {
            // cas 更新状态失败，这里时间轮的状态可能会是两种：
            // 1. WORKER_STATE_INIT 还未启动
            // 2. WORKER_STATE_SHUTDOWN 已经停止
            if (WORKER_STATE_UPDATER.getAndSet(this, WORKER_STATE_SHUTDOWN) != WORKER_STATE_SHUTDOWN) {
                // 如果时间轮还未启动，那么直接停止就好了
                if (leak != null) {
                    boolean closed = leak.close(this);
                    assert closed;
                }
            }
            // 时间轮没有启动，那么肯定也就没有延时任务，这里直接返回一个空集合就行
            return Collections.emptySet();
        }

        try {
            boolean interrupted = false;
            while (workerThread.isAlive()) {
                // 中断 workerThread，使其从 sleep 中唤醒，执行时间轮关闭的逻辑
                // 如果 workerThread 在运行，那么此时时间轮已经是 WORKER_STATE_SHUTDOWN 状态
                // workerThread 会退出 do while 循环转去执行时间轮的关闭逻辑
                workerThread.interrupt();
                try {
                    // 等待 workerThread 的结束
                    workerThread.join(100);
                } catch (InterruptedException ignored) {
                    // 当前线程被中断
                    interrupted = true;
                }
            }

            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        } finally {
            if (leak != null) {
                // 关闭资源泄露探测
                boolean closed = leak.close(this);
                assert closed;
            }
        }
        // 获取还未来得及处理的延时任务
        Set<Timeout> unprocessed = worker.unprocessedTimeouts();
        Set<Timeout> cancelled = new HashSet<Timeout>(unprocessed.size());
        // 将还未来得及处理的任务全部取消，然后返回
        for (Timeout timeout : unprocessed) {
            if (timeout.cancel()) {
                cancelled.add(timeout);
            }
        }
        return cancelled;
    }
}
```

## 4\. 单层时间轮的设计缺陷

Netty 采用的是单层时间轮的设计，时钟间隔 tickDuration 为 100ms , 一共 512 个刻度。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2dXO0dwGj18CIuYibmUL0r8Y1qTczhI8auxGR4zuS7ald3k1ic68gX9iaQ/640?wx_fmt=other&from=appmsg#imgIndex=16)

image.png

workerThread 每隔 100ms 将时钟 tick 向前推进一格，并执行对应 bucket 中的延时任务。但如果长时间没有任务过期的话，那么时钟 tick 就会一直空转，造成不必要的性能损耗。

比如，我们在 `tick = 0` 这个时刻向时间轮分别添加延时 100ms , 延时 51100ms 的定时任务，它们会被添加到 HashedWheelBucket1， HashedWheelBucket511 中，剩下的 HashedWheelBucket 全部都是空的。

当 100ms 的延时任务被执行完之后，时钟 tick 将不得不一直空转，直到空转到 511 这个刻度，才会去执行 51100ms 的延时任务。当这两个延时任务被执行完之后，如果不在向时间轮添加新的任务，那么时间轮将会一直空转下去。

除了会有时钟 tick 空转的现象之外，单层时间轮还无法高效应对延时时间跨度比较大的定时任务。比如，定时任务的延时时间横跨多个时钟周期。Netty 时间轮的时钟周期为 51200ms ，假设我们在 `tick = 1` 这个时刻向时间轮添加三个延时任务，它们的延时时间分别为：51200ms , 2 \* 51200ms , 3 \* 51200ms 。

这三个延时任务都会被添加到 HashedWheelBucket1 中，但它们的 `remainingRounds` 都是大于 0 的。

```
private static final class HashedWheelTimeout {
    // 执行该延时任务需要经过多少时钟周期
    long remainingRounds;
}
```

虽然在当前时钟周期内，HashedWheelBucket1 中没有任何可以执行的延时任务，但 workerThread 仍然需要将 HashedWheelBucket1 中的延时任务全部遍历一遍。如果像这样的延时任务非常多，那么 workerThread 遍历 HashedWheelBucket 的操作也会产生不必要的性能开销。

综上所述，单层时间轮的设计缺陷总结下来主要有两个方面：

1.  在长时间没有到期任务的情况下，单层时间轮会有时钟 tick 空转的现象。
    
2.  面对海量延时时间跨度比较大的定时任务场景，除了时钟 tick 空转之外，还会产生不必要的 HashedWheelBucket 遍历开销。
    

而 Kafka 中的多层时间轮设计则完美地解决了上面的这两个缺陷，那怎么解决的呢 ？ 下面就让我们带着这两个疑问来深入探究一下 Kafka 多层时间轮的设计与实现。

## 5\. Kafka 多层时间轮的设计

如下图所示，Kafka 时间轮的设计模型可以说是和 Netty 时间轮的设计非常相似。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2YslE6wzFmicPzxxsxp6dQCqJQOsVtY64NiaDuLplIFlSh233y5VOlT0Q/640?wx_fmt=other&from=appmsg#imgIndex=17)

image.png

时间轮的表盘都是一个环形数组，Kafka 中的刻度为 20 ，Netty 中的刻度为 512 。

```
// Kafka
public class TimingWheel {
    private final TimerTaskList[] buckets;
}

// Netty
public class HashedWheelTimer implements Timer {
    private final HashedWheelBucket[] wheel;
}
```

Netty 使用的是 HashedWheelBucket 结构来描述时间轮中的刻度，它是一个带有头尾指针的双向链表。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2YwWcEsA0cT745d2mluFDE3nsq7viaMVQxs7wibfKyWN4Gmzd1icSF5ibjw/640?wx_fmt=other&from=appmsg#imgIndex=18)

image.png

```
    private static final class HashedWheelBucket {
        private HashedWheelTimeout head;
        private HashedWheelTimeout tail;
    }
```

Kafka 则使用 TimerTaskList 结构来描述时间轮中的刻度，它是一个带有头结点的双向循环链表。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2efl29Fg4pE7aGKn9xvcdm9aBWLicT758ZRqZQy8lhuC3mUDe6XfDguQ/640?wx_fmt=other&from=appmsg#imgIndex=19)

image.png

```
class TimerTaskList implements Delayed {
    // 头结点
    private final TimerTaskEntry root;

    TimerTaskList(
        AtomicInteger taskCounter,
        Time time
    ) {
        this.root = new TimerTaskEntry(null, -1L);
        this.root.next = root;
        this.root.prev = root;
    }
}
```

时间轮的延时任务在 Netty 中是用 HashedWheelTimeout 结构来表示。

```
private static final class HashedWheelTimeout implements Timeout, Runnable {
        private final TimerTask task;
        private final long deadline;
        HashedWheelTimeout next;
        HashedWheelTimeout prev;
}
```

而在 Kafka 中则是用 TimerTaskEntry 结构来表示，可以看出两种结构是非常相似的，只不过是名字变了一下而已。

```
public class TimerTaskEntry {
    public final TimerTask timerTask;
    public final long expirationMs;
    TimerTaskEntry next;
    TimerTaskEntry prev;
}
```

Netty 时间轮中的时钟间隔是用 tickDuration 来表示，默认为 100ms 。Kafka 中的时钟间隔是用 tickMs 来表示，默认为 1ms 。

```
// Netty
private final long tickDuration;
// Kafka
private final long tickMs;
```

Netty 中的时钟周期是 51200ms , Kafka 中的时钟周期是 20ms 。Netty 时间轮的转动是一个 tick 一个 tick 地向前转动，时间每经过一个 tickDuration，时钟 tick++ 。tick 是一个绝对值，从 0 开始递增，只增不减。

```
// Netty
private long tick;
// Kafka
private long currentTimeMs;
```

而 Kafka 时钟是由 currentTimeMs 来表示的，与 Netty 不同的是，currentTimeMs 是系统当前时间的一个时间戳，单位为毫秒。时间每经过 tickMs，currentTimeMs 就增加 tickMs。Kafka 时间轮的转动是通过更新 currentTimeMs 时间戳来驱动的。

以上就是 Kafka 与 Netty 在时间轮的设计上比较相似的地方，那么不同的地方在哪里呢 ？

我们都知道 Kafka 采用的是多层时间轮的设计，主要是为了应对海量延时时间跨度比较大的定时任务场景。 Kafka 中的时间轮是按需添加的，在初始的时候只有一个时间轮（第一层），如下图所示：

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2YslE6wzFmicPzxxsxp6dQCqJQOsVtY64NiaDuLplIFlSh233y5VOlT0Q/640?wx_fmt=other&from=appmsg#imgIndex=20)

image.png

第一层时间轮的精度，也就是时钟间隔 tickMs 为 1ms , 一共 20 个刻度，时钟周期 interval 为 20ms 。和 Netty 一样，时间轮中每个 bucket (TimerTaskList) 负责存放到期时间在相同范围内的延时任务 TimerTaskEntry。

比如我们以时间轮的初始状态为例， Kafka 第一层时间轮的环形数组 buckets\[0\] 中，存放的是到期时间在`[ 0 , 1 ) ms` 内的延时任务，buckets\[1\] 中存放的是到期时间在 `[ 1 , 2 ) ms` 内的延时任务，buckets\[19\] 中存放的是到期时间在 `[ 19 , 20) ms` 内的延时任务。

假设现在我们要向时间轮中添加一个延时 28ms 执行的定时任务。很明显，它已经超过了第一层时间轮的时钟周期 —— 20ms，所以我们应该将这个定时任务添加到第二层时间轮中。

前面我们刚刚提过，Kafka 的多层时间轮是按需创建的，初始的时候只有一层时间轮，这时我们就需要创建第二层时间轮，每一层时间轮 TimingWheel 中有一个字段 overflowWheel 用于指向其上层时间轮。

```
public class TimingWheel {
    // 上层时间轮
    private volatile TimingWheel overflowWheel = null;
}
```

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF26sDoBVIx3mib0EibpQeNQz0XiawcwCIY7ibYEicX824S6b5UmnxSU1SuneQ/640?wx_fmt=other&from=appmsg#imgIndex=21)

image.png

第二层时间轮的时钟间隔 tickMs 就是第一层时间轮的时钟周期 interval 为 20ms , 时钟刻度仍然是 20 ， 所以第二层时间轮的时钟周期 interval 为 400ms。这个延时 28ms 执行的定时任务将会添加到第二层时间轮的 buckets\[1\] 中。

假设现在我们又向时间轮中添加一个延时 450ms 执行的定时任务，然而第二层时间轮的时钟周期却是 400ms , 无法满足这个延时时间，所以我们就需要创建第三层时间轮。将这个延时 450ms 的定时任务添加到第三层时间轮中。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2kuia8qJTE0Nu4wM2icico7sZKX6Ymj5xDtczia7HhRHs8MDtXq3sMXT15Q/640?wx_fmt=other&from=appmsg#imgIndex=22)

image.png

同样的道理，第三层时间轮的时钟间隔 tickMs 又是第二层时间轮的时钟周期 interval 为 400ms , 时钟刻度仍然是 20 ， 所以第三层时间轮的时钟周期 interval 为 8000ms。这个延时 450ms 执行的定时任务将会添加到第三层时间轮的 buckets\[1\] 中。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2wVNqeUABddAKwkicXQsRK4icRe0y2TkM9aNObdMicKayibBaia0qIWPcNAQ/640?wx_fmt=other&from=appmsg#imgIndex=23)

image.png

Kafka 并没有限制时间轮的层数，每当最高层时间轮的时钟周期 interval 无法满足延时任务的延时时间 delayMs 时，Kafka 就会按需创建一个更高层的时间轮，该时间轮的时钟间隔 tickMs 就是其下一层时间轮的时钟周期 interval。

Kafka 的多层时间轮设计与我们日常生活中的钟表非常的相似，笔者以三层时间轮进行类比说明：

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2hd8oyhMsEEylkf2KRWghAZqIibMf2Mw0oWmSSv9icAc2bawBDNEz10Yg/640?wx_fmt=other&from=appmsg#imgIndex=24)

image.png

第一层时间轮可以看做是只有秒针的一个表盘，秒表的时钟间隔 tickMs 就是 1s , 时钟周期 interval 是 60s。

第二层时间轮可以看做是只有分针的一个表盘，时钟间隔 tickMs 是 60s , 正好是秒表的时钟周期。分表的时钟周期 interval 是 60m 。

第三层时间轮可以看做是只有时针的一个表盘，时钟间隔 tickMs 是 60m，正好是分表的时钟周期。时表的时钟周期 interval 是 12h 。

秒针每走一个刻度就是一秒，秒针走完一个时钟周期，分针走一个刻度就是一分钟，当分针走完一个时钟周期，时针走一个刻度就是一个小时。

同样的道理， Kafka 第一层时间轮走一个刻度，就是 1ms , 走完一个时钟周期就是 20ms 。随后第二层时间轮走一个刻度，当第二层时间轮走完一个时钟周期（400ms） 之后，第三层时间轮走一个刻度。第三层时间轮走完一个时钟周期就是 8000ms 。

但是这样的设计只是解决了延时任务跨度比较大的问题，却无法解决 Netty 时间轮中存在的空推进现象。假设，我们现在一共有三层时间轮，但是却没有一个延时任务。这种情况下，每一层时间轮也还是会一下一下的向前推进，但是却执行不到任何延时任务。产生了更加严重的空推进问题。

那如何解决呢 ？ 我们是不是应该换一种思路去想，如果时间轮中没有任何延时任务我们就不推进，仿佛时间静止了一样，因为在这种情况下继续推进时间轮没有任何意义。

如果时间轮中有延时任务，那我们也不会一个刻度一个刻度地推进，而是等到延时任务到期之后，才去推进时间轮。这样一来，空推进的问题不就解决了吗 ？

设计思路确定了，那么如何实现呢 ？ 核心问题就是有没有一种数据结构，可以将延时任务按照过期时间由近及远地组织起来，如果没有任何任务过期，时间轮就阻塞在这个数据结构上，如果有任务过期，时间轮就从这个数据结构上唤醒，然后执行延时任务，并将时间轮的 currentTimeMs 直接推进到任务的过期时间点。

这种数据结构不就是笔者在本文 1.2 小节中介绍的 DelayQueue 吗 ？但是如果我们用 DelayQueue 来组织时间轮中的所有延时任务的话，那么又会遇到同样的问题，就是面对海量延时任务的添加，取消时，DelayQueue 的时间复杂度太高 —— `O(logn)`。

我们可以进一步折中设计一下，仔细观察一下时间轮的设计特点，每一个 bucket (TimerTaskList) 存放的是过期时间在相同范围内的任务。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2YslE6wzFmicPzxxsxp6dQCqJQOsVtY64NiaDuLplIFlSh233y5VOlT0Q/640?wx_fmt=other&from=appmsg#imgIndex=25)

image.png

我们还是以时间轮的初始状态为例进行说明，每个 bucket 都有一个固定的过期时间范围 —— \[ startTime , endTime ) , 延时任务的过期时间只要在这个范围之内，那么他们都会被组织在同一个 bucket 中。

延时任务有过期时间的概念，而 bucket 是用来组织延时任务的，那么我们反过来思考一下，bucket 自然也就具备了过期时间的概念。在 Kafka 中，bucket 的过期时间就是对应的 startTime。

```
class TimerTaskList implements Delayed {
    // 过期时间
    private final AtomicLong expiration;
}
```

当时间到达 startTime 这个时刻之后，Kafka 就会将对应 bucket 中的延时任务挨个拿出来进行处理，这就是多层时间轮整体的一个运转思路。

既然 bucket 有过期时间的概念，那么我们何不用 DelayQueue 只保存 bucket ，而时间轮中 bucket 的数量是很少的。比如，单层时间轮只有 20 个 bucket，双层时间轮也才 40 个 bucket，三层时间轮也只有 60 个 bucket。

用 DelayQueue 来组织时间轮中所有的 bucket ， 时间复杂度 `O(logn)` 就可以忽略不计了，因为 bucket 的数量太少了。

这样一来，当时间轮中没有任何到期的 bucket 时，时间轮将会一直阻塞在 DelayQueue 上，不会向前推进，这样就避免了无意义的空推进现象。当 DelayQueue 中有 bucket 到期时，时间轮被唤醒，将 currentTimeMs 推进到过期的时间点  expiration ， 然后将到期 bucket 中的延时任务挨个拿下来处理。

笔者还是以之前的例子进行说明，假设我们现在有三层时间轮，初始状态下，这三层时间轮的 currentTimeMs 均指向 0ms 这个时刻，时间轮中有两个延时任务，分别在 28ms ， 450ms 之后执行。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2kuia8qJTE0Nu4wM2icico7sZKX6Ymj5xDtczia7HhRHs8MDtXq3sMXT15Q/640?wx_fmt=other&from=appmsg#imgIndex=26)

image.png

其中延时  28ms 之后执行的任务被存放在第二层时间轮的 bucket\[1\] 中，它的到期时间是 20ms 。延时 450ms 之后执行的任务被存放在第三层时间轮的 bucket\[1\] 中，它的到期时间是 400ms。

在 20ms 之内，时间轮就一直在 DelayQueue 上阻塞，不会向前空推进。当时间达到 20ms 这个时刻之后，第二层时间轮 bucket\[1\] 到期，时间轮从 DelayQueue 上唤醒。

时间轮的推进首先会从第一层时间轮开始，将第一层时间轮的 currentTimeMs 向前推进到 20ms 这个时刻，接着将第二层时间轮的 currentTimeMs 也向前推进到 20ms 这个时刻。

第三层时间轮的 currentTimeMs 保持不变，仍然停留在 0ms 这个时刻。因为第三层时间轮的时钟间隔 tickMs 是 400ms , 20ms 还不足以让第三层时间轮向前推进一格。

时间轮向前推进完毕之后，接下来 Kafka 就会处理到期的 bucket，就是第二层时间轮的 bucket\[1\] ，其中只有一个任务，它是延时 28ms 之后执行。但现在才过去 20ms ，还有 8ms 才能执行这个延时任务。所以 Kafka 会先将这个任务从第二层时间轮的 bucket\[1\] 中摘下，重新按照延时 8ms 执行的延时任务插入到第一层时间轮的 bucket\[8\] 中，然后将 bucket\[8\]  放入到 DelayQueue 中。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2kUH1HhpjQHaNYo5y9DAbLcwalpKbLMl8cfX00u0UgbE5YpshkUE0HA/640?wx_fmt=other&from=appmsg#imgIndex=27)

image.png

此时 DelayQueue 中有两个 bucket，一个是第一层时间轮的 bucket\[8\]，8ms 之后到期，另一个是之前第三层时间轮的 bucket\[1\]，380ms 之后到期（因为时间已经过了 20ms）。

还是同样的推进逻辑，当时间过了 8ms 之后，第一层时间轮的 bucket\[8\] 到期，时间轮从 DelayQueue 中唤醒，还是从第一层时间轮开始推进，不过此时经过上一轮的推进，第一层时间轮的 currentTimeMs 现在停留在 20 ms 这个时刻，所以首先需要将第一层的 currentTimeMs 向前推进到 28ms 。

第二层时间轮当前的 currentTimeMs 也是停留在 20 ms 这个时刻，而第二层时间轮的时钟间隔是 20ms , 但此时只是过了 8ms ，所以第二层的 currentTimeMs 不会向前推进，仍然是 20ms 。同理第三层时间轮的  currentTimeMs 也不会向前推进，仍然停留在 0ms 这个时刻。

当新一轮的推进工作完成之后，Kafka 就会着手处理过期的 bucket\[8\]（第一层），此时 bucket\[8\] 中的延时任务也已经到期，所以 Kafka 会将 bucket\[8\] 中的所有延时任务摘下挨个执行。此时第一层时间轮 currentTimeMs 指向 28ms 这个时刻，一开始加入时间轮的这个延时 28ms 的任务刚好被执行。

当时间来到了 400ms 这个时刻，第三层时间轮的 bucket\[1\] 到期，Kafka 再一次从 DelayQueue 中唤醒，首先将第一层时间轮的 currentTimeMs 推进到 400ms 这个时刻，然后将第二层时间轮的 currentTimeMs 也推进到 400ms 这个时刻。

由于 400ms 刚好满足第三层时间轮的时钟间隔 tickMs（400ms），所以第三层时间轮的 currentTimeMs 也推进到 400ms 这个位置。但 bucket\[1\] 中的延时任务是要延时 450ms 之后执行，现在时间才过了 400ms ，还有 50ms 才能执行该任务。

所以需要将该任务近一步降级到低层时间轮中，延时任务从高层时间轮降级到低层时间轮的逻辑是：

1.  首先尝试将延时任务降级到第一层时间轮中，但第一层时间轮的时钟周期是 20ms ，无法满足延时 50ms 的任务。接着尝试降级到第二层时间轮中。
    
2.  第二层时间轮的时钟周期是 400ms ， 可以满足延时 50ms 的任务，于是将该任务重新添加到第二层时间轮的 bucket\[2\] 中。然后将 bucket\[2\] 添加到 DelayQueue 中。
    

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2AH7kQsC8fX50V5Zs6Gf8oa5O3TWqsxg4gjmXgaFdSD2ia324rnCDS3A/640?wx_fmt=other&from=appmsg#imgIndex=28)

image.png

此时 DelayQueue 中就只包含一个 bucket 了，这个 bucket 就是第二层时间轮的 bucket\[2\]，它在 40ms 之后过期。此时三层时间轮的 currentTimeMs 都是指向 400ms 这个时刻。

当时间在一次经过 40ms 之后，bucket\[2\] 到期，第一层时间轮的 currentTimeMs 从 400ms 推进到 440ms ，第二层时间轮的 currentTimeMs 也从 400ms 推进到 440ms，指向 bucket\[2\] 。由于 40ms 不满足第三层时间轮的时钟间隔（400ms），所以第三层的 currentTimeMs 仍然停留在 400ms 。

然而 bucket\[2\] 中的任务是要在 50ms 之后才能执行，但现在才过了 40ms ，还有 10ms 才能执行该任务。所以需要近一步降级，而第一层时间轮的时钟周期（20ms）刚好能满足这个延时时间（10ms）, 所以该任务会被重新插入到第一层时间轮的 bucket\[10\] 中。随后 bucket\[10\] 被加入到 DelayQueue 中。

当时间在一次经过 10ms 之后，DelayQueue 中的 bucket\[10\]（第一层）到期，第一层时间轮的 currentTimeMs 从 440ms 推进到 450ms 这个时刻，指向 bucket\[10\] 。由于 10ms 不满足第二层时间轮的时钟间隔（20ms），所以第二层的 currentTimeMs 仍然停留在 440ms。同理第三层的 currentTimeMs 仍然停留在 400ms 。

当时间轮向前推进的工作结束之后，Kafka 就开始处理过期的 bucket\[10\]，其中需要延时 10ms 执行的任务也已经过期，所以 Kafka 会将 bucket\[10\] 中的所有延时任务挨个摘下执行。那么一开始加入时间轮的这个需要延时 450ms 之后执行的任务就刚好被执行了。

那么到现在为止，笔者在第 4 小节最后提出的那两个问题就完美的解决了，总结一下就是 Kafka 通过引入多层时间轮的设计解决了海量延时时间跨度比较大的定时任务场景。通过引入 DelayQueue 解决了时间轮空推进的问题。

## 6\. Kafka 多层时间轮相关设计模型的实现

经过上一小节的介绍，现在我们已经对 Kafka 多层时间轮的设计要点以及众多模型概念非常熟悉了，那么在此基础之上，在回头来看源码实现就很清晰明了了，下面笔者先从 Kafka 的时间轮模型 SystemTimer ，TimingWheel 开始介绍。

### 6.1 时间轮的创建

SystemTimer 用于组织管理多层时间轮，核心属性如下：

```
public class SystemTimer implements Timer {
    // 用于执行延时任务的 Executor
    private final ExecutorService taskExecutor;
    // 组织时间轮中所有的 TimerTaskList
    private final DelayQueue<TimerTaskList> delayQueue;
    // 时间轮中管理的延时任务个数
    private final AtomicInteger taskCounter;
    // 指向第一层时间轮，初始状态下只有一层时间轮，后续按需创建
    private final TimingWheel timingWheel;
}
```

taskExecutor 用于执行多层时间轮中的延时任务，它是一个单线程的 FixedThreadPool。

delayQueue 用于组织管理多层时间轮中的所有 TimerTaskList，按照 TimerTaskList 的到期时间 expiration ，由近及远的排列。主要用于防止时间轮空推进的现象。

taskCounter 用于统计多层时间轮中总共管理的延时任务个数。

timingWheel 用于指向第一层时间轮，在初始状态下，只有一层时间轮，后续会根据延时任务的时间跨度按需创建多层时间轮。

```
public class SystemTimer implements Timer {

    public SystemTimer(String executorName) {
        this(executorName, 1, 20, Time.SYSTEM.hiResClockMs());
    }

    public SystemTimer(
        String executorName,
        long tickMs,
        int wheelSize,
        long startMs
    ) {
        this.taskExecutor = Executors.newFixedThreadPool(1,
            runnable -> KafkaThread.nonDaemon(SYSTEM_TIMER_THREAD_PREFIX + executorName, runnable));
        this.delayQueue = new DelayQueue<>();
        this.taskCounter = new AtomicInteger(0);
        this.timingWheel = new TimingWheel(
            tickMs, // 1ms
            wheelSize, // 20
            startMs, // 时间轮创建那一刻的时间戳
            taskCounter,
            delayQueue
        );
    }
}
```

TimingWheel 则是真正的时间轮模型，其核心结构如下图所示：

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2YslE6wzFmicPzxxsxp6dQCqJQOsVtY64NiaDuLplIFlSh233y5VOlT0Q/640?wx_fmt=other&from=appmsg#imgIndex=29)

image.png

```
public class TimingWheel {
    // 时间轮的时钟间隔，第一层时间轮的 tickMs 为 1ms
    private final long tickMs;
    // 时间轮的刻度，默认为 20 
    private final int wheelSize;
    // 延时任务个数在多层时间轮中的全局计数
    private final AtomicInteger taskCounter;
    // 多层时间轮全局只有一个 DelayQueue 实例，组织管理所有 TimerTaskList
    private final DelayQueue<TimerTaskList> queue;
    // 时间轮的时钟周期，同时也是其上一层时间轮的时钟间隔 tickMs
    // 第一层时间轮的 interval 为 20ms
    private final long interval;
    // 时间轮的表盘，环形数组
    private final TimerTaskList[] buckets;
    // 时间轮的指针，初始为创建时间轮时候的时间戳，它是一个绝对值，只增不减
    private long currentTimeMs;
    // 用于指向其上一层时间轮，按需创建
    private volatile TimingWheel overflowWheel = null;
}
```

Kafka 时间轮中的指针是 currentTimeMs，它是一个绝对时间戳，初始为时间轮创建时候的时间戳 —— `Time.SYSTEM.hiResClockMs()`, 单位为毫秒。由于时间轮是根据 tickMs 来一下一下的转动，所以 currentTimeMs 必须是 tickMs 的整数倍。

```
    TimingWheel(
        long tickMs,
        int wheelSize,
        long startMs,
        AtomicInteger taskCounter,
        DelayQueue<TimerTaskList> queue
    ) {
        this.tickMs = tickMs;
        this.wheelSize = wheelSize;
        this.taskCounter = taskCounter;
        this.queue = queue;
        this.buckets = new TimerTaskList[wheelSize];
        this.interval = tickMs * wheelSize;
        // currentTimeMs 必须是 tickMs 的整数倍
        this.currentTimeMs = startMs - (startMs % tickMs);

        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new TimerTaskList(taskCounter);
        }
    }
```

TimerTaskList 结构来描述时间轮中的刻度，它是一个带有头结点的双向循环链表。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2efl29Fg4pE7aGKn9xvcdm9aBWLicT758ZRqZQy8lhuC3mUDe6XfDguQ/640?wx_fmt=other&from=appmsg#imgIndex=30)

image.png

```
class TimerTaskList implements Delayed {
    // 时间工具类
    private final Time time;
    // 全局延时任务个数统计
    private final AtomicInteger taskCounter;
    // TimerTaskList 的过期时间
    private final AtomicLong expiration;
    // 双向循环链表的头结点
    private final TimerTaskEntry root;

    TimerTaskList(
        AtomicInteger taskCounter,
        Time time
    ) {
        this.time = time;
        this.taskCounter = taskCounter;
        // 初始状态下，一个空的 TimerTaskList，它的 expiration 为 -1
        // 表示未使用，不会加入到 DelayQueue
        this.expiration = new AtomicLong(-1L);
        this.root = new TimerTaskEntry(null, -1L);
        this.root.next = root;
        this.root.prev = root;
    }
}
```

### 6.2 延时任务的添加

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2kuia8qJTE0Nu4wM2icico7sZKX6Ymj5xDtczia7HhRHs8MDtXq3sMXT15Q/640?wx_fmt=other&from=appmsg#imgIndex=31)

image.png

由于 Kafka 多层时间轮的设计，所以在延时任务添加这一块会比 Netty 的单层时间轮更加复杂一点，因为会涉及到延时任务的升级。另外当 DelayQueue 中的  TimerTaskList 到期的时候，如果 TimerTaskList 中的延时任务还没到期，也会涉及到延时任务的降级，那么在降级的过程中延时任务会被添加到低层时间轮中。

```
public class SystemTimer implements Timer {

    public void add(TimerTask timerTask) {
        readLock.lock();
        try {
            // 将延时任务 TimerTask 封装成 TimerTaskEntry 添加到时间轮中
            addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs()));
        } finally {
            readLock.unlock();
        }
    }

    private void addTimerTaskEntry(TimerTaskEntry timerTaskEntry) {
        // 尝试向时间轮中添加延时任务
        // 返回 false 表示延时任务已经被取消或者到期
        // 返回 true 表示延时任务添加成功
        if (!timingWheel.add(timerTaskEntry)) {
            // Already expired or cancelled
            if (!timerTaskEntry.cancelled()) {
                // 如果延时任务到期，则立马执行延时任务
                taskExecutor.submit(timerTaskEntry.timerTask);
            }
        }
    }
}
```

无论是在升级还是降级的过程中，如果发现延时任务已经到期，那么 Kafka 就会立即执行延时任务，延时任务的执行由单线程的 FixedThreadPool 负责。

无论是延时任务的添加还是延时任务从高层时间轮降级到低层时间轮，Kafka 首先都是从第一层时间轮开始尝试添加延时任务。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2YslE6wzFmicPzxxsxp6dQCqJQOsVtY64NiaDuLplIFlSh233y5VOlT0Q/640?wx_fmt=other&from=appmsg#imgIndex=32)

image.png

第一层时间轮的时钟间隔 tickMs 是 1ms , 一共 20 个 TimerTaskList，时钟周期为 20ms。每个 TimerTaskList 都有一个延时任务的到期时间范围 \[ startTime , endTime ) 。

我们以 `currentTimeMs = 0ms` 这个时刻为例，TimerTaskList0 表示的延时任务到期时间范围是 `[ 0ms , 1ms)` ，过期时间在这个范围内的延时任务都会被添加到 TimerTaskList0 中。同理，TimerTaskList1 表示的时间范围为  `[ 1ms , 2ms)` ， TimerTaskList19 表示的时间范围为  `[ 19ms , 20ms)` 。

每个 TimerTaskList 也有一个自己的过期时间戳 expiration。TimerTaskList 在初始状态下，也就是没有任何延时任务的时候，它的 expiration 是 -1 。

```
class TimerTaskList implements Delayed {
  // 过期时间戳
  private final AtomicLong expiration;
}
```

当向 TimerTaskList 第一次添加延时任务的时候，expiration 将会被设置为 startTime 。随后会被加入到 DelayQueue 中。

当 currentTimeMs 到达某个 TimerTaskList 的 startTime 的时候，那么该 TimerTaskList 就过期了，Kafka 就可以处理该 TimerTaskList 中的延时任务。

比如，当第一层时间轮的 currentTimeMs 到达 0ms 这个时刻时，TimerTaskList0 过期，TimerTaskList0 中存放的是过期时间范围在 `[ 0ms , 1ms)` 之内的延时任务。例如，延时 0.5ms ,0.6ms , .... , 0.9ms 的任务将会在 `currentTimeMs = 0ms` 这个时刻执行。从这里我们可以看出 Kafka 时间轮的时钟精度就是 1ms 。

所以判断一个延时任务是否过期的条件就是看该任务的过期时间 expiration 是否小于当前时间轮的 `currentTimeMs + tickMs`，如果小于，则表示该延时任务已经过期，需要被立即执行。

当延时任务的过期时间 expiration 小于当前时间轮的时钟周期 —— `currentTimeMs + interval` 的时候，就表示当前时间轮可以满足该延时任务的时间跨度，所以该延时任务就会被添加到当前时间轮中，这里的逻辑就和 Netty 的单层时间轮一样了。

```
// Kafka
long virtualId = expiration / tickMs;
int bucketId = (int) (virtualId % (long) wheelSize);
TimerTaskList bucket = buckets[bucketId];
bucket.add(timerTaskEntry);

// Netty
long calculated = timeout.deadline / tickDuration;            
final long ticks = Math.max(calculated, tick);
int stopIndex = (int) (ticks & mask);
HashedWheelBucket bucket = wheel[stopIndex];
bucket.addTimeout(timeout);
```

比如第一层时间轮中，在 1.5ms 这个时刻到期的延时任务就会被添加到 TimerTaskList1 中，TimerTaskList1 的时间范围为 \[ 1ms , 2ms)，TimerTaskList1 的过期时间为 `virtualId * tickMs = 1ms`。

在 19.6ms 这个时刻到期的延时任务就会被添加到 TimerTaskList19 中，TimerTaskList19 的时间范围为 \[ 19ms , 20ms)，TimerTaskList19 的过期时间为 `virtualId * tickMs = 19ms`。

当延时任务被添加到对应的 TimerTaskList 之后，我们就需要设置 TimerTaskList 的过期时间 expiration。

```
class TimerTaskList implements Delayed {
    public boolean setExpiration(long expirationMs) {
        return expiration.getAndSet(expirationMs) != expirationMs;
    }
}
```

TimerTaskList 的过期时间 expiration 初始为 -1 , 当第一次向 TimerTaskList 添加延时任务的时候 `setExpiration` 方法返回 true ,后续再向该 TimerTaskList 添加延时任务时，TimerTaskList 的 expiration 就保持不变了， `setExpiration` 方法返回 false 。

当 `setExpiration` 方法返回 true 的时候，表示我们第一次向 TimerTaskList 添加延时任务，那么这个 TimerTaskList 就会被加入到 DelayQueue 中。

如果延时任务的过期时间 expiration 大于等于当前时间轮的时钟周期 —— `currentTimeMs + interval` 的时候，就表示当前时间轮无法满足该延时任务的时间跨度。这时候 Kafka 就会按需创建上一层时间轮，随后尝试向上一层时间轮添加延时任务。

```
    private synchronized void addOverflowWheel() {
        if (overflowWheel == null) {
            overflowWheel = new TimingWheel(
                interval, // 高层时间轮的 tickMs 恰好是其低一层时间轮的 interval
                wheelSize,
                currentTimeMs,
                taskCounter,
                queue
            );
        }
    }
```

这里我们可以看到，上一层时间轮的时钟间隔 tickMs 恰好是其下一层时间轮的时钟周期 interval。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2kuia8qJTE0Nu4wM2icico7sZKX6Ymj5xDtczia7HhRHs8MDtXq3sMXT15Q/640?wx_fmt=other&from=appmsg#imgIndex=33)

image.png

下面是向 Kafka 多层时间轮添加延时任务的完整逻辑：

```
public class TimingWheel {
    public boolean add(TimerTaskEntry timerTaskEntry) {
        // 获取延时任务到期时间戳
        long expiration = timerTaskEntry.expirationMs;

        if (timerTaskEntry.cancelled()) {
            // 延时任务被取消则返回 false
            return false;
        } else if (expiration < currentTimeMs + tickMs) { 
            // 表示延时任务已经到期     
            return false;
        } else if (expiration < currentTimeMs + interval) {
            // 当前时间轮的时钟周期可以满足延时任务的时间跨度
            // 那么就将延时任务添加到当前时间轮中
            // 计算延时任务应该被添加到哪个 TimerTaskList 中
            long virtualId = expiration / tickMs;
            int bucketId = (int) (virtualId % (long) wheelSize);
            TimerTaskList bucket = buckets[bucketId];
            bucket.add(timerTaskEntry);
            // 设置 TimerTaskList 的过期时间
            if (bucket.setExpiration(virtualId * tickMs)) {
                // 将 TimerTaskList 添加到 DelayQueue 中
                queue.offer(bucket);
            }
            // 延时任务添加成功返回 true
            return true;
        } else {
            // 当前时间轮的时钟周期无法满足延时任务的时间跨度
            // 那么就将延时任务升级到上一层时间轮中
            if (overflowWheel == null) addOverflowWheel(); // 按需添加多层时间轮
            return overflowWheel.add(timerTaskEntry);
        }
    }
}
```

### 6.3 多层时间轮的运转

Netty 是通过一个 workerThread 每隔 tickDuration（100ms）将时钟 tick 向前推进一格。Kafka 中也有一个工作线程 —— Reaper 来推动多层时间轮的运转。

```
    private static final long WORK_TIMEOUT_MS = 200L;

    class Reaper extends ShutdownableThread {
        @Override
        public void doWork() {
            try {
                timer.advanceClock(WORK_TIMEOUT_MS);
            } catch (InterruptedException ex) {
                // Ignore.
            }
        }
    }
```

而 Kafka 为了解决时间轮空推进的问题，只有 TimerTaskList 到期的时候 Reaper 线程才会向前推进多层时间轮，如果没有 TimerTaskList 到期，Kafka 是不会向前推进的，仿佛时间静止了一样。

延时任务会按照过期时间的不同被组织在不同的 TimerTaskList 中， 每个 TimerTaskList 都有一个过期时间范围 —— \[ startTime , endTime) ， 只要过期时间在这个范围内的延时任务，那么它就会被添加到该 TimerTaskList 中。TimerTaskList 自身的过期时间被设置为 startTime。关于这一点，笔者已经在前面的内容中反复强调过了。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2kuia8qJTE0Nu4wM2icico7sZKX6Ymj5xDtczia7HhRHs8MDtXq3sMXT15Q/640?wx_fmt=other&from=appmsg#imgIndex=34)

image.png

当没有任何 TimerTaskList 到期的情况下，Reaper 线程就会一直在 delayQueue 上阻塞等待，直到有到期的 TimerTaskList 出现，Reaper 线程从 delayQueue 上被唤醒，开始处理 TimerTaskList 中的延时任务，并向前推进多层时间轮。

```
TimerTaskList bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
```

当某一个 TimerTaskList 到期之后，说明现在时间已经来到了该 TimerTaskList 对应的 startTime , 那么 Kafka 就会先从第一层时间轮开始，尝试将多层时间轮的 currentTimeMs 推进到 startTime。

但在推进之前，首先需要判断 startTime 与当前 currentTimeMs 的时间间隔是否满足当前时间轮的 tickMs（时钟间隔），如果不满足，时间轮就不能向前推进，因为还不够一个时钟间隔。

比如上图中展示的延时任务。当时间达到 20ms 这个时刻之后，第二层时间轮 bucket\[1\] 到期，时间轮从 DelayQueue 上唤醒。时间轮的推进首先会从第一层时间轮开始，因为 20ms 已经达到了第一层时间轮的时钟间隔（1ms）, 所以 Kafka 会将第一层时间轮的 currentTimeMs 向前推进到 20ms 这个时刻。

接着开始尝试推进第二层时间轮，由于 20ms 仍然满足第二层时间轮的时钟间隔（20ms），所以也将第二层时间轮的 currentTimeMs 向前推进到 20ms 这个时刻。

后面紧跟着就会尝试推进第三层时间轮。但此时 20ms 已经无法满足第三层时间轮的时钟间隔（400ms）了，所以第三层时间轮的 currentTimeMs 仍然停留在 0ms 这个时刻，不会向前推进。

```
public class TimingWheel {
    // 从第一层时间轮开始，尝试将各层时间轮的 currentTimeMs 推进到过期 TimerTaskList 的 startTime（timeMs）
    public void advanceClock(long timeMs) {
        // 时间跨度是否满足当前时间轮的时钟间隔
        if (timeMs >= currentTimeMs + tickMs) {
            // 将时间轮的时间推进到过期 TimerTaskList 的 startTime 位置
            // 必须确保 currentTimeMs 是时钟间隔 tickMs 的整数倍
            currentTimeMs = timeMs - (timeMs % tickMs);
            // 尝试推进更高层时间轮
            if (overflowWheel != null) overflowWheel.advanceClock(currentTimeMs);
        }
    }
}
```

当多层时间轮的推进工作结束之后，Reaper 线程就会着手处理到期 TimerTaskList 中的延时任务，那么 Kafka 这里该如何处理这些延时任务呢 ？

由于是多层时间轮的设计，所以 TimerTaskList 到期，并不代表其中的延时任务到期。比如，上图第二层时间轮中的 TimerTaskList1，它在 20ms 这个时刻到期，但是其中的延时任务却是在 28ms 这个时刻到期。

所以就要将 TimerTaskList 中所有的延时任务挨个进行降级，降级的过程上一小节笔者已经详细介绍过了，Kafka 首先会尝试将延时任务降级到第一层时间轮，如果第一层时间轮的时钟周期无法满足延时跨度，那么在尝试向第二层时间轮降级，这样循环往复，直到可以找到某一层时间轮的时钟周期能够满足该延时跨度。

比如，第一层时间轮的 currentTimeMs 现在已经推进到了 20ms 这个时刻，指向 TimerTaskList0 ， 而延时 28ms 的任务降级到第一层时间轮之后，currentTimeMs 只需要等待 8ms 之后就可以转动到 28ms 的位置，而第一层时间轮的时钟周期是 20ms ，所以可以满足这个 8ms 的延时跨度，于是该延时任务就会被降级到第一层时间轮的 TimerTaskList8 中。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2kUH1HhpjQHaNYo5y9DAbLcwalpKbLMl8cfX00u0UgbE5YpshkUE0HA/640?wx_fmt=other&from=appmsg#imgIndex=35)

image.png

同理，当时间经过 400ms 之后，第三层时间轮的 TimerTaskList1 到期，但是其中的延时任务却是在 450ms 这个时刻才能到期，于是首先尝试向第一层时间轮降级。

第一层时间轮的 currentTimeMs 现在已经推进到了 400ms 这个时刻，仍然指向 TimerTaskList0，如果延时 450ms 的任务添加到第一层时间轮之后，currentTimeMs 需要等待 50ms 之后才可以转动到 450ms 的位置，但是第一层时间轮的时钟周期是 20ms ，无法满足这个 50ms 的延时跨度。

于是该延时任务就会近一步尝试向第二层时间轮降级，第二层时间轮的 currentTimeMs 也已经推进到了 400ms 这个时刻，等待 50ms 之后就可以转动到 450ms 的位置，而第二层时间轮的时钟周期是 400ms ，可以满足这个 50ms 的延时跨度，于是该延时任务最终会被降级到第二层时间轮的 TimerTaskList2中。

![图片](https://mmbiz.qpic.cn/sz_mmbiz_jpg/sOIZXFW0vUZ8UYsgahI7T0vA8vZ1cpF2AH7kQsC8fX50V5Zs6Gf8oa5O3TWqsxg4gjmXgaFdSD2ia324rnCDS3A/640?wx_fmt=other&from=appmsg#imgIndex=36)

image.png

下面就是 Kafka 针对以上过程的源码实现，过程梳理清晰了，源码就变得简单明了了：

```
public class SystemTimer implements Timer {

    public boolean advanceClock(long timeoutMs) throws InterruptedException {
        // 等待到期的 TimerTaskList
        TimerTaskList bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
        if (bucket != null) {
            writeLock.lock();
            try {
                while (bucket != null) {
                    // 尝试将多层时间轮的 currentTimeMs 向前推进到 TimerTaskList 的过期时间
                    timingWheel.advanceClock(bucket.getExpiration());
                    // 将过期 TimerTaskList 中的延时任务挨个降级到低层时间轮中
                    bucket.flush(this::addTimerTaskEntry);
                    bucket = delayQueue.poll();
                }
            } finally {
                writeLock.unlock();
            }
            return true;
        } else {
            // 没有到期的任务
            return false;
        }
    }
}
```

## 总结

本文我们主要讨论了定时任务调度相关的主题，笔者一开始介绍了 JDK 的三种调度组件：Timer，DelayQueue，ScheduledThreadPoolExecutor。但他们的共同特点都是采用了小根堆这种数据结构来组织管理定时任务，然而在面对海量定时任务的添加与取消时，这种设计的时间复杂度会比较高 —— `O(logn)`。

随后笔者介绍了 Netty 的单层时间轮 HashedWheelTimer，它将海量定时任务的添加与取消操作的时间复杂度降低到了 `O(1)` ， 但无法避免时间轮的空推进现象以及无法应对海量延时跨度比较大的定时任务场景。

最后笔者详细讨论了 Kafka 时间轮的设计与实现，Kafka 通过引入 DelayQueue 以及多层时间轮，巧妙地解决了时间轮的空推进现象和海量延时任务时间跨度大的管理问题。好了，今天的内容到这里就全部结束了，我们下篇文章见~~~
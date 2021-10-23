/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.impl;

import com.google.common.base.Preconditions;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.FastThreadLocal;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

//为什么UnAckedMessageTracker继承了closeAble接口，而NegativeAcksTracker没有，并且本类实现了对应的close和stop等清理动作
//Un(未) Acked(确认的) Message(消息) Tracker(追踪器)
public class UnAckedMessageTracker implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(UnAckedMessageTracker.class);
    //消息和partition的映射，但为什么partition会是一个MessageId的集合呢
    //这个map只是为了记录每一个messageId对应的分区箱子是哪个
    protected final ConcurrentHashMap<MessageId, ConcurrentOpenHashSet<MessageId>> messageIdPartitionMap;
    //这个变量又是干啥的，记录消息进入的时间顺序？
    //这个队列就是用来存放箱子的，就想火车车厢一样，消息按照顺序放到这些箱子里，然后上面的map中再加上消息和箱子的映射
    protected final ArrayDeque<ConcurrentOpenHashSet<MessageId>> timePartitions;
    //读锁
    protected final Lock readLock;
    //写锁
    protected final Lock writeLock;
    //当ack timeout设置为0时，会关闭ack超时重发功能，这个类就是用来赋值关闭ack超时的
    public static final UnAckedMessageTrackerDisabled UNACKED_MESSAGE_TRACKER_DISABLED = new UnAckedMessageTrackerDisabled();
    //ack超时时间
    private final long ackTimeoutMillis;
    //持续时间，看起来像是周期间隔类的意思
    private final long tickDurationInMs;
    //集成UnAckedMessageTracker，这样才方便赋值，当ack 超时为0时，需要用这个类的实例对象来赋值
    private static class UnAckedMessageTrackerDisabled extends UnAckedMessageTracker {
        //关闭ack功能就是什么都不做，而且还表现的和做了一样

        //清理方法
        @Override
        public void clear() {
        }
        //应该是等待ack的消息总size
        @Override
        long size() {
            return 0;
        }

        //添加消息
        @Override
        public boolean add(MessageId m) {
            return true;
        }

        //移除消息
        @Override
        public boolean remove(MessageId m) {
            return true;
        }

        //移除消息直到某个条件发生
        @Override
        public int removeMessagesTill(MessageId msgId) {
            return 0;
        }

        //关闭
        @Override
        public void close() {
        }
    }

    //超时器,netty出品
    private Timeout timeout;

    //定义这个什么都不做的变量就是为了定义UnAckedMessageTrackerDisabled类,否则UnAckedMessageTrackerDisabled这个类的定义就得自己取初始化这些变量了
    public UnAckedMessageTracker() {
        //初始化怎么把这些变量都设置为null呀
        readLock = null;
        writeLock = null;
        timePartitions = null;
        messageIdPartitionMap = null;
        //超时时间和持续时间都设置未0
        this.ackTimeoutMillis = 0;
        this.tickDurationInMs = 0;
    }
    //传入参数的构造函数
    //pulsar客户端、consumer客户端、ack超时时间
    public UnAckedMessageTracker(PulsarClientImpl client, ConsumerBase<?> consumerBase, long ackTimeoutMillis) {
        this(client, consumerBase, ackTimeoutMillis, ackTimeoutMillis);
    }

    //构建一个快速线程local，作用是啥 类似是ThreadLocal但比thread local更快，其实就是想实现线程之间的隔离
    private static final FastThreadLocal<HashSet<MessageId>> TL_MESSAGE_IDS_SET = new FastThreadLocal<HashSet<MessageId>>() {
        @Override
        protected HashSet<MessageId> initialValue() throws Exception {
            return new HashSet<>();
        }
    };

    //tickDurationInMs对应的是周期执行的间隔，而ackTimeoutMillis只是用来算需要多少个箱子,箱子的个数是向上取整
    //当ackTimeoutMillis和tickDurationInMs部署整数倍关系时，那么实际触发超时重传动作就会比实际配置的超时时间晚最多tickDurationInMs
    //这样箱子队列中队首的箱子永远是达到超时时间的箱子
    //传入的tickDurationInMs<=ackTimeoutMillis
    public UnAckedMessageTracker(PulsarClientImpl client, ConsumerBase<?> consumerBase, long ackTimeoutMillis, long tickDurationInMs) {
        //使用了google的guava框架,ack超时时间需要大于tickDurationInMs周期时间
        Preconditions.checkArgument(tickDurationInMs > 0 && ackTimeoutMillis >= tickDurationInMs);
        //超时时间
        this.ackTimeoutMillis = ackTimeoutMillis;
        //周期执行时间
        this.tickDurationInMs = tickDurationInMs;
        //可重入的读写锁
        ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        //读所
        this.readLock = readWriteLock.readLock();
        //写锁
        this.writeLock = readWriteLock.writeLock();
        //messageId和partition的映射
        this.messageIdPartitionMap = new ConcurrentHashMap<>();
        //message等待ack的时间顺序
        this.timePartitions = new ArrayDeque<>();
        //箱子的个数是向上取整
        int blankPartitions = (int)Math.ceil((double)this.ackTimeoutMillis / this.tickDurationInMs);
        //用空白分区来占位
        //注意这里最少都是两个，这样就解决了只有一个时，timeout不准的问题
        //向上取整不够，还得再额外多一个，如果没除尽，向上取整，这样会比给定的超时时间多一个<tickDurationInMs时间才会触发，也就是实际超时时间比配置的最多要多tickDurationInMs
        for (int i = 0; i < blankPartitions + 1; i++) {
            timePartitions.add(new ConcurrentOpenHashSet<>(16, 1));
        }
        //获取超时定时器,这里直接就启动超时任务了？
        timeout = client.timer().newTimeout(new TimerTask() {
            @Override
            public void run(Timeout t) throws Exception {
                //获取对应的message集合
                Set<MessageId> messageIds = TL_MESSAGE_IDS_SET.get();
                //先清空(不是不相互影响吗，为啥要清空，难道是为了同一个线程前后两次执行不相互影响？)
                //这个threadload更像是一个map，key=threadName value=实例对象，这样在反复切换线程时候，就可以比较顺利的切换线程上下文了
                messageIds.clear();
                //先给一个写锁
                writeLock.lock();
                try {
                    //刚启动后，在第一个周期内，消息超时可能是不准的
                    ConcurrentOpenHashSet<MessageId> headPartition = timePartitions.removeFirst();
                    //判断分区是否未空
                    //这里判空后，再forEeach会不会多此一举了
                    //在放入第一个消息之前，队首的箱子消息数是空，所以需要判断下是否为空，以此来判断是否是第一次
                    if (!headPartition.isEmpty()) {
                        log.warn("[{}] {} messages have timed-out", consumerBase, headPartition.size());
                        headPartition.forEach(messageId -> {
                            //处理chunk数据，如果是chunk
                            addChunkedMessageIdsAndRemoveFromSequnceMap(messageId, messageIds, consumerBase);
                            //加入该线程的messageIDs集合中
                            messageIds.add(messageId);
                            //移除该message的map映射
                            messageIdPartitionMap.remove(messageId);
                        });
                    }

                    //清空partiiton
                    headPartition.clear();
                    //将该partition放入到队尾
                    timePartitions.addLast(headPartition);
                } finally {
                    //释放锁
                    writeLock.unlock();
                    //判断是否有消息
                    if (messageIds.size() > 0) {
                        //触发ack超时重发前的拦截器
                        consumerBase.onAckTimeoutSend(messageIds);
                        //触发超时重发
                        consumerBase.redeliverUnacknowledgedMessages(messageIds);
                    }
                    timeout = client.timer().newTimeout(this, tickDurationInMs, TimeUnit.MILLISECONDS);
                }
            }
        }, this.tickDurationInMs, TimeUnit.MILLISECONDS);
    }

    //这里是针对chunk数据块的消息，会把该消息对应的所有messageId都放入用来触发超时重传
    public static void addChunkedMessageIdsAndRemoveFromSequnceMap(MessageId messageId, Set<MessageId> messageIds,
            ConsumerBase<?> consumerBase) {
        //这里的messageId应该是大消息的Id，也就是说，每一个chunk数据块都有一个大消息的messaeId
        if (messageId instanceof MessageIdImpl) {
            //获取chunk数据块数组
            MessageIdImpl[] chunkedMsgIds = consumerBase.unAckedChunkedMessageIdSequenceMap.get((MessageIdImpl) messageId);
            if (chunkedMsgIds != null && chunkedMsgIds.length > 0) {
                for (MessageIdImpl msgId : chunkedMsgIds) {
                    messageIds.add(msgId);
                }
            }
            //移除该messageId，从map中
            consumerBase.unAckedChunkedMessageIdSequenceMap.remove((MessageIdImpl) messageId);
        }
    }

    public void clear() {
        //持写锁
        writeLock.lock();
        try {
            //清空messageIdPartitionMap，那么这些不用等ack超时，下次启动的时候，反正因为没有发ack，服务端会重发的
            messageIdPartitionMap.clear();
            //这个队列也情况掉timePartitions
            timePartitions.forEach(tp -> tp.clear());
        } finally {
            //释放锁
            writeLock.unlock();
        }
    }

    //首先设计了一个箱子队列，当放入消息的时候，先从队列中取出箱子，然后把消息放入到箱子中，最后把箱子放到队列末尾
    //在这个过程中，如果该消息已经在某一个箱子中了，那么久忽略，返回添加失败的状态：false
    //每次加入一个消息都是放入队首的第一个箱子中，只有在每次触发执行的时候才会把第一个箱子移动到末尾，所以在每次触发执行的时候，队首的箱子里面的就是满足ack超时的消息
    public boolean add(MessageId messageId) {
        //加上写锁
        writeLock.lock();
        try {
            //从队首取出一个箱子
            //注意这里是放在最后一个箱子里,但是当
            ConcurrentOpenHashSet<MessageId> partition = timePartitions.peekLast();
            //从map中取出该消息对应的箱子
            ConcurrentOpenHashSet<MessageId> previousPartition = messageIdPartitionMap.putIfAbsent(messageId,
                    partition);
            if (previousPartition == null) {
                //如果前一个箱子未空，那么就用用该箱子，把消息放入进去
                return partition.add(messageId);
            } else {
                //否则就放入失败，证明该消息已经有箱子了：已经在等待超时重发了
                return false;
            }
        } finally {
            //释放锁
            writeLock.unlock();
        }
    }

    //判断等待超时重发的消息是否为空
    boolean isEmpty() {
        readLock.lock();
        try {
            return messageIdPartitionMap.isEmpty();
        } finally {
            //释放锁
            readLock.unlock();
        }
    }

    public boolean remove(MessageId messageId) {
        //加上写锁
        writeLock.lock();
        try {
            //记录是否移除成功
            boolean removed = false;
            //从map中移除该 message项，返回该messageId对应的分区箱子
            ConcurrentOpenHashSet<MessageId> exist = messageIdPartitionMap.remove(messageId);
            //如果该message对应的箱子存在，那么就从该箱子中移除该消息
            if (exist != null) {
                removed = exist.remove(messageId);
            }
            //返回移除结果
            return removed;
        } finally {
            //释放锁
            writeLock.unlock();
        }
    }

    long size() {
        //添加读锁
        readLock.lock();
        try {
            //返回没有ack的消息数,这个size名字感觉没取好
            //这个到底是未ack的消息size总和，还是消息数啊
            return messageIdPartitionMap.size();
        } finally {
            //释放写锁
            readLock.unlock();
        }
    }

    //移除消息直到某个条件发生
    //和上面哪个remove方法有什么区别呢 前面哪个是移除一条消息，这个是移除直到该消息的多条（对应消息id小于等于给定的msgId的消息都移除掉）
    public int removeMessagesTill(MessageId msgId) {
        //添加写锁
        writeLock.lock();
        try {
            //记录remove结果
            int removed = 0;
            //遍历map的key，也就是所有等待ack的消息
            Iterator<MessageId> iterator = messageIdPartitionMap.keySet().iterator();
            while (iterator.hasNext()) {
                //获取消息id
                MessageId messageId = iterator.next();
                //如果该消息id小于msgId，那么久移除,也就是说把小于等于该id的所有消息都移除掉
                if (messageId.compareTo(msgId) <= 0) {
                    ConcurrentOpenHashSet<MessageId> exist = messageIdPartitionMap.get(messageId);
                    if (exist != null) {
                        exist.remove(messageId);
                    }
                    iterator.remove();
                    removed ++;
                }
            }
            //返回移除结果
            return removed;
        } finally {
            //释放掉锁
            writeLock.unlock();
        }
    }

    //停掉
    private void stop() {
        //添加写锁
        writeLock.lock();
        try {
            //取消定时器
            if (timeout != null && !timeout.isCancelled()) {
                timeout.cancel();
                timeout = null;
            }
            //清理掉等待确认的消息
            this.clear();
        } finally {
            //释放写锁
            writeLock.unlock();
        }
    }

    @Override
    public void close() {
        stop();
    }
}

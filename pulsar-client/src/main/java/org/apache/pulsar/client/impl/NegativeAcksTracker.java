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

import io.netty.util.Timeout;
import io.netty.util.Timer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import static org.apache.pulsar.client.impl.UnAckedMessageTracker.addChunkedMessageIdsAndRemoveFromSequnceMap;

//这个类名翻译过来意思是：否定的ack追踪器，意思就是哪些些小的ack确认我们是忽略了的，那么在一定时候，我们对于这些忽略ack的消息再让服务端发一次
class NegativeAcksTracker {
    //重发的消息和此时的时间戳
    private HashMap<MessageId, Long> nackedMessages = null;
    //用来发送重发请求的，也就是之前发送了这些消息的consumer
    private final ConsumerBase<?> consumer;
    //定时器，用来定时触发
    private final Timer timer;
    //延迟nack的时间，距离提交nack到再次重发的延迟
    private final long nackDelayNanos;
    //检查周期，默认是nack延迟时间的三分之一，那就有意思了，这个三分之一是怎么确认的呢:三分之一用来积攒要nack的消息，然后用三分之二的时间来发送
    private final long timerIntervalNanos;
    //定时调度器
    private Timeout timeout;

    //为了积攒一批来发送，所以这里设置的delay时间最少都要是100毫秒，当然如果不设置getNegativeAckRedeliveryDelayMicros的话，默认getNegativeAckRedeliveryDelayMicros是1分钟，那么这里的delay时间就是1分钟了
    // Set a min delay to allow for grouping nacks within a single batch
    private static final long MIN_NACK_DELAY_NANOS = TimeUnit.MILLISECONDS.toNanos(100);

    public NegativeAcksTracker(ConsumerBase<?> consumer, ConsumerConfigurationData<?> conf) {
        this.consumer = consumer;
        this.timer = consumer.getClient().timer();
        this.nackDelayNanos = Math.max(TimeUnit.MICROSECONDS.toNanos(conf.getNegativeAckRedeliveryDelayMicros()),
                MIN_NACK_DELAY_NANOS);
        this.timerIntervalNanos = nackDelayNanos / 3;
    }

    private synchronized void triggerRedelivery(Timeout t) {
        if (nackedMessages.isEmpty()) {
            this.timeout = null;
            return;
        }

        //把本轮满足重发条件的消息全部收集起来
        // Group all the nacked messages into one single re-delivery request
        Set<MessageId> messagesToRedeliver = new HashSet<>();
        long now = System.nanoTime();
        nackedMessages.forEach((msgId, timestamp) -> {
            if (timestamp < now) {
                //如果是chunk的消息，那么从chunk数据块的记录中把所有相关该消息的数据块都加入到messagesToRedeliver中
                //说明这个msgId变量是chunk数据块中记录的大消息的那个消息id
                addChunkedMessageIdsAndRemoveFromSequnceMap(msgId, messagesToRedeliver, this.consumer);
                messagesToRedeliver.add(msgId);
            }
        });

        //从map中移除
        messagesToRedeliver.forEach(nackedMessages::remove);
        //调用nack重发前的拦截器，来处理一些前提动作:比如记录一些metric，统计重发频率
        consumer.onNegativeAcksSend(messagesToRedeliver);
        //发送重发请求给服务端，指定哪些消息要重发
        consumer.redeliverUnacknowledgedMessages(messagesToRedeliver);

        //每次调用后，都会准备一个调用的定时器
        this.timeout = timer.newTimeout(this::triggerRedelivery, timerIntervalNanos, TimeUnit.NANOSECONDS);
    }

    //这个方法的作用：①把消息和时间戳放入map；②构建timeout定时器；
    public synchronized void add(MessageId messageId) {
        if (messageId instanceof BatchMessageIdImpl) {
            //这里看起来是把batchmessage对象转换为MessageIdImpl对象，这么做的目的是什么？这两个类的区别是什么
            //BatchMessageIdImpl是MessageIdImpl的子类，那这里为啥要转换呢，直接put到nackedMessages不就行了吗 会自动向下转
            BatchMessageIdImpl batchMessageId = (BatchMessageIdImpl) messageId;
            messageId = new MessageIdImpl(batchMessageId.getLedgerId(), batchMessageId.getEntryId(),
                    batchMessageId.getPartitionIndex());
        }

        if (nackedMessages == null) {
            nackedMessages = new HashMap<>();
        }
        nackedMessages.put(messageId, System.nanoTime() + nackDelayNanos);

        if (this.timeout == null) {
            // Schedule a task and group all the redeliveries for same period. Leave a small buffer to allow for
            // nack immediately following the current one will be batched into the same redeliver request.
            this.timeout = timer.newTimeout(this::triggerRedelivery, timerIntervalNanos, TimeUnit.NANOSECONDS);
        }
    }
}

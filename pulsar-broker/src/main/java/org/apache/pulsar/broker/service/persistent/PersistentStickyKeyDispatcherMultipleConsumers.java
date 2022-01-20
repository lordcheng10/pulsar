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
package org.apache.pulsar.broker.service.persistent;

import io.netty.util.concurrent.FastThreadLocal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.ConsistentHashingStickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.HashRangeAutoSplitStickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.HashRangeExclusiveStickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.SendMessageInfo;
import org.apache.pulsar.broker.service.StickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.KeySharedMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistentStickyKeyDispatcherMultipleConsumers extends PersistentDispatcherMultipleConsumers {
    // 是否允许乱序，这个是客户端传入的.
    // 怎么保证不乱序?如果该变量为true，那么会创建一个recentlyJoinedConsumers map，用来存放最近加入的consumer
    private final boolean allowOutOfOrderDelivery;
    //consumer的select选择器
    private final StickyKeyConsumerSelector selector;
    //调度员是否卡在重播上
    private boolean isDispatcherStuckOnReplays = false;
    //key share模式
    private final KeySharedMode keySharedMode;

    /**
     * 当消费者加入时，它将以当前读取位置添加到此地图中。
     * 这意味着，为了保持顺序，新的消费者只能接收旧的消息，直到标记删除位置超过这个点。
     *
     * When a consumer joins, it will be added to this map with the current read position.
     * This means that, in order to preserve ordering, new consumers can only receive old
     * messages, until the mark-delete position will move past this point.
     */
    private final LinkedHashMap<Consumer, PositionImpl> recentlyJoinedConsumers;

    //卡住消费者
    private final Set<Consumer> stuckConsumers;
    // 下一个卡住的消费者
    private final Set<Consumer> nextStuckConsumers;

    PersistentStickyKeyDispatcherMultipleConsumers(PersistentTopic topic, ManagedCursor cursor,
            Subscription subscription, ServiceConfiguration conf, KeySharedMeta ksm) {
        super(topic, cursor, subscription, ksm.isAllowOutOfOrderDelivery());

        this.allowOutOfOrderDelivery = ksm.isAllowOutOfOrderDelivery();
        this.recentlyJoinedConsumers = allowOutOfOrderDelivery ? null : new LinkedHashMap<>();
        this.stuckConsumers = new HashSet<>();
        this.nextStuckConsumers = new HashSet<>();
        this.keySharedMode = ksm.getKeySharedMode();
        switch (this.keySharedMode) {
        case AUTO_SPLIT:
            if (conf.isSubscriptionKeySharedUseConsistentHashing()) {
                selector = new ConsistentHashingStickyKeyConsumerSelector(
                        conf.getSubscriptionKeySharedConsistentHashingReplicaPoints());
            } else {
                selector = new HashRangeAutoSplitStickyKeyConsumerSelector();
            }
            break;

        case STICKY:
            this.selector = new HashRangeExclusiveStickyKeyConsumerSelector();
            break;

        default:
            throw new IllegalArgumentException("Invalid key-shared mode: " + keySharedMode);
        }
    }

    //加入消费者
    @Override
    public synchronized void addConsumer(Consumer consumer) throws BrokerServiceException {
        super.addConsumer(consumer);
        try {
            //加入consumer selector中
            selector.addConsumer(consumer);
        } catch (BrokerServiceException e) {
            consumerSet.removeAll(consumer);
            consumerList.remove(consumer);
            throw e;
        }

        // 获取当前读取的位置
        PositionImpl readPositionWhenJoining = (PositionImpl) cursor.getReadPosition();
        //设置consumer的读取位置
        consumer.setReadPositionWhenJoining(readPositionWhenJoining);
        // If this was the 1st consumer, or if all the messages are already acked, then we
        // don't need to do anything special
        if (!allowOutOfOrderDelivery
                && recentlyJoinedConsumers != null
                && consumerList.size() > 1
                && cursor.getNumberOfEntriesSinceFirstNotAckedMessage() > 1) {
            //走到这里的条件：①需要保序；②有consumer；③没有ack的消息大于1
            recentlyJoinedConsumers.put(consumer, readPositionWhenJoining);
        }
    }

    @Override
    public synchronized void removeConsumer(Consumer consumer) throws BrokerServiceException {
        //  在调用超类 removeConsumer 方法之前，必须从选择器中删除消费者。
        //  在超类 removeConsumer 方法中，消费者拥有的未决确认被添加到 redeliveryMessages。
        //  如果此时消费者尚未从选择器中移除，则代理将尝试将消息重新传递给已关闭的消费者。
        //  结果消息没有重新投递给任何消费者，并且mark-delete位置不动，最终导致所有消费者卡住。

        // The consumer must be removed from the selector before calling the superclass removeConsumer method.
        // In the superclass removeConsumer method, the pending acks that the consumer has are added to
        // redeliveryMessages. If the consumer has not been removed from the selector at this point,
        // the broker will try to redeliver the messages to the consumer that has already been closed.
        // As a result, the messages are not redelivered to any consumer, and the mark-delete position does not move,
        // eventually causing all consumers to get stuck.
        // 先从selector中移除
        selector.removeConsumer(consumer);
        // 移除该consumer
        super.removeConsumer(consumer);
        // 如果recentlyJoinedConsumers不为null，就代表需要保序
        if (recentlyJoinedConsumers != null) {
            // 从最近加入的集合中移除
            recentlyJoinedConsumers.remove(consumer);
            if (consumerList.size() == 1) {//如果只有一个consumer了，那么久直接把最近加入的consumer清理掉,因为此时就只剩下该consumer了
                recentlyJoinedConsumers.clear();
            }
            //removeConsumersFromRecentJoinedConsumers方法会遍历所有最近加入的consumer，然后判断其是否可以从最近加入的集合中移除掉
            if (removeConsumersFromRecentJoinedConsumers() || !redeliveryMessages.isEmpty()) {
                //如果从最近加入的集合中移除了consumer或重发消息不为空时，就会重新去读取更多的数据
                readMoreEntries();
            }
        }
    }

    //快速thread local
    private static final FastThreadLocal<Map<Consumer, List<Entry>>> localGroupedEntries =
            new FastThreadLocal<Map<Consumer, List<Entry>>>() {
                @Override
                protected Map<Consumer, List<Entry>> initialValue() throws Exception {
                    return new HashMap<>();
                }
            };

    @Override
    protected void sendMessagesToConsumers(ReadType readType, List<Entry> entries) {
        //发送的总消息数
        long totalMessagesSent = 0;
        //发送总的bytes数
        long totalBytesSent = 0;
        //总的entry数
        long totalEntries = 0;
        //要推送的entry 数
        int entriesCount = entries.size();

        // 触发阅读更多消息
        // Trigger read more messages
        if (entriesCount == 0) {
            readMoreEntries();
            return;
        }

        //如果consumer集合为空，也就是没有consumer，那么就释放entry，重置cursor
        if (consumerSet.isEmpty()) {
            entries.forEach(Entry::release);
            cursor.rewind();
            return;
        }

        // 我们必须重试 readMoreEntries 以保留订单交付的极端情况。
        // 这可能在消费者关闭时发生。 有关详细信息，请参阅问题 #12885。
        // A corner case that we have to retry a readMoreEntries in order to preserver order delivery.
        // This may happen when consumer closed. See issue #12885 for details.
        if (!allowOutOfOrderDelivery) {
            //如果需要保序，allowOutOfOrderDelivery=false
            //立即获取要重播的消息
            //如果isDispatcherStuckOnReplays为true，那么这里返回的messagesToReplayNow为空
            Set<PositionImpl> messagesToReplayNow = this.getMessagesToReplayNow(1);
            if (messagesToReplayNow != null && !messagesToReplayNow.isEmpty() && this.minReplayedPosition != null) {
                // 获取要重发的位置
                PositionImpl relayPosition = messagesToReplayNow.stream().findFirst().get();
                // 如果relayPosition 是一个新条目，在此异步读取期间插入较小的位置以重新传递，则此relayPosition 可能应首先分派给消费者。
                // 因此，为了保持订单交付，我们需要丢弃这个读取结果，并通过调用 readMoreEntries 尝试触发包含“relayPosition”的重播读取。
                // If relayPosition is a new entry wither smaller position is inserted for redelivery during this async
                // read, it is possible that this relayPosition should dispatch to consumer first. So in order to
                // preserver order delivery, we need to discard this read result, and try to trigger a replay read,
                // that containing "relayPosition", by calling readMoreEntries.
                if (relayPosition.compareTo(minReplayedPosition) < 0) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Position {} (<{}) is inserted for relay during current {} read, discard this "
                                + "read and retry with readMoreEntries.",
                                name, relayPosition, minReplayedPosition, readType);
                    }
                    if (readType == ReadType.Normal) {
                        //如果是正常读取，那么就将所有entry加入到重发队列中
                        entries.forEach(entry -> {
                            long stickyKeyHash = getStickyKeyHash(entry);
                            addMessageToReplay(entry.getLedgerId(), entry.getEntryId(), stickyKeyHash);
                            entry.release();
                        });
                    } else if (readType == ReadType.Replay) {
                        //如果是重发的entry，那么直接释放掉
                        entries.forEach(Entry::release);
                    }
                    //读取更多数据
                    readMoreEntries();
                    return;
                }
            }
        }
        //清理掉下一个卡主的消费者集合
        nextStuckConsumers.clear();
        //按照group来进行分组
        final Map<Consumer, List<Entry>> groupedEntries = localGroupedEntries.get();
        //首先清空groupedEntries
        groupedEntries.clear();
        //这个会记录每个consumer对应的hash值
        final Map<Consumer, Set<Integer>> consumerStickyKeyHashesMap = new HashMap<>();

        //遍历所有entry，将所有entry按照consumer进行分组
        for (Entry entry : entries) {
            //获取该entry的hash key
            int stickyKeyHash = getStickyKeyHash(entry);
            //根据hash选择对应consumer
            Consumer c = selector.select(stickyKeyHash);
            if (c != null) {
                groupedEntries.computeIfAbsent(c, k -> new ArrayList<>()).add(entry);
                consumerStickyKeyHashesMap.computeIfAbsent(c, k -> new HashSet<>()).add(stickyKeyHash);
            } else {
                //如果没选到consumer，那么久释放entry
                entry.release();
            }
        }

        //这里回统计consumer的数量，这里的命名是不是有问题呀，不应该叫keyNumbers吧  consumerNumbers吧
        AtomicInteger keyNumbers = new AtomicInteger(groupedEntries.size());
        //当前线程的consumer数
        int currentThreadKeyNumber = groupedEntries.size();
        if (currentThreadKeyNumber == 0) {
            //如果consumer数为0，也就是没有consumer，那么currentThreadKeyNumber久设置为-1
            currentThreadKeyNumber = -1;
        }
        //遍历所有consumer
        for (Map.Entry<Consumer, List<Entry>> current : groupedEntries.entrySet()) {
            //获取consumer
            Consumer consumer = current.getKey();
            //获取hash到该consumer的entry集合
            List<Entry> entriesWithSameKey = current.getValue();
            //都hash到同一个consumer的entry数量
            int entriesWithSameKeyCount = entriesWithSameKey.size();
            //获取该consumer的permit
            final int availablePermits = consumer == null ? 0 : Math.max(consumer.getAvailablePermits(), 0);
            //从要推送给该consumer的entry数和可获得的permit数两者取一个最小值
            //对于这个consumer可发送的最大消息数
            int maxMessagesForC = Math.min(entriesWithSameKeyCount, availablePermits);
            //这里获取大于最近加入的consumer map中记录的read position位置
            int messagesForC = getRestrictedMaxEntriesForConsumer(consumer, entriesWithSameKey, maxMessagesForC,
                    readType, consumerStickyKeyHashesMap.get(consumer));
            if (log.isDebugEnabled()) {
                log.debug("[{}] select consumer {} with messages num {}, read type is {}",
                        name, consumer == null ? "null" : consumer.consumerName(), messagesForC, readType);
            }

            //如果存在不可以读的entry消息，那么这里需要将这些消息放入重发map中
            if (messagesForC < entriesWithSameKeyCount) {
                // We are not able to push all the messages with given key to its consumer,
                // so we discard for now and mark them for later redelivery
                for (int i = messagesForC; i < entriesWithSameKeyCount; i++) {
                    Entry entry = entriesWithSameKey.get(i);
                    long stickyKeyHash = getStickyKeyHash(entry);
                    addMessageToReplay(entry.getLedgerId(), entry.getEntryId(), stickyKeyHash);
                    entry.release();
                    //这里回把这些entry重置为null
                    entriesWithSameKey.set(i, null);
                }
            }

            //如果messagesForC大于0，那么久说明存在一些entry消息，是小于该consumer刚加入时的read position位置的
            if (messagesForC > 0) {
                //对于这些消息，那么如果是重发的，那么久先从重发集合中移除掉
                // remove positions first from replay list first : sendMessages recycles entries
                if (readType == ReadType.Replay) {
                    for (int i = 0; i < messagesForC; i++) {
                        Entry entry = entriesWithSameKey.get(i);
                        redeliveryMessages.remove(entry.getLedgerId(), entry.getEntryId());
                    }
                }

                //构建一个专门统计发送信息的对象SendMessageInfo
                SendMessageInfo sendMessageInfo = SendMessageInfo.getThreadLocal();
                //这个用来记录每个entry的大小
                EntryBatchSizes batchSizes = EntryBatchSizes.get(messagesForC);
                //记录batch中每个消息的ack状态
                EntryBatchIndexesAcks batchIndexesAcks = EntryBatchIndexesAcks.get(messagesForC);
                //这里回过滤出正在发送的消息
                totalEntries += filterEntriesForConsumer(entriesWithSameKey, batchSizes, sendMessageInfo,
                        batchIndexesAcks, cursor, readType == ReadType.Replay);
                //这里会把小于刚加入时记录的read position位置的entry进行发送
                consumer.sendMessages(entriesWithSameKey, batchSizes, batchIndexesAcks,
                        sendMessageInfo.getTotalMessages(),
                        sendMessageInfo.getTotalBytes(), sendMessageInfo.getTotalChunkedMessages(),
                        getRedeliveryTracker()).addListener(future -> {
                    if (future.isDone() && keyNumbers.decrementAndGet() == 0) {
                        //当keyNumbers为0时，就说明所有的consumer对应的消息都推送完毕了，然后会继续读取更多数据
                        readMoreEntries();
                    }
                });


                //这里会更新下消息计数
                TOTAL_AVAILABLE_PERMITS_UPDATER.getAndAdd(this,
                        -(sendMessageInfo.getTotalMessages() - batchIndexesAcks.getTotalAckedIndexCount()));
                totalMessagesSent += sendMessageInfo.getTotalMessages();
                totalBytesSent += sendMessageInfo.getTotalBytes();
            } else {
                currentThreadKeyNumber = keyNumbers.decrementAndGet();
            }
        }

        // 获取已发送消息的消息分发许可,这里应该是和限速有关系,默认是没有开启的
        // acquire message-dispatch permits for already delivered messages
        if (serviceConfig.isDispatchThrottlingOnNonBacklogConsumerEnabled() || !cursor.isActive()) {
            //TODO-这里会导致dispatchThrottlingOnBatchMessageEnabled动态配置失效
            long permits = dispatchThrottlingOnBatchMessageEnabled ? totalEntries : totalMessagesSent;
            if (topic.getDispatchRateLimiter().isPresent()) {
                topic.getDispatchRateLimiter().get().tryDispatchPermit(permits, totalBytesSent);
            }

            if (dispatchRateLimiter.isPresent()) {
                dispatchRateLimiter.get().tryDispatchPermit(permits, totalBytesSent);
            }
        }
        // 情况卡住的消费者
        stuckConsumers.clear();

        //如果发送的消息数为0并且最近没有加入consumer，或者可以允许乱序，那么久继续读数据
        if (totalMessagesSent == 0 && (recentlyJoinedConsumers == null || recentlyJoinedConsumers.isEmpty())) {
            //这意味着，我们刚刚阅读的所有消息现在都无法发送。
            //这种情况只会在以下情况下发生：
            //1.我们让消费者准备好接受消息（否则不会被触发）
            //2.当前这组消息中的所有key都路由到当前繁忙的consumer上，卡住不是由stdConsumers引起的
            //这里的解决方案是继续阅读下一批消息，这些消息可能还包含用于其他消费者的密钥。 我们这样做，除非那是“最近加入的消费者”。
            //在这种情况下，我们将在新消费者尚未准备好接受新消息时在流中向前看，因此很可能只会增加读取位置和标记删除之间的距离位置。
            // This means, that all the messages we've just read cannot be dispatched right now.
            // This condition can only happen when:
            //  1. We have consumers ready to accept messages (otherwise the would not haven been triggered)
            //  2. All keys in the current set of messages are routing to consumers that are currently busy
            //     and stuck is not caused by stuckConsumers
            //
            // The solution here is to move on and read next batch of messages which might hopefully contain
            // also keys meant for other consumers.
            //
            // We do it unless that are "recently joined consumers". In that case, we would be looking
            // ahead in the stream while the new consumers are not ready to accept the new messages,
            // therefore would be most likely only increase the distance between read-position and mark-delete
            // position.
            if (!nextStuckConsumers.isEmpty()) {
                isDispatcherStuckOnReplays = true;
                stuckConsumers.addAll(nextStuckConsumers);
            }
            // readMoreEntries should run regardless whether or not stuck is caused by
            // stuckConsumers for avoid stopping dispatch.
            readMoreEntries();
        }  else if (currentThreadKeyNumber == 0) {
            //如果consumer数为0，那么久执行下面的继续读取，这里都没有consumer了为啥还要读取
            topic.getBrokerService().executor().schedule(() -> {
                synchronized (PersistentStickyKeyDispatcherMultipleConsumers.this) {
                    readMoreEntries();
                }
            }, 100, TimeUnit.MILLISECONDS);
        }
    }
    //获取消费者的受限最大条目
    //maxMessages是可发送的最大消息数
    //readType: 读取类型
    // stickyKeyHashes:该consumer对应的hash值
    private int getRestrictedMaxEntriesForConsumer(Consumer consumer, List<Entry> entries, int maxMessages,
            ReadType readType, Set<Integer> stickyKeyHashes) {
        //可发送的消息数如果为0，那么久直接放入下一个卡主的consumer中
        if (maxMessages == 0) {
            // 该consumer被卡主了
            // the consumer was stuck
            nextStuckConsumers.add(consumer);
            return 0;
        }
        //如果readType是正常的读取类型
        //如果重发消息里面包含hash值stickyKeyHashes
        if (readType == ReadType.Normal && stickyKeyHashes != null
                && redeliveryMessages.containsStickyKeyHashes(stickyKeyHashes)) {
            // 如果 redeliveryMessages 包含与调度程序尝试发送的消息对应的哈希值相同的消息，请不要发送这些消息以保证订单.
            // If redeliveryMessages contains messages that correspond to the same hash as the messages
            // that the dispatcher is trying to send, do not send those messages for order guarantee
            return 0;
        }
        //如果recentlyJoinedConsumers为null，就说明不保序，那么久直接返回可读的最大消息数maxMessages
        if (recentlyJoinedConsumers == null) {
            return maxMessages;
        }
        //从最近加入的消息中移除
        removeConsumersFromRecentJoinedConsumers();
        //获取该consumer刚加入时的position位置
        PositionImpl maxReadPosition = recentlyJoinedConsumers.get(consumer);
        // 此时，所有旧消息都已被消费，该消费者现在已准备好接收任何消息.
        // At this point, all the old messages were already consumed and this consumer
        // is now ready to receive any message
        if (maxReadPosition == null) {
            // 停止调度被困住的消费者.
            // stop to dispatch by stuckConsumers
            if (stuckConsumers.contains(consumer)) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] stop to dispatch by stuckConsumers, consumer: {}", name, consumer);
                }
                return 0;
            }
            // 如果为null，说明这个consumer不是最近加入的，所以可以发送全部的消息
            // The consumer has not recently joined, so we can send all messages
            return maxMessages;
        }

        // 如果读取类型是Replay，我们应该避免将其他消费者持有的消息发送给新消费者，例如，我们有 10 条消息 [0,1,2,3,4,5,6,7,8,9]
        // 如果consumer0得到消息0和1，并且没有acked message 0，那么consumer1加入，
        // 当consumer1 得到消息2,3 时，broker 不会向consumer1 发送消息，因为mark delete 位置没有前移。
        // 因此消息 2,3 将存储在重新传递跟踪器中。
        // 现在，consumer2 加入，它会从游标读取新消息，所以对于 consumer2，recentJoinedPosition 是 4.
        // 因为有消息需要重新投递，所以broker会先读取重投消息[2,3], 消息[2,3]低于recentJoinedPosition 4,
        // 因此消息 [2,3] 将发送给消费者 2
        // 但是消息 [2,3] 不应该发送给 consumer2。

        // If the read type is Replay, we should avoid send messages that hold by other consumer to the new consumers,
        // For example, we have 10 messages [0,1,2,3,4,5,6,7,8,9]
        // If the consumer0 get message 0 and 1, and does not acked message 0, then consumer1 joined,
        // when consumer1 get message 2,3, the broker will not dispatch messages to consumer1
        // because of the mark delete position did not move forward.
        // So message 2,3 will stored in the redeliver tracker.
        // Now, consumer2 joined, it will read new messages from the cursor,
        // so the recentJoinedPosition is 4 for consumer2
        // Because of there are messages need to redeliver, so the broker will read the redelivery message first [2,3]
        // message [2,3] is lower than the recentJoinedPosition 4,
        // so the message [2,3] will dispatched to the consumer2
        // But the message [2,3] should not dispatch to consumer2.

        if (readType == ReadType.Replay) {
            //这里会找到最近加入的consumer中，最小的read position
            PositionImpl minReadPositionForRecentJoinedConsumer = recentlyJoinedConsumers.values().iterator().next();
            if (minReadPositionForRecentJoinedConsumer != null
                    && minReadPositionForRecentJoinedConsumer.compareTo(maxReadPosition) < 0) {
                maxReadPosition = minReadPositionForRecentJoinedConsumer;
            }
        }

        // 在这里，消费者是最近加入的，所以我们只能发送在它加入之前发布的消息。
        // Here, the consumer is one that has recently joined, so we can only send messages that were
        // published before it has joined.
        for (int i = 0; i < maxMessages; i++) {
            if (((PositionImpl) entries.get(i).getPosition()).compareTo(maxReadPosition) >= 0) {
                //我们已经越过了分界线。 列表中的所有消息现在都比我们当前可以发送给该消费者的消息更新
                // We have already crossed the divider line. All messages in the list are now
                // newer than what we can currently dispatch to this consumer
                return i;
            }
        }

        return maxMessages;
    }

    @Override
    public synchronized void markDeletePositionMoveForward() {
        // 如果开启了保序，并且还有最近加入的consumer，那么会尝试从最近加入的consumer中移除掉被游标赶上的consumer
        if (recentlyJoinedConsumers != null && !recentlyJoinedConsumers.isEmpty()
                && removeConsumersFromRecentJoinedConsumers()) {
            // 在我们处理完ack之后，我们需要检查mark-delete位置是否提前，我们终于可以阅读更多的消息了。 多次调用 readMoreEntries() 是安全的。
            // After we process acks, we need to check whether the mark-delete position was advanced and we can finally
            // read more messages. It's safe to call readMoreEntries() multiple times.
            readMoreEntries();
        }
    }

    // 从最近加入的消费者中删除消费者
    //当consumer ack的数量达到此时的读取位置时，就会移除该consumer，从最近加入的map中
    private boolean removeConsumersFromRecentJoinedConsumers() {
        //遍历所有最近加入的consumer
        Iterator<Map.Entry<Consumer, PositionImpl>> itr = recentlyJoinedConsumers.entrySet().iterator();
        //记录是否有移除的consumer
        boolean hasConsumerRemovedFromTheRecentJoinedConsumers = false;
        //获取ack的位置
        PositionImpl mdp = (PositionImpl) cursor.getMarkDeletedPosition();
        //如果获取到了
        if (mdp != null) {
            //已确认位置的下一个位置
            PositionImpl nextPositionOfTheMarkDeletePosition =
                    ((ManagedLedgerImpl) cursor.getManagedLedger()).getNextValidPosition(mdp);
            while (itr.hasNext()) {
                //遍历所有consumer，当时加入的位置是否被游标位置追上了,如果追上了就从最近加入的consumer集合中移除
                Map.Entry<Consumer, PositionImpl> entry = itr.next();
                if (entry.getValue().compareTo(nextPositionOfTheMarkDeletePosition) <= 0) {
                    //从中移除该consumer
                    itr.remove();
                    hasConsumerRemovedFromTheRecentJoinedConsumers = true;
                } else {
                    //为什么不遍历完所有的才退出?因为recentlyJoinedConsumers是按照加入顺序添加的，如果遍历到有一个的读位置大于游标位置了，说明后面的都大于了（真的是这样吗，如果reset cursor呢）
                    //cursor一定就是单调递增的吗
                    break;
                }
            }
        }
        return hasConsumerRemovedFromTheRecentJoinedConsumers;
    }

    @Override
    protected synchronized Set<PositionImpl> getMessagesToReplayNow(int maxMessagesToRead) {
        //isDispatcherStuckOnReplays这个变量到底是什么意思,默认是false
        //判断dispatcher是否是卡在重播上，如果是，那么这里回将isDispatcherStuckOnReplays设置为false
        if (isDispatcherStuckOnReplays) {
            // 如果我们被困在重播上，我们希望继续阅读该主题（直到全部最大未确认消息开始），而不是继续重播相同的旧消息，因为这些消息路由到的消费者此时可能很忙
            // If we're stuck on replay, we want to move forward reading on the topic (until the overall max-unacked
            // messages kicks in), instead of keep replaying the same old messages, since the consumer that these
            // messages are routing to might be busy at the moment
            this.isDispatcherStuckOnReplays = false;
            return Collections.emptySet();
        } else {
            //如果没有卡在重发上，那么这里会直接从重发的消息中，选取maxMessagesToRead条来重发
            return super.getMessagesToReplayNow(maxMessagesToRead);
        }
    }

    //返回订阅类型
    @Override
    public SubType getType() {
        return SubType.Key_Shared;
    }

    //异步重放消息
    @Override
    protected Set<? extends Position> asyncReplayEntries(Set<? extends Position> positions) {
        return cursor.asyncReplayEntries(positions, this, ReadType.Replay, true);
    }
    //获取key share模式
    public KeySharedMode getKeySharedMode() {
        return this.keySharedMode;
    }
    //是允许乱序发送
    public boolean isAllowOutOfOrderDelivery() {
        return this.allowOutOfOrderDelivery;
    }

    //判断是否有相同的key share策略
    public boolean hasSameKeySharedPolicy(KeySharedMeta ksm) {
        return (ksm.getKeySharedMode() == this.keySharedMode
                && ksm.isAllowOutOfOrderDelivery() == this.allowOutOfOrderDelivery);
    }
    //返回最近加入的consumer集合
    public LinkedHashMap<Consumer, PositionImpl> getRecentlyJoinedConsumers() {
        return recentlyJoinedConsumers;
    }
    //返回consumer和range对应的map
    public Map<Consumer, List<Range>> getConsumerKeyHashRanges() {
        return selector.getConsumerKeyHashRanges();
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentStickyKeyDispatcherMultipleConsumers.class);

}

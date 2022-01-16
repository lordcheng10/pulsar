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
package org.apache.pulsar.broker.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.common.api.proto.IntRange;
import org.apache.pulsar.common.api.proto.KeySharedMeta;

/**
 * 这是基于用户提供的范围的 粘键消费者选择器。
 * This is a sticky-key consumer selector based user provided range.
 * 用户有责任确保为所有消费者提供的范围覆盖 rangeSize ，否则密钥可能会落入未被任何消费者处理的“整体”中。
 * User is responsible for making sure provided range for all consumers cover the rangeSize
 * else there'll be chance that a key fall in a `whole` that not handled by any consumer.
 */
public class HashRangeExclusiveStickyKeyConsumerSelector implements StickyKeyConsumerSelector {
    //获取range大小
    private final int rangeSize;
    //每个range对应的consumer
    private final ConcurrentSkipListMap<Integer, Consumer> rangeMap;

    //rangeSize默认值也是2的15次方
    public HashRangeExclusiveStickyKeyConsumerSelector() {
        this(DEFAULT_RANGE_SIZE);
    }

    public HashRangeExclusiveStickyKeyConsumerSelector(int rangeSize) {
        super();
        //如果range size小于1，那么久抛异常
        if (rangeSize < 1) {
            throw new IllegalArgumentException("range size must greater than 0");
        }
        this.rangeSize = rangeSize;
        this.rangeMap = new ConcurrentSkipListMap<>();
    }

    @Override
    public void addConsumer(Consumer consumer) throws BrokerServiceException.ConsumerAssignException {
        //加入consumer时，检查该consumer的key share模式是否合法
        validateKeySharedMeta(consumer);
        //看起来这个是用户直接指定用哪个range呀，在consumer加入的时候，就指定好用哪个range了,
        //但如果要用户指定的话，那么必须给定的范围不能有漏洞，不能某个范围没有对应的consumer，这样就不能推送消息了.这个一般不怎么推进使用
        for (IntRange intRange : consumer.getKeySharedMeta().getHashRangesList()) {
            //这里记录下range对应的consumer和consumer对应的range
            rangeMap.put(intRange.getStart(), consumer);
            rangeMap.put(intRange.getEnd(), consumer);
        }
    }

    @Override
    public void removeConsumer(Consumer consumer) {
        //移除就简单，遍历找到该consumer移除掉
        rangeMap.entrySet().removeIf(entry -> entry.getValue().equals(consumer));
    }

    @Override
    public Map<Consumer, List<Range>> getConsumerKeyHashRanges() {
        Map<Consumer, List<Range>> result = new HashMap<>();
        Map.Entry<Integer, Consumer> prev = null;
        for (Map.Entry<Integer, Consumer> entry: rangeMap.entrySet()) {
            if (prev == null) {
                prev = entry;
            } else {
                if (prev.getValue().equals(entry.getValue())) {
                    result.computeIfAbsent(entry.getValue(), key -> new ArrayList<>())
                            .add(Range.of(prev.getKey(), entry.getKey()));
                }
                prev = null;
            }
        }
        return result;
    }

    @Override
    public Consumer select(int hash) {
        if (rangeMap.size() > 0) {
            int slot = hash % rangeSize;
            Map.Entry<Integer, Consumer> ceilingEntry = rangeMap.ceilingEntry(slot);
            Map.Entry<Integer, Consumer> floorEntry = rangeMap.floorEntry(slot);
            Consumer ceilingConsumer = ceilingEntry != null ? ceilingEntry.getValue() : null;
            Consumer floorConsumer = floorEntry != null ? floorEntry.getValue() : null;
            if (floorConsumer != null && floorConsumer.equals(ceilingConsumer)) {
                return ceilingConsumer;
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    private void validateKeySharedMeta(Consumer consumer) throws BrokerServiceException.ConsumerAssignException {
        //如果没有指定key share模式，那么肯定抛异常.因为走到这里，说明是指定了key share模式的
        if (consumer.getKeySharedMeta() == null) {
            throw new BrokerServiceException.ConsumerAssignException("Must specify key shared meta for consumer.");
        }
        //获取指定的范围
        List<IntRange> ranges = consumer.getKeySharedMeta().getHashRangesList();
        //如果这个范围列表为空也要抛异常
        if (ranges.isEmpty()) {
            throw new BrokerServiceException.ConsumerAssignException("Ranges for KeyShared policy must not be empty.");
        }
        //遍历每一个范围
        for (IntRange intRange : ranges) {
            //如果开始位置大于结束位置，那么也抛异常，说明用户给定错了
            if (intRange.getStart() > intRange.getEnd()) {
                throw new BrokerServiceException.ConsumerAssignException("Fixed hash range start > end");
            }

            //这里举个例子，比如已经注册了1~8、12~15，然后此时第3个consumer来注册range是7~13，那么这里就会找到对应的ceilingEntry.range=12~15
            // 和floorEntry.range=1~8，然后取判断12是否大于13，这里发现并不是，所以给定的end值就和之前注册的consumer冲突了；通用的比较下限值;
            //获取大于等于给定开始位置的最接近的entry,这里获取到和该consumer网上最接近的一个范围
            Map.Entry<Integer, Consumer> ceilingEntry = rangeMap.ceilingEntry(intRange.getStart());
            //获取小于等于end值的最接近的entry，这里获取到和该consumer往下最接近的一个范围
            Map.Entry<Integer, Consumer> floorEntry = rangeMap.floorEntry(intRange.getEnd());

            //如果找到的比给定范围小的最接近的已经注册的范围和这里给定的下边界start重合了，那么久抛异常说冲突了，也就是用户上报的各个consumer的 range有重叠冲突
            if (floorEntry != null && floorEntry.getKey() >= intRange.getStart()) {
                throw new BrokerServiceException.ConsumerAssignException("Range conflict with consumer "
                        + floorEntry.getValue());
            }
            //给定的范围上边界和上面最接近的一个range看看是否有重叠冲突
            if (ceilingEntry != null && ceilingEntry.getKey() <= intRange.getEnd()) {
                throw new BrokerServiceException.ConsumerAssignException("Range conflict with consumer "
                        + ceilingEntry.getValue());
            }

            //这个看起来像是1~8和12~15都是同一个consumer的，然后这里注册了一个9~11的consumer2，这里还是在比较范围是否有冲突
            //有点没想明白,这里判断有点多余吧 只要上面的两个判断满足，这里肯定会满足的啊
            if (ceilingEntry != null && floorEntry != null && ceilingEntry.getValue().equals(floorEntry.getValue())) {
                KeySharedMeta keySharedMeta = ceilingEntry.getValue().getKeySharedMeta();
                for (IntRange range : keySharedMeta.getHashRangesList()) {
                    int start = Math.max(intRange.getStart(), range.getStart());
                    int end = Math.min(intRange.getEnd(), range.getEnd());
                    if (end >= start) {
                        throw new BrokerServiceException.ConsumerAssignException("Range conflict with consumer "
                                + ceilingEntry.getValue());
                    }
                }
            }
        }
    }

    //这里是获取每个range对应的consumer
    Map<Integer, Consumer> getRangeConsumer() {
        return Collections.unmodifiableMap(rangeMap);
    }

}

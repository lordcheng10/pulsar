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
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerAssignException;
import org.apache.pulsar.client.api.Range;

/**
 * 这是一个基于固定哈希范围的消费者选择器.
 * 1.每个消费者服务一个固定范围的哈希值;
 * 2.全范围的哈希值可以被所有消费者覆盖;
 * 3.一旦消费者被移除，剩下的消费者仍然可以服务于整个范围;
 *
 * This is a consumer selector based fixed hash range.
 *
 * 1.Each consumer serves a fixed range of hash value
 * 2.The whole range of hash value could be covered by all the consumers.
 * 3.Once a consumer is removed, the left consumers could still serve the whole range.
 *
 * 使用固定哈希范围初始化，默认为 2 << 15。
 * Initializing with a fixed hash range, by default 2 << 15.
 * 添加的第一个消费者，哈希范围如下所示：
 * First consumer added, hash range looks like:
 *
 * 0 -> 65536(consumer-1)  （2 << 15值是65536）
 *
 * 添加的第二个消费者，将找到一个最大的分割范围：
 * Second consumer added, will find a biggest range to split:
 *
 * 0 -> 32768(consumer-2) -> 65536(consumer-1)
 *
 * 当一个消费者被移除时，这个消费者的范围将被其他消费者接管，消费者2被移除：
 * While a consumer removed, The range for this consumer will be taken over
 * by other consumer, consumer-2 removed:
 *
 * 0 -> 65536(consumer-1)
 * 在这种方法中，使用跳过列表映射来维护哈希范围和消费者。
 * In this approach use skip list map to maintain the hash range and consumers.
 * 选择消费者将返回消息键哈希码 % 范围大小的上限键。
 * Select consumer will return the ceiling key of message key hashcode % range size.
 *
 */
public class HashRangeAutoSplitStickyKeyConsumerSelector implements StickyKeyConsumerSelector {
    //范围大小，干嘛的,这个是初始化给定范围的值，默认值应该是65536（2 << 15,也就是2的15次方）
    private final int rangeSize;
    //key是range范围的end值，value是consumer值
    private final ConcurrentSkipListMap<Integer, Consumer> rangeMap;
    //key是consumer值，value是range范围值
    private final Map<Consumer, Integer> consumerRange;

    //rangeSize默认值65536，也就是说一个partiiton最多只能被6万多个consumer消费。
    public HashRangeAutoSplitStickyKeyConsumerSelector() {
        this(DEFAULT_RANGE_SIZE);
    }

    public HashRangeAutoSplitStickyKeyConsumerSelector(int rangeSize) {
        //rangeSize至少都要大于2
        if (rangeSize < 2) {
            throw new IllegalArgumentException("range size must greater than 2");
        }
        //必须是2的多少次方
        if (!is2Power(rangeSize)) {
            throw new IllegalArgumentException("range size must be nth power with 2");
        }
        //每个范围对应的consumer
        this.rangeMap = new ConcurrentSkipListMap<>();
        //每个consumer对应的范围
        this.consumerRange = new HashMap<>();
        //最大范围大小
        this.rangeSize = rangeSize;
    }

    @Override
    public synchronized void addConsumer(Consumer consumer) throws ConsumerAssignException {
        //添加consumer，如果是第一个consumer，那么就直接加入rangeMap和consumerRange进去
        if (rangeMap.size() == 0) {
            rangeMap.put(rangeSize, consumer);
            consumerRange.put(consumer, rangeSize);
        } else {
            //否则就进行split：选出一个范围最大的来进行split
            splitRange(findBiggestRange(), consumer);
        }
    }

    @Override
    public synchronized void removeConsumer(Consumer consumer) {
        //首先从consumerRange中移除该consumer，返回该consumer对应的range范围
        Integer removeRange = consumerRange.remove(consumer);
        if (removeRange != null) {//如果范围不为null，那么就说明存在该consumer.如果为null，就不存在，那么久不用管
            if (removeRange == rangeSize && rangeMap.size() > 1) {//如果是最后一个范围，并且不止一个consumer
                //因为不止一个范围，所以这里lowerEntry肯定不会为null
                Map.Entry<Integer, Consumer> lowerEntry = rangeMap.lowerEntry(removeRange);
                //更新removeRange对应的consumer
                rangeMap.put(removeRange, lowerEntry.getValue());
                //删除原来lowerEntry对应的consumer对应的range
                rangeMap.remove(lowerEntry.getKey());
                //更新该consumer对应的range范围
                consumerRange.put(lowerEntry.getValue(), removeRange);
            } else {//如果不是最后一个范围，或是最后一个，但本来就只有1个consumer，那么就直接移除该consumer从rangeMap中
                rangeMap.remove(removeRange);
            }
        }
    }

    @Override
    public Consumer select(int hash) {
        if (rangeMap.size() > 0) {
            //首先通过取模运算来获取对应的插槽位置
            int slot = hash % rangeSize;
            //ceilingEntry会返回大于等于slot的最接近的entry<range,consumer>
            return rangeMap.ceilingEntry(slot).getValue();
        } else {
            //如果rangeMap为空，说明没有consumer，那么久返回null
            return null;
        }
    }

    @Override
    public Map<Consumer, List<Range>> getConsumerKeyHashRanges() {
        Map<Consumer, List<Range>> result = new HashMap<>();
        int start = 0;
        //遍历每个range和consumer的映射，这里应该是排序好了的 ，所以才可以直接用上一个entry的range+1作为下一个的start
        //ConcurrentSkipListMap会按照key进行排序
        for (Map.Entry<Integer, Consumer> entry: rangeMap.entrySet()) {
            result.computeIfAbsent(entry.getValue(), key -> new ArrayList<>())
                    .add(Range.of(start, entry.getKey()));
            start = entry.getKey() + 1;
        }
        //最后返回每个consumer对应的range范围
        return result;
    }

    //找到间隔最大的区间范围
    private int findBiggestRange() {
        //slots:插槽 区间有多大就意味着有多少个插槽
        int slots = 0;
        //最繁忙的范围，其实就是间隔最大的区间
        int busiestRange = rangeSize;
        //遍历每个consumer
        for (Entry<Integer, Consumer> entry : rangeMap.entrySet()) {
            //找到小于该key的最接近的key值
            Integer lowerKey = rangeMap.lowerKey(entry.getKey());
            //如果没找到，那么开始位置就是0
            if (lowerKey == null) {
                lowerKey = 0;
            }
            //这里用该key减去距离他最近的上一个key值，就可以得到范围值，然后和记录的上一次范围slots比较，从中选出最大的范围
            if (entry.getKey() - lowerKey > slots) {
                slots = entry.getKey() - lowerKey;
                busiestRange = entry.getKey();
            }
        }
        //busiestRange是范围中的end值，但由于所有consumer是均分0 -> 65536范围，所以只要知道范围的end值，和consumer数，那么就可以算出起始位置
        return busiestRange;
    }

    private void splitRange(int range, Consumer targetConsumer) throws ConsumerAssignException {
        //通过查找小于该range的最接近的key值，其实也就是该范围的开始位置
        Integer lowerKey = rangeMap.lowerKey(range);
        if (lowerKey == null) {//如果开始位置为null，那么该位置就是第一个位置，开始位置就是0
            lowerKey = 0;
        }
        //如果end-start小于等于1，那么久不能split了，此时会报错
        if (range - lowerKey <= 1) {
            throw new ConsumerAssignException("No more range can assigned to new consumer, assigned consumers "
                    + rangeMap.size());
        }
        //计算分割后的范围，也就是从中间一分为二，eg: 0 -> 65536(consumer-1) ->0 -> 32768(consumer-2) -> 65536(consumer-1)
        int splitRange = range - ((range - lowerKey) >> 1);
        //计算好后放入rangeMap和consumerRange中
        rangeMap.put(splitRange, targetConsumer);
        consumerRange.put(targetConsumer, splitRange);
    }

    //返单给定的值是否是2的n次方
    private boolean is2Power(int num) {
        if (num < 2) {
            return false;
        }
        return (num & num - 1) == 0;
    }

    Map<Integer, Consumer> getRangeConsumer() {
        return Collections.unmodifiableMap(rangeMap);
    }
}

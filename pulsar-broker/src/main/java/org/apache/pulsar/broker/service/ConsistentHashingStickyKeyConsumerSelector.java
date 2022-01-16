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

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerAssignException;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.common.util.Murmur3_32Hash;

/**
 * 这是一个基于消费者选择器的固定哈希范围。
 * This is a consumer selector based fixed hash range.
 *
 * 该实现使用一致的散列来平均分配分配给每个消费者的键的数量。
 * The implementation uses consistent hashing to evenly split, the
 * number of keys assigned to each consumer.
 */
public class ConsistentHashingStickyKeyConsumerSelector implements StickyKeyConsumerSelector {

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    // Consistent-Hash ring
    private final NavigableMap<Integer, List<Consumer>> hashRing;

    private final int numberOfPoints;

    public ConsistentHashingStickyKeyConsumerSelector(int numberOfPoints) {
        this.hashRing = new TreeMap<>();
        this.numberOfPoints = numberOfPoints;
    }

    @Override
    public void addConsumer(Consumer consumer) throws ConsumerAssignException {
        rwLock.writeLock().lock();
        try {
            //为每个消费者在哈希环上插入多个点
            //这些点是根据消费者名称的哈希确定性添加的
            // Insert multiple points on the hash ring for every consumer
            // The points are deterministically added based on the hash of the consumer name
            for (int i = 0; i < numberOfPoints; i++) {
                //计算key
                String key = consumer.consumerName() + i;
                //计算hash值
                int hash = Murmur3_32Hash.getInstance().makeHash(key.getBytes());
                // 一个consumer会加入到多个list中
                hashRing.compute(hash, (k, v) -> {
                    if (v == null) {
                        return Lists.newArrayList(consumer);
                    } else {
                        if (!v.contains(consumer)) {
                            //加入到hashRing中
                            v.add(consumer);
                            //排序
                            v.sort(Comparator.comparing(Consumer::consumerName, String::compareTo));
                        }
                        return v;
                    }
                });
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void removeConsumer(Consumer consumer) {
        rwLock.writeLock().lock();
        try {
            // Remove all the points that were added for this consumer
            for (int i = 0; i < numberOfPoints; i++) {
                //构建key
                String key = consumer.consumerName() + i;
                //获取hash值
                int hash = Murmur3_32Hash.getInstance().makeHash(key.getBytes());
                hashRing.compute(hash, (k, v) -> {
                    if (v == null) {
                        //如果不存在该hash值，那么久直接返回null
                        return null;
                    } else {
                        //从中移除该consumer
                        v.removeIf(c -> c.consumerName().equals(consumer.consumerName()));
                        if (v.isEmpty()) {
                            //如果移除后为空，那么就重置value为null
                            v = null;
                        }
                        return v;
                    }
                });
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public Consumer select(int hash) {
        rwLock.readLock().lock();
        try {
            //如果hashRing为空，那么久直接返回null
            if (hashRing.isEmpty()) {
                return null;
            }

            List<Consumer> consumerList;
            //从hashRing中找出全部大于hash的consumer
            Map.Entry<Integer, List<Consumer>> ceilingEntry = hashRing.ceilingEntry(hash);
            if (ceilingEntry != null) {
                consumerList =  ceilingEntry.getValue();
            } else {
                //如果所有hash值都小于给定的hash值，那么久从所有的consumer中去选取
                consumerList = hashRing.firstEntry().getValue();
            }

            //这里用给定的hash值，莫除以选出的consumer数，这里consumer列表大概率是会存在重复的consumer
            return consumerList.get(hash % consumerList.size());
        } finally {
            rwLock.readLock().unlock();
        }
    }

    //返回每个consumer对应的hash范围,因为同一个consumeer可能对应了多个key，所以这里的范围也有可能是多个
    @Override
    public Map<Consumer, List<Range>> getConsumerKeyHashRanges() {
        Map<Consumer, List<Range>> result = new LinkedHashMap<>();
        rwLock.readLock().lock();
        try {
            int start = 0;
            //遍历所有的consumer,注意这里的hashRing是一个TreeMap对象，是按照key排好序的，所以这里遍历的key是从小到大遍历的.第一个元素hash范围开始位置为0
            for (Map.Entry<Integer, List<Consumer>> entry: hashRing.entrySet()) {
                for (Consumer consumer: entry.getValue()) {
                    result.computeIfAbsent(consumer, key -> new ArrayList<>())
                            //start是在hashRing中的index编号，end是该consumer对应的hash值
                            .add(Range.of(start, entry.getKey()));
                }
                start = entry.getKey() + 1;
            }
        } finally {
            rwLock.readLock().unlock();
        }
        return result;
    }

    Map<Integer, List<Consumer>> getRangeConsumer() {
        return Collections.unmodifiableMap(hashRing);
    }
}

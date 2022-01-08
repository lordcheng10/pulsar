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
package org.apache.bookkeeper.mledger.util;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.Lists;
import io.netty.util.ReferenceCounted;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.tuple.Pair;

/**
 * 特殊类型的缓存，其中 get() 和 delete() 操作可以在一系列键上完成。
 * Special type of cache where get() and delete() operations can be done over a range of keys.
 *
 * @param <Key> 缓存键。 需要有可比性
 *            Cache key. Needs to be Comparable
 * @param <Value> 缓存值
 *            Cache value
 */
public class RangeCache<Key extends Comparable<Key>, Value extends ReferenceCounted> {
    // Map from key to nodes inside the linked list
    private final ConcurrentNavigableMap<Key, Value> entries;
    //当前cache中总的数据size
    private AtomicLong size; // Total size of values stored in cache
    //这个实际是一个通用的函数接口：用于从值中提取大小的加权对象
    private final Weighter<Value> weighter; // Weighter object used to extract the size from values
    //这个也是一个函数接口：提取与值关联的时间戳
    private final TimestampExtractor<Value> timestampExtractor; // Extract the timestamp associated with a value

    /**
     * Construct a new RangeLruCache with default Weighter.
     */
    public RangeCache() {
        this(new DefaultWeighter<>(), (x) -> System.nanoTime());
    }

    /**
     * Construct a new RangeLruCache.
     *
     * @param weighter
     *            a custom weighter to compute the size of each stored value
     */
    public RangeCache(Weighter<Value> weighter, TimestampExtractor<Value> timestampExtractor) {
        this.size = new AtomicLong(0);
        this.entries = new ConcurrentSkipListMap<>();
        this.weighter = weighter;
        this.timestampExtractor = timestampExtractor;
    }

    /**
     * Insert.
     *
     * @param key
     * @param value
     *            ref counted value with at least 1 ref to pass on the cache
     * @return whether the entry was inserted in the cache
     */
    public boolean put(Key key, Value value) {
        if (entries.putIfAbsent(key, value) == null) {
            size.addAndGet(weighter.getSize(value));
            return true;
        } else {
            return false;
        }
    }

    public Value get(Key key) {
        Value value = entries.get(key);
        if (value == null) {
            return null;
        } else {
            try {
                value.retain();
                return value;
            } catch (Throwable t) {
                // Value was already destroyed between get() and retain()
                return null;
            }
        }
    }

    /**
     *
     * @param first
     *            the first key in the range
     * @param last
     *            the last key in the range (inclusive)
     * @return a collections of the value found in cache
     */
    public Collection<Value> getRange(Key first, Key last) {
        List<Value> values = Lists.newArrayList();

        // Return the values of the entries found in cache
        for (Value value : entries.subMap(first, true, last, true).values()) {
            try {
                value.retain();
                values.add(value);
            } catch (Throwable t) {
                // Value was already destroyed between get() and retain()
            }
        }

        return values;
    }

    /**
     *
     * @param first
     * @param last
     * @param lastInclusive
     * @return an pair of ints, containing the number of removed entries and the total size
     */
    public Pair<Integer, Long> removeRange(Key first, Key last, boolean lastInclusive) {
        Map<Key, Value> subMap = entries.subMap(first, true, last, lastInclusive);

        int removedEntries = 0;
        long removedSize = 0;

        for (Key key : subMap.keySet()) {
            Value value = entries.remove(key);
            if (value == null) {
                continue;
            }

            removedSize += weighter.getSize(value);
            value.release();
            ++removedEntries;
        }

        size.addAndGet(-removedSize);

        return Pair.of(removedEntries, removedSize);
    }

    /**
     *
     * @param minSize
     * @return a pair containing the number of entries evicted and their total size 一对包含被驱逐的条目数量及其总大小
     */
    public Pair<Integer, Long> evictLeastAccessedEntries(long minSize) {
        //检查要驱逐的size是否合法
        checkArgument(minSize > 0);

        //记录已经移除的size和entry数
        long removedSize = 0;
        int removedEntries = 0;

        while (removedSize < minSize) {
            //移除掉key最小的entry，这里就是position最小的entry, 这里的key是PositionImpl类，所以这里是选出ledger最小，并且entryId最小的entry来删除
            Map.Entry<Key, Value> entry = entries.pollFirstEntry();
            //如果没有，就直接退出循环
            if (entry == null) {
                break;
            }

            //获取对应的value值，就是一个EntryImpl类
            Value value = entry.getValue();
            //移除的entry数+1
            ++removedEntries;
            //获取该entry的size，这里直接调用EntryImpl.lenght不就得到了吗 ，为啥还要通过weighter来获得
            // 这里用的是函数接口，实际调用的是EntryImpl::getLength方法
            removedSize += weighter.getSize(value);
            //释放该entry的buffer
            value.release();
        }

        //从cache里面减去驱逐的数据size
        size.addAndGet(-removedSize);
        //返回驱逐的entry数和数据size
        return Pair.of(removedEntries, removedSize);
    }

    /**
    *
    * @param maxTimestamp the max timestamp of the entries to be evicted
    * @return the tota
    */
   public long evictLEntriesBeforeTimestamp(long maxTimestamp) {
       long removedSize = 0;

       while (true) {
           Map.Entry<Key, Value> entry = entries.firstEntry();
           if (entry == null || timestampExtractor.getTimestamp(entry.getValue()) > maxTimestamp) {
               break;
           }

           entry = entries.pollFirstEntry();
           if (entry == null) {
               break;
           }

           Value value = entry.getValue();
           removedSize += weighter.getSize(value);
           value.release();
       }

       size.addAndGet(-removedSize);
       return removedSize;
   }

    /**
     * Just for testing. Getting the number of entries is very expensive on the conncurrent map
     */
    protected long getNumberOfEntries() {
        return entries.size();
    }

    public long getSize() {
        return size.get();
    }

    /**
     * Remove all the entries from the cache.
     *
     * @return size of removed entries
     */
    public synchronized long clear() {
        long removedSize = 0;

        while (true) {
            Map.Entry<Key, Value> entry = entries.pollFirstEntry();
            if (entry == null) {
                break;
            }
            Value value = entry.getValue();
            removedSize += weighter.getSize(value);
            value.release();
        }

        entries.clear();
        size.getAndAdd(-removedSize);
        return removedSize;
    }

    /**
     * 这个实际就是一个函数接口
     * 能够提取缓存值的“权重”（大小/成本/空间）的对象接口。
     * Interface of a object that is able to the extract the "weight" (size/cost/space) of the cached values.
     *
     * @param <ValueT>
     */
    public interface Weighter<ValueT> {
        long getSize(ValueT value);
    }

    /**
     * Interface of a object that is able to the extract the "timestamp" of the cached values.
     *
     * @param <ValueT>
     */
    public interface TimestampExtractor<ValueT> {
        long getTimestamp(ValueT value);
    }

    /**
     * Default cache weighter, every value is assumed the same cost.
     *
     * @param <Value>
     */
    private static class DefaultWeighter<Value> implements Weighter<Value> {
        @Override
        public long getSize(Value value) {
            return 1;
        }
    }

}

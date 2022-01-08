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
package org.apache.bookkeeper.mledger.impl;

import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.commons.lang3.tuple.Pair;

/**
 * 单个 ManagedLedger 使用的条目缓存。
 * EntryCache 与其他 EntryCache 实例使用它们的大小（每个实例占用的内存）进行比较。
 *
 * Cache of entries used by a single ManagedLedger. An EntryCache is compared to other EntryCache instances using their
 * size (the memory that is occupied by each of them).
 */
public interface EntryCache extends Comparable<EntryCache> {

    /**
     * 缓存的名称.
     *
     * @return the name of the cache
     */
    String getName();

    /**
     * 在缓存中插入一个条目。
     * Insert an entry in the cache.
     *
     * 如果已达到总体限制，这将触发其他条目的驱逐，可能来自其他 EntryCache 实例.
     *
     * @param entry 要缓存的条目.
     * @return 条目是否插入缓存中.
     * <p/>If the overall limit have been reached, this will triggered the eviction of other entries, possibly from
     * other EntryCache instances
     *
     * @param entry
     *            the entry to be cached
     * @return whether the entry was inserted in cache
     */
    boolean insert(EntryImpl entry);

    /**
     * 从缓存中删除所有与账本相关的条目，直到包含的 lastPosition。
     * @param lastPosition
     * 要失效的最后一个条目位置（不包括在内）
     *
     * Remove from cache all the entries related to a ledger up to lastPosition included.
     *
     * @param lastPosition
     *            the position of the last entry to be invalidated (non-inclusive)
     */
    void invalidateEntries(PositionImpl lastPosition);
    // 使时间戳之前的条目无效
    void invalidateEntriesBeforeTimestamp(long timestamp);

    /**
     * 从缓存中删除属于特定分类帐的所有条目。
     * Remove from the cache all the entries belonging to a specific ledger.
     *
     * @param ledgerId
     *            the ledger id
     */
    void invalidateAllEntries(long ledgerId);

    /**
     * 从缓存中删除所有条目。
     * Remove all the entries from the cache.
     */
    void clear();

    /**
     * 强制删除缓存条目以释放空间。
     * Force the cache to drop entries to free space.
     * @param sizeToFree
     * 要释放的总内存大小.
     * @return
     * 一对包含被驱逐的条目数及其总大小.
     *
     * @param sizeToFree
     *            the total memory size to free
     * @return a pair containing the number of entries evicted and their total size
     */
    Pair<Integer, Long> evictEntries(long sizeToFree);

    /**
     * 从缓存或簿记员读取条目。
     * 从缓存或簿记员获取条目数据，并将结果混合在一个列表中。
     *
     * Read entries from the cache or from bookkeeper.
     *
     * <p/>Get the entry data either from cache or bookkeeper and mixes up the results in a single list.
     *
     * @param lh
     *            the ledger handle
     * @param firstEntry
     *            the first entry to read (inclusive)
     * @param lastEntry
     *            the last entry to read (inclusive)
     * @param isSlowestReader
     *            whether the reader cursor is the most far behind in the stream
     * @param callback
     *            the callback object that will be notified when read is done
     * @param ctx
     *            the context object
     */
    void asyncReadEntry(ReadHandle lh, long firstEntry, long lastEntry, boolean isSlowestReader,
            ReadEntriesCallback callback, Object ctx);

    /**
     * Read entry at given position from the cache or from bookkeeper.
     *
     * <p/>Get the entry data either from cache or bookkeeper and mixes up the results in a single list.
     *
     * @param lh
     *            the ledger handle
     * @param position
     *            position to read the entry from
     * @param callback
     *            the callback object that will be notified when read is done
     * @param ctx
     *            the context object
     */
    void asyncReadEntry(ReadHandle lh, PositionImpl position, ReadEntryCallback callback, Object ctx);

    /**
     * Get the total size in bytes of all the entries stored in this cache.
     *
     * @return the size of the entry cache
     */
    long getSize();
}

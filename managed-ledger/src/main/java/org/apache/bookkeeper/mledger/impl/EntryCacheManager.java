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

import static org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.createManagedLedgerException;
import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import io.netty.buffer.ByteBuf;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// 抑制警告
@SuppressWarnings("checkstyle:javadoctype")
public class EntryCacheManager {
    //cache配置的最大size
    private final long maxSize;
    //驱逐阈值：cache中缓存的数据达到多大时进行驱逐
    private final long evictionTriggerThreshold;
    //cache驱逐水位
    private final double cacheEvictionWatermark;
    //cache当前使用的大小
    private final AtomicLong currentSize = new AtomicLong(0);
    //实际存放entry的cache
    private final ConcurrentMap<String, EntryCache> caches = Maps.newConcurrentMap();
    //驱逐策略，目前只有一个默认的驱逐策略EntryCacheDefaultEvictionPolicy
    private final EntryCacheEvictionPolicy evictionPolicy;
    //是否正在驱逐
    private final AtomicBoolean evictionInProgress = new AtomicBoolean(false);
    //这个又是啥玩意
    private final ManagedLedgerFactoryImpl mlFactory;
    protected final ManagedLedgerFactoryMBeanImpl mlFactoryMBean;

    protected static final double MB = 1024 * 1024;

    //驱逐的阈值，写死的达到0.98就会驱逐
    private static final double evictionTriggerThresholdPercent = 0.98;


    public EntryCacheManager(ManagedLedgerFactoryImpl factory) {
        this.maxSize = factory.getConfig().getMaxCacheSize();
        this.evictionTriggerThreshold = (long) (maxSize * evictionTriggerThresholdPercent);
        this.cacheEvictionWatermark = factory.getConfig().getCacheEvictionWatermark();
        this.evictionPolicy = new EntryCacheDefaultEvictionPolicy();
        this.mlFactory = factory;
        this.mlFactoryMBean = factory.mbean;

        log.info("Initialized managed-ledger entry cache of {} Mb", maxSize / MB);
    }

    public EntryCache getEntryCache(ManagedLedgerImpl ml) {
        if (maxSize == 0) {
            // Cache is disabled
            return new EntryCacheDisabled(ml);
        }

        EntryCache newEntryCache = new EntryCacheImpl(this, ml, mlFactory.getConfig().isCopyEntriesInCache());
        EntryCache currentEntryCache = caches.putIfAbsent(ml.getName(), newEntryCache);
        if (currentEntryCache != null) {
            return currentEntryCache;
        } else {
            return newEntryCache;
        }
    }

    void removeEntryCache(String name) {
        EntryCache entryCache = caches.remove(name);
        if (entryCache == null) {
            return;
        }

        long size = entryCache.getSize();
        entryCache.clear();

        if (log.isDebugEnabled()) {
            log.debug("Removed cache for {} - Size: {} -- Current Size: {}", name, size / MB, currentSize.get() / MB);
        }
    }

    //判断cache中是否有多余空，如果没有就需要驱逐
    boolean hasSpaceInCache() {
        long currentSize = this.currentSize.get();

        //这里currentSize应该>=才对呀
        // 在后台触发一次驱逐。 当驱逐运行时，我们停止在缓存中插入条目
        // 当使用size超过阈值，并且此时并未正在驱逐数据，那么久需要在后台进行驱逐.那这样cache实际使用size 在后台驱逐过程中不就可能超过吗
        // Trigger a single eviction in background. While the eviction is running we stop inserting entries in the cache
        if (currentSize > evictionTriggerThreshold && evictionInProgress.compareAndSet(false, true)) {
            mlFactory.scheduledExecutor.execute(safeRun(() -> {
                // 触发新的缓存逐出周期，使已用内存低于 cacheEvictionWatermark 百分比限制
                // Trigger a new cache eviction cycle to bring the used memory below the cacheEvictionWatermark
                // percentage limit
                //要驱逐的sizesizeToEvict，助理这里的cacheEvictionWatermark一般会比evictionTriggerThresholdPercent低一点，因为不能比着阈值驱逐，应该多驱逐一点
                long sizeToEvict = currentSize - (long) (maxSize * cacheEvictionWatermark);
                //开始驱逐时间
                long startTime = System.nanoTime();
                log.info("Triggering cache eviction. total size: {} Mb -- Need to discard: {} Mb", currentSize / MB,
                        sizeToEvict / MB);

                try {
                    evictionPolicy.doEviction(Lists.newArrayList(caches.values()), sizeToEvict);

                    long endTime = System.nanoTime();
                    double durationMs = TimeUnit.NANOSECONDS.toMicros(endTime - startTime) / 1000.0;

                    log.info("Eviction completed. Removed {} Mb in {} ms", (currentSize - this.currentSize.get()) / MB,
                            durationMs);
                } finally {
                    //统计驱逐次数
                    mlFactoryMBean.recordCacheEviction();
                    evictionInProgress.set(false);
                }
            }));
        }

        //TODO-这里应该是小于evictionTriggerThreshold才对呀
        //判断cache使用大小是否小于maxSize
        //这样的条件，有可能会超过cache总大小
        return currentSize < maxSize;
    }

    void entryAdded(long size) {
        currentSize.addAndGet(size);
    }

    void entriesRemoved(long size) {
        currentSize.addAndGet(-size);
    }

    public long getSize() {
        return currentSize.get();
    }

    public long getMaxSize() {
        return maxSize;
    }

    public void clear() {
        caches.values().forEach(EntryCache::clear);
    }

    //关闭cache用这个
    protected class EntryCacheDisabled implements EntryCache {
        private final ManagedLedgerImpl ml;
        private final ManagedLedgerInterceptor interceptor;

        public EntryCacheDisabled(ManagedLedgerImpl ml) {
            this.ml = ml;
            this.interceptor = ml.getManagedLedgerInterceptor();
        }

        @Override
        public String getName() {
            return ml.getName();
        }

        @Override
        public boolean insert(EntryImpl entry) {
            return false;
        }

        @Override
        public void invalidateEntries(PositionImpl lastPosition) {
        }

        @Override
        public void invalidateAllEntries(long ledgerId) {
        }

        @Override
        public void clear() {
        }

        @Override
        public Pair<Integer, Long> evictEntries(long sizeToFree) {
            return Pair.of(0, (long) 0);
        }

        @Override
        public void invalidateEntriesBeforeTimestamp(long timestamp) {
        }

        @Override
        public void asyncReadEntry(ReadHandle lh, long firstEntry, long lastEntry, boolean isSlowestReader,
                final ReadEntriesCallback callback, Object ctx) {
            lh.readAsync(firstEntry, lastEntry).thenAcceptAsync(
                    ledgerEntries -> {
                        List<Entry> entries = Lists.newArrayList();
                        long totalSize = 0;
                        try {
                            for (LedgerEntry e : ledgerEntries) {
                                // Insert the entries at the end of the list (they will be unsorted for now)
                                EntryImpl entry = create(e, interceptor);
                                entries.add(entry);
                                totalSize += entry.getLength();
                            }
                        } finally {
                            ledgerEntries.close();
                        }
                        mlFactoryMBean.recordCacheMiss(entries.size(), totalSize);
                        ml.mbean.addReadEntriesSample(entries.size(), totalSize);

                        callback.readEntriesComplete(entries, ctx);
                    }, ml.getExecutor().chooseThread(ml.getName())).exceptionally(exception -> {
                        callback.readEntriesFailed(createManagedLedgerException(exception), ctx);
                        return null;
            });
        }

        @Override
        public void asyncReadEntry(ReadHandle lh, PositionImpl position, AsyncCallbacks.ReadEntryCallback callback,
                Object ctx) {
            lh.readAsync(position.getEntryId(), position.getEntryId()).whenCompleteAsync(
                    (ledgerEntries, exception) -> {
                        if (exception != null) {
                            ml.invalidateLedgerHandle(lh);
                            callback.readEntryFailed(createManagedLedgerException(exception), ctx);
                            return;
                        }

                        try {
                            Iterator<LedgerEntry> iterator = ledgerEntries.iterator();
                            if (iterator.hasNext()) {
                                LedgerEntry ledgerEntry = iterator.next();
                                EntryImpl returnEntry = create(ledgerEntry, interceptor);

                                mlFactoryMBean.recordCacheMiss(1, returnEntry.getLength());
                                ml.getMBean().addReadEntriesSample(1, returnEntry.getLength());
                                callback.readEntryComplete(returnEntry, ctx);
                            } else {
                                callback.readEntryFailed(new ManagedLedgerException("Could not read given position"), ctx);
                            }
                        } finally {
                            ledgerEntries.close();
                        }
                    }, ml.getExecutor().chooseThread(ml.getName()));
        }

        @Override
        public long getSize() {
            return 0;
        }

        @Override
        public int compareTo(EntryCache other) {
            return Longs.compare(getSize(), other.getSize());
        }

    }

    public static Entry create(long ledgerId, long entryId, ByteBuf data) {
        return EntryImpl.create(ledgerId, entryId, data);
    }

    public static EntryImpl create(LedgerEntry ledgerEntry, ManagedLedgerInterceptor interceptor) {
        ManagedLedgerInterceptor.PayloadProcessorHandle processorHandle = null;
        if (interceptor != null) {
            ByteBuf duplicateBuffer = ledgerEntry.getEntryBuffer().retainedDuplicate();
            processorHandle = interceptor
                    .processPayloadBeforeEntryCache(duplicateBuffer);
            if (processorHandle != null) {
                ledgerEntry  = LedgerEntryImpl.create(ledgerEntry.getLedgerId(),ledgerEntry.getEntryId(),
                        ledgerEntry.getLength(),processorHandle.getProcessedPayload());
            } else {
                duplicateBuffer.release();
            }
        }
        EntryImpl returnEntry = EntryImpl.create(ledgerEntry);
        if (processorHandle != null) {
            processorHandle.release();
            ledgerEntry.close();
        }
        return returnEntry;
    }

    private static final Logger log = LoggerFactory.getLogger(EntryCacheManager.class);
}

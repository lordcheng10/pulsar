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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.reverseOrder;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 默认驱逐策略。
 * Default eviction policy.
 *
 * 此策略仅考虑用于驱逐的较大缓存。
 * <p/>This policy consider only the bigger caches for doing eviction.
 *
 * PercentOfSizeToConsiderForEviction 参数应始终大于 cacheEvictionWatermak，否则驱逐周期将释放比所需内存少的内存。
 * <p/>The PercentOfSizeToConsiderForEviction parameter should always be bigger than the cacheEvictionWatermak,
 * otherwisethe eviction cycle will free less memory than what was required.
 */
public class EntryCacheDefaultEvictionPolicy implements EntryCacheEvictionPolicy {

    //驱逐考虑的大小百分比,写死的0.5
    private static final double PercentOfSizeToConsiderForEviction = 0.5;

    //从caches列表中，驱逐大小为sizeToFree的数据
    @Override
    public void doEviction(List<EntryCache> caches, long sizeToFree) {
        //检查sizeToFree参数,需要大于0
        checkArgument(sizeToFree > 0);
        //caches列表不能为空，否则需要抛异常
        checkArgument(!caches.isEmpty());

        //对cache列表，按照使用大小，从大到小排序
        caches.sort(reverseOrder());

        //算总cache大小
        long totalSize = 0;
        for (EntryCache cache : caches) {
            totalSize += cache.getSize();
        }

        // 该算法仅对组合大小达到 PercentOfSizeToConsiderForEviction 的缓存组应用驱逐
        // This algorithm apply the eviction only the group of caches whose combined size reaches the
        // PercentOfSizeToConsiderForEviction
        //要驱逐的cache列表
        List<EntryCache> cachesToEvict = Lists.newArrayList();
        //记录选择要驱逐的cache中的数据总大小
        long cachesToEvictTotalSize = 0;
        //想驱逐的size,一次最多驱逐50%的数据size，如果传入的sizeToFree大于50%的数据，那么也只能驱逐50%的数据
        long sizeToConsiderForEviction = (long) (totalSize * PercentOfSizeToConsiderForEviction);
        log.debug("Need to gather at least {} from caches", sizeToConsiderForEviction);

        int cacheIdx = 0;
        while (cachesToEvictTotalSize < sizeToConsiderForEviction) {
            // 考虑到我们无法释放比缓存中更多的大小，这个条件应该始终为真
            // This condition should always be true, considering that we cannot free more size that what we have in
            // cache
            checkArgument(cacheIdx < caches.size());

            //从cache列表中选出第一个，也就是使用size最多的cache先来驱逐处理
            EntryCache entryCache = caches.get(cacheIdx++);
            //记录驱逐了的数据size
            cachesToEvictTotalSize += entryCache.getSize();
            //将该cache加入到要驱逐的cache集合中
            cachesToEvict.add(entryCache);

            log.debug("Added cache {} with size {}", entryCache.getName(), entryCache.getSize());
        }

        //驱逐entry数
        int evictedEntries = 0;
        //驱逐数据大小
        long evictedSize = 0;
        //遍历每个需要驱逐的cache
        for (EntryCache entryCache : cachesToEvict) {
            // 对于每个选择用于驱逐的 entryCache，我们将要求驱逐一定比例的数据量,这个比例是用该cache中的数据总size/最多要区中的数据大小cachesToEvictTotalSize
            // To each entryCache chosen to for eviction, we'll ask to evict a proportional amount of data
            long singleCacheSizeToFree = (long) (sizeToFree * (entryCache.getSize() / (double) cachesToEvictTotalSize));

            if (singleCacheSizeToFree == 0) {
                // 如果此缓存的大小变为 0，则可能意味着自我们计算排名以来其条目已从缓存中删除
                // If the size of this cache went to 0, it probably means that its entries has been removed from the
                // cache since the time we've computed the ranking
                continue;
            }

            //从该cache中驱逐singleCacheSizeToFree大小的数据,返回<驱逐的entry数，驱逐的数据大小>
            //从该cache中，按照ledger和entryId从小到大进行驱逐，一直驱逐掉singleCacheSizeToFree数据大小
            Pair<Integer, Long> evicted = entryCache.evictEntries(singleCacheSizeToFree);
            //更新驱逐的entry数和数据大小
            evictedEntries += evicted.getLeft();
            evictedSize += evicted.getRight();
        }

        log.info("Completed cache eviction. Removed {} entries from {} caches. ({} Mb)", evictedEntries,
                cachesToEvict.size(), evictedSize / EntryCacheManager.MB);
    }

    private static final Logger log = LoggerFactory.getLogger(EntryCacheDefaultEvictionPolicy.class);
}

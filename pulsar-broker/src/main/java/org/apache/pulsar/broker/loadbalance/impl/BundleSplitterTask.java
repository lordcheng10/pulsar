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
package org.apache.pulsar.broker.loadbalance.impl;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TimeAverageMessageData;
import org.apache.pulsar.broker.loadbalance.BundleSplitStrategy;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Determines which bundles should be split based on various thresholds.
 */
public class BundleSplitterTask implements BundleSplitStrategy {
    private static final Logger log = LoggerFactory.getLogger(BundleSplitStrategy.class);
    private final Set<String> bundleCache;

    /**
     * Construct a BundleSplitterTask.
     *
     */
    public BundleSplitterTask() {
        bundleCache = new HashSet<>();
    }

    //基于特定阈值来决定哪些bundle需要被拆分，返回需要拆分的 bundle 集
    /**
     * Determines which bundles should be split based on various thresholds.
     *
     * @param loadData
     *            Load data to base decisions on (does not have benefit of preallocated data since this may not be the
     *            leader broker).
     * @param pulsar
     *            Service to use.
     * @return All bundles who have exceeded configured thresholds in number of topics, number of sessions, total
     *         message rates, or total throughput.
     */
    @Override
    public Set<String> findBundlesToSplit(final LoadData loadData, final PulsarService pulsar) {
        bundleCache.clear();
        final ServiceConfiguration conf = pulsar.getConfiguration();
        //命名空间中包的最大数量
        //如果一个bundle里面某一个namespace在整个集群的bundle总数超过阈值了，那么该namespace所涉及的bundle就不能再进行split了，
        //为啥要做这个限制呢，感觉如果一个namespace下topic太多了，甚至可以split到一个bundle一个topic呀？
        int maxBundleCount = conf.getLoadBalancerNamespaceMaximumBundles();
        //任意bundle上最大topic数
        long maxBundleTopics = conf.getLoadBalancerNamespaceBundleMaxTopics();
        //任意bundle上最大会话数
        long maxBundleSessions = conf.getLoadBalancerNamespaceBundleMaxSessions();
        //任意bundle上最大消息速率
        long maxBundleMsgRate = conf.getLoadBalancerNamespaceBundleMaxMsgRate();
        //任意bundle上最大带宽（包括扇入扇出）
        long maxBundleBandwidth = conf.getLoadBalancerNamespaceBundleMaxBandwidthMbytes() * LoadManagerShared.MIBI;
        //先按照每个主机来进行迭代
        loadData.getBrokerData().forEach((broker, brokerData) -> {
            LocalBrokerData localData = brokerData.getLocalData();
            //一个主机下，所有的 NamespaceBundle 负载数据迭代
            for (final Map.Entry<String, NamespaceBundleStats> entry : localData.getLastStats().entrySet()) {
                //这里是一个 bundle ，并且对应的状态
                final String bundle = entry.getKey();
                final NamespaceBundleStats stats = entry.getValue();
                //如果只有一个topic，那继续看下一个bundle
                if (stats.topics == 1) {
                    log.info("namespace bundle {} only have 1 topic", bundle);
                    continue;
                }
                double totalMessageRate = 0;
                double totalMessageThroughput = 0;
                // 读取 bundleData 数据，测试是否包含 bundle，如果有，则获取长周期数据，否则忽略
                // Attempt to consider long-term message data, otherwise effectively ignore.
                if (loadData.getBundleData().containsKey(bundle)) {
                    final TimeAverageMessageData longTermData = loadData.getBundleData().get(bundle).getLongTermData();
                    totalMessageRate = longTermData.totalMsgRate();
                    totalMessageThroughput = longTermData.totalMsgThroughput();
                }
                // 比较是否超过配置中定义的阈值，只要任意一个超过阈值，则将可能被拆分
                if (stats.topics > maxBundleTopics || stats.consumerCount + stats.producerCount > maxBundleSessions
                        || totalMessageRate > maxBundleMsgRate || totalMessageThroughput > maxBundleBandwidth) {
                    final String namespace = LoadManagerShared.getNamespaceNameFromBundleName(bundle);
                    try {//读取当前 namespaceName 被分割数
                        final int bundleCount = pulsar.getNamespaceService()
                                .getBundleCount(NamespaceName.get(namespace));
                        // 如果分割数小于阈值，则可以尝试分割，否则给出警告信息
                        if (bundleCount < maxBundleCount) {
                            bundleCache.add(bundle);
                        } else {
                            log.warn(
                                    "Could not split namespace bundle {} because namespace {} has too many bundles: {}",
                                    bundle, namespace, bundleCount);
                        }
                    } catch (Exception e) {
                        log.warn("Error while getting bundle count for namespace {}", namespace, e);
                    }
                }
            }
        });
        return bundleCache;
    }
}

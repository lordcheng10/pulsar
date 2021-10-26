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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.Map;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TimeAverageMessageData;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.LoadSheddingStrategy;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load shedding strategy which will attempt to shed exactly one bundle on brokers which are overloaded, that is, whose
 * maximum system resource usage exceeds loadBalancerBrokerOverloadedThresholdPercentage. A bundle will be recommended
 * for unloading off that broker if and only if the following conditions hold: The broker has at least two bundles
 * assigned and the broker has at least one bundle that has not been unloaded recently according to
 * LoadBalancerSheddingGracePeriodMinutes. The unloaded bundle will be the most expensive bundle in terms of message
 * rate that has not been recently unloaded.
 */
public class OverloadShedder implements LoadSheddingStrategy {

    private static final Logger log = LoggerFactory.getLogger(OverloadShedder.class);

    private final Multimap<String, String> selectedBundlesCache = ArrayListMultimap.create();

    private static final double ADDITIONAL_THRESHOLD_PERCENT_MARGIN = 0.05;

    /**
     * Attempt to shed some bundles off every broker which is overloaded.
     *
     * @param loadData
     *            The load data to used to make the unloading decision.
     * @param conf
     *            The service configuration.
     * @return A map from bundles to unload to the brokers on which they are loaded.
     */
    @Override
    public Multimap<String, String> findBundlesForUnloading(final LoadData loadData, final ServiceConfiguration conf) {
        //清理上一次使用缓存
        selectedBundlesCache.clear();
        //资源超载比率阈值
        final double overloadThreshold = conf.getLoadBalancerBrokerOverloadedThresholdPercentage() / 100.0;
        //获取最近未卸载的 namespaceBundles
        final Map<String, Long> recentlyUnloadedBundles = loadData.getRecentlyUnloadedBundles();

        // 检查当前集群里每一个 broker，并选出使用资源已超载的
        // Check every broker and select
        loadData.getBrokerData().forEach((broker, brokerData) -> {

            final LocalBrokerData localData = brokerData.getLocalData();
            //获取 broker 上最大资源使用率
            final double currentUsage = localData.getMaxResourceUsage();
            //没有超载，直接返回，继续下一个 broker
            if (currentUsage < overloadThreshold) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Broker is not overloaded, ignoring at this point ({})", broker,
                            localData.printResourceUsage());
                }
                return;
            }

            //调整后的负载设置 = broker 当前负载比率 - 超载阈值比率 + 宽限步进比率（主要用于调整后的负载比不至于在超载阈值比率附近，这样很有可能频繁触发减载操作，通俗的将就是有个安全缓存区域）
            // We want to offload enough traffic such that this broker will go below the overload threshold
            // Also, add a small margin so that this broker won't be very close to the threshold edge.
            double percentOfTrafficToOffload = currentUsage - overloadThreshold + ADDITIONAL_THRESHOLD_PERCENT_MARGIN;
            // broker 当前消息吞吐量 = 消息扇入量 + 消息扇出量
            double brokerCurrentThroughput = localData.getMsgThroughputIn() + localData.getMsgThroughputOut();
            //最小的消息吞吐量超载量（简单的说，broker 超载时经过负载均衡后，目标要减载到这个值）
            double minimumThroughputToOffload = brokerCurrentThroughput * percentOfTrafficToOffload;

            log.info(
                    "Attempting to shed load on {}, which has resource usage {}% above threshold {}%"
                            + " -- Offloading at least {} MByte/s of traffic ({})",
                    broker, 100 * currentUsage, 100 * overloadThreshold, minimumThroughputToOffload / 1024 / 1024,
                    localData.printResourceUsage());

            MutableDouble trafficMarkedToOffload = new MutableDouble(0);
            MutableBoolean atLeastOneBundleSelected = new MutableBoolean(false);

            //namespaceBundle 数量
            if (localData.getBundles().size() > 1) {
                // 按照吞吐量为 namespaceBundle 排序
                // Sort bundles by throughput, then pick the biggest N which combined
                // make up for at least the minimum throughput to offload
                loadData.getBundleData().entrySet().stream()
                        .filter(e -> localData.getBundles().contains(e.getKey()))
                        .map((e) -> {
                            // 这里返回一个以key为 namespaceBundle，以value为 namespaceBundle 短期采样吞吐量数据的 Map，以减少系统资源负载
                            // Map to throughput value
                            // Consider short-term byte rate to address system resource burden
                            String bundle = e.getKey();
                            BundleData bundleData = e.getValue();
                            TimeAverageMessageData shortTermData = bundleData.getShortTermData();
                            double throughput = shortTermData.getMsgThroughputIn() + shortTermData
                                    .getMsgThroughputOut();
                    return Pair.of(bundle, throughput);
                }).filter(e -> {
                    // 仅考虑最近尚未卸载的 nameBundle
                    // Only consider bundles that were not already unloaded recently
                    return !recentlyUnloadedBundles.containsKey(e.getLeft());
                }).filter(e ->
                        localData.getBundles().contains(e.getLeft())
                ).sorted((e1, e2) -> {
                    // 按照吞吐量降序排序（简单说就是，吞吐量大的排前面）
                    // Sort by throughput in reverse order
                    return Double.compare(e2.getRight(), e1.getRight());
                }).forEach(e -> {
                    //这里有二层意思，对于一 broker 来说，其所有的 namespaceBundle 上对应的吞吐量累加后小于 minimumThroughputToOffload 值的，都会被选进入 selectedBundlesCache 中，
                    // 其二，第一个 namespaceBundle 都大于等于 minimumThroughputToOffload 值，就直接选入，也就是只要进入这层循环，
                    // 都会选至少一个 namespaceBundle 出来（简单的说就是选负载之和超过minimumThroughputToOffload的 namespaceBundle集合）
                    if (trafficMarkedToOffload.doubleValue() < minimumThroughputToOffload
                            || atLeastOneBundleSelected.isFalse()) {
                       selectedBundlesCache.put(broker, e.getLeft());
                       trafficMarkedToOffload.add(e.getRight());
                       atLeastOneBundleSelected.setTrue();
                   }
                });
            } else if (localData.getBundles().size() == 1) {
                //broker 只有一个 namespaceBundle 情况，只打印警告信息
                log.warn(
                        "HIGH USAGE WARNING : Sole namespace bundle {} is overloading broker {}. "
                                + "No Load Shedding will be done on this broker",
                        localData.getBundles().iterator().next(), broker);
            } else {
                //broker 还没有一个 namespaceBundle 情况，只打印警告信息
                log.warn("Broker {} is overloaded despite having no bundles", broker);
            }

        });

        return selectedBundlesCache;
    }
}

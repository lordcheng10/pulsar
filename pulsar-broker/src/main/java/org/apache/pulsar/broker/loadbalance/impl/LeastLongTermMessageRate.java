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

import java.util.ArrayList;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.pulsar.broker.BrokerData;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TimeAverageBrokerData;
import org.apache.pulsar.broker.TimeAverageMessageData;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.ModularLoadManagerStrategy;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// 此方法位于 ModularLoadManagerStrategy 接口实现类 LeastLongTermMessageRate 中，
// 从类名就可以看出长期消息速率最小（策略），简单的选出长期内消息速率最小的 broker，来看看吧
/**
 * bundle放置策略，根据哪个代理具有最低的长期消息速率来选择代理。
 * */
/**
 * Placement strategy which selects a broker based on which one has the least long term message rate.
 */
public class LeastLongTermMessageRate implements ModularLoadManagerStrategy {
    private static Logger log = LoggerFactory.getLogger(LeastLongTermMessageRate.class);

    // 维护此列表以减少对象创建。
    // Maintain this list to reduce object creation.
    private ArrayList<String> bestBrokers;
    //最低长期消息率
    //conf多余的
    public LeastLongTermMessageRate(final ServiceConfiguration conf) {
        bestBrokers = new ArrayList<>();
    }

    //通过 broker 的负载数据以及系统配置来计分
    // 使用其预先分配的捆绑数据和时间平均数据为经纪人形成分数。
    // 这是通过将所有预先分配的长期消息速率相加并将它们添加到代理的整体长期消息速率中来完成的，它本身就是每个分配包的长期消息速率的总和。
    // 任何处于（或高于）过载阈值的代理的得分为 POSITIVE_INFINITY。
    // Form a score for a broker using its preallocated bundle data and time average data.
    // This is done by summing all preallocated long-term message rates and adding them to the broker's overall
    // long-term message rate, which is itself the sum of the long-term message rate of every allocated bundle.
    // Any broker at (or above) the overload threshold will have a score of POSITIVE_INFINITY.
    private static double getScore(final BrokerData brokerData, final ServiceConfiguration conf) {
        // 过载阈值
        final double overloadThreshold = conf.getLoadBalancerBrokerOverloadedThresholdPercentage() / 100.0;
        // 最大资源使用率
        final double maxUsage = brokerData.getLocalData().getMaxResourceUsage();
        // 超过阈值
        if (maxUsage > overloadThreshold) {
            log.warn("Broker {} is overloaded: max usage={}", brokerData.getLocalData().getWebServiceUrl(), maxUsage);
            return Double.POSITIVE_INFINITY;
        }

        // 计算一个broker的预分配bundle消息速率总和,这个只能在leader里面才有
        //这里的getPreallocatedBundleData是什么
        //注意这里是算的预分配(还未分配上去的bundle消息速率)
        double totalMessageRate = 0;
        for (BundleData bundleData : brokerData.getPreallocatedBundleData().values()) {
            final TimeAverageMessageData longTermData = bundleData.getLongTermData();
            totalMessageRate += longTermData.getMsgRateIn() + longTermData.getMsgRateOut();
        }

        //这里算的是实际一段时间的消息速率
        // calculate estimated score
        final TimeAverageBrokerData timeAverageData = brokerData.getTimeAverageData();
        final double timeAverageLongTermMessageRate = timeAverageData.getLongTermMsgRateIn()
                + timeAverageData.getLongTermMsgRateOut();
        //这两部分相加才是预分配完成后，该broker的消息速率,最后以这来进行打分
        final double totalMessageRateEstimate = totalMessageRate + timeAverageLongTermMessageRate;

        if (log.isDebugEnabled()) {
            log.debug("Broker {} has long term message rate {}",
                    brokerData.getLocalData().getWebServiceUrl(), totalMessageRateEstimate);
        }
        return totalMessageRateEstimate;
    }

    /**
     * Find a suitable broker to assign the given bundle to.
     *
     * @param candidates
     *            The candidates for which the bundle may be assigned.
     * @param bundleToAssign
     *            The data for the bundle to assign.
     * @param loadData
     *            The load data from the leader broker.
     * @param conf
     *            The service configuration.
     * @return The name of the selected broker as it appears on ZooKeeper.
     */
    @Override
    public Optional<String> selectBroker(final Set<String> candidates, final BundleData bundleToAssign,
                                         final LoadData loadData,
                                         final ServiceConfiguration conf) {
        //给各个broker打分，选出打分最低的broker
        bestBrokers.clear();
        double minScore = Double.POSITIVE_INFINITY;
        // Maintain of list of all the best scoring brokers and then randomly
        // select one of them at the end.
        for (String broker : candidates) {
            final BrokerData brokerData = loadData.getBrokerData().get(broker);
            //针对每一个候选broker打分
            final double score = getScore(brokerData, conf);
            if (score == Double.POSITIVE_INFINITY) {//如果等于无穷大，那么就打印下日志
                final LocalBrokerData localData = brokerData.getLocalData();
                log.warn(
                        "Broker {} is overloaded: CPU: {}%, MEMORY: {}%, DIRECT MEMORY: {}%, BANDWIDTH IN: {}%, "
                                + "BANDWIDTH OUT: {}%",
                        broker, localData.getCpu().percentUsage(), localData.getMemory().percentUsage(),
                        localData.getDirectMemory().percentUsage(), localData.getBandwidthIn().percentUsage(),
                        localData.getBandwidthOut().percentUsage());

            }
            //打分如果小于最小分数，那么就清空bestBrokers并将当前打分最低的broker重新添加到bestBrokers
            if (score < minScore) {
                // Clear best brokers since this score beats the other brokers.
                bestBrokers.clear();
                bestBrokers.add(broker);
                minScore = score;
            } else if (score == minScore) {
                // Add this broker to best brokers since it ties with the best score.
                bestBrokers.add(broker);
            }
        }


        if (bestBrokers.isEmpty()) {
            // 所有代理都过载了。
            // 在这种情况下随机分配。
            // All brokers are overloaded.
            // Assign randomly in this case.
            bestBrokers.addAll(candidates);
        }

        if (bestBrokers.isEmpty()) {
            //当返回空时候，服务端会给客户端返回一个异常报错：newLookupErrorResponse(ServerError.ServiceNotReady,
            //                                        "No broker was available to own " + topicName, requestId)
            //走到这里肯定经过了bestBrokers.addAll(candidates);，说明candidates就是空
            // If still, it means there are no available brokers at this point
            return Optional.empty();
        }
        //从bestBrokers中随机选一个
        return Optional.of(bestBrokers.get(ThreadLocalRandom.current().nextInt(bestBrokers.size())));
    }
}

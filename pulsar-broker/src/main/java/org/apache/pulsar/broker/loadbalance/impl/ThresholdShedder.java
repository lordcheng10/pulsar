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
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.BrokerData;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TimeAverageMessageData;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.LoadSheddingStrategy;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThresholdShedder implements LoadSheddingStrategy {
    private static final Logger log = LoggerFactory.getLogger(ThresholdShedder.class);

    //Multimap就类似<key,List<value>>
    //selectedBundlesCache存放了要卸载的bundle缓存
    private final Multimap<String, String> selectedBundlesCache = ArrayListMultimap.create();
    //额外加一个卸载比例，防止卸载的得太少
    private static final double ADDITIONAL_THRESHOLD_PERCENT_MARGIN = 0.05;

    private static final double MB = 1024 * 1024;

    //broker平均资源使用率,key是brokerIp:Port,这个map记录了每一个broker对应的资源使用率，该资源使用率是通过权重来计算出来的，并不是平均值，这里名字有歧义
    //变量名也不对，有歧义
    //这个变量的目的是为了记录历史负载，方便在计算的时候算上
    private final Map<String, Double> brokerAvgResourceUsage = new HashMap<>();

    @Override
    public Multimap<String, String> findBundlesForUnloading(final LoadData loadData, final ServiceConfiguration conf) {
        //清理缓存
        selectedBundlesCache.clear();
        //把配置的过载百分比转换成小数
        final double threshold = conf.getLoadBalancerBrokerThresholdShedderPercentage() / 100.0;
        //最近卸载过的bundle
        final Map<String, Long> recentlyUnloadedBundles = loadData.getRecentlyUnloadedBundles();
        //最少卸载的流量
        final double minThroughputThreshold = conf.getLoadBalancerBundleUnloadMinThroughputThreshold() * MB;

        //计算出所有broker系统资源的平均使用率
        final double avgUsage = getBrokerAvgUsage(loadData, conf.getLoadBalancerHistoryResourcePercentage(), conf);

        //如果平均使用率是0，那么就直接返回空cache
        if (avgUsage == 0) {
            log.warn("average max resource usage is 0");
            return selectedBundlesCache;
        }

        //遍历每个broker的系统负载
        loadData.getBrokerData().forEach((broker, brokerData) -> {
            final LocalBrokerData localData = brokerData.getLocalData();
            //获取当前负载
            final double currentUsage = brokerAvgResourceUsage.getOrDefault(broker, 0.0);
            //判断是否小于阈值
            if (currentUsage < avgUsage + threshold) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] broker is not overloaded, ignoring at this point", broker);
                }
                return;
            }

            double percentOfTrafficToOffload =
                    currentUsage - avgUsage - threshold + ADDITIONAL_THRESHOLD_PERCENT_MARGIN;
            double brokerCurrentThroughput = localData.getMsgThroughputIn() + localData.getMsgThroughputOut();
            double minimumThroughputToOffload = brokerCurrentThroughput * percentOfTrafficToOffload;

            if (minimumThroughputToOffload < minThroughputThreshold) {
                if (log.isDebugEnabled()) {
                    log.info("[{}] broker is planning to shed throughput {} MByte/s less than "
                                    + "minimumThroughputThreshold {} MByte/s, skipping bundle unload.",
                            broker, minimumThroughputToOffload / MB, minThroughputThreshold / MB);
                }
                return;
            }

            log.info(
                    "Attempting to shed load on {}, which has max resource usage above avgUsage  and threshold {}%"
                            + " > {}% + {}% -- Offloading at least {} MByte/s of traffic, left throughput {} MByte/s",
                    broker, currentUsage, avgUsage, threshold, minimumThroughputToOffload / MB,
                    (brokerCurrentThroughput - minimumThroughputToOffload) / MB);

            //流量标记为卸载
            MutableDouble trafficMarkedToOffload = new MutableDouble(0);
            //至少一个捆绑已选择
            MutableBoolean atLeastOneBundleSelected = new MutableBoolean(false);

            if (localData.getBundles().size() > 1) {//看该broker上的bundle数是否超过1,只有超过1才有卸载的必要
                loadData.getBundleData().entrySet().stream().map((e) -> {
                    String bundle = e.getKey();
                    BundleData bundleData = e.getValue();
                    TimeAverageMessageData shortTermData = bundleData.getShortTermData();
                    double throughput = shortTermData.getMsgThroughputIn() + shortTermData.getMsgThroughputOut();
                    return Pair.of(bundle, throughput);
                }).filter(e ->
                        !recentlyUnloadedBundles.containsKey(e.getLeft())
                ).filter(e ->
                        //这个判断是多余的吧
                        localData.getBundles().contains(e.getLeft())
                ).sorted((e1, e2) ->
                        //推理后面逻辑，这里应该是从小到大排序，不然后面放入就可能放入不止一个bundle了
                        //但实际确实从大到小排序
                        Double.compare(e2.getRight(), e1.getRight())
                ).forEach(e -> {
                    //当前卸载流量没有超过计算要卸载的值或者至少卸载一个bundle
                    if (trafficMarkedToOffload.doubleValue() < minimumThroughputToOffload
                            || atLeastOneBundleSelected.isFalse()) {
                        //放入cache中
                        selectedBundlesCache.put(broker, e.getLeft());
                        //卸载流量递增
                        trafficMarkedToOffload.add(e.getRight());
                        //设置标记
                        atLeastOneBundleSelected.setTrue();
                    }
                });
            } else if (localData.getBundles().size() == 1) {
                log.warn(
                        "HIGH USAGE WARNING : Sole namespace bundle {} is overloading broker {}. "
                                + "No Load Shadding will be done on this broker",
                        localData.getBundles().iterator().next(), broker);
            } else {
                log.warn("Broker {} is overloaded despit having no bundles", broker);
            }
        });

        return selectedBundlesCache;
    }

    //计算出所有broker系统资源的平均使用率和整体的平均负载
    private double getBrokerAvgUsage(final LoadData loadData, final double historyPercentage,
                                     final ServiceConfiguration conf) {
        double totalUsage = 0.0;
        int totalBrokers = 0;

        for (Map.Entry<String, BrokerData> entry : loadData.getBrokerData().entrySet()) {
            LocalBrokerData localBrokerData = entry.getValue().getLocalData();
            String broker = entry.getKey();
            //计算该broker的负载，然后放入brokerAvgResourceUsage中
            updateAvgResourceUsage(broker, localBrokerData, historyPercentage, conf);
            //从brokerAvgResourceUsage中获取负载，然后进行累计总负载
            totalUsage += brokerAvgResourceUsage.getOrDefault(broker, 0.0);
            totalBrokers++;
        }

        //最后计算出平均值
        return totalBrokers > 0 ? totalUsage / totalBrokers : 0;
    }

    //TODO-chenlin:方法名字就不对，有歧义
    private void updateAvgResourceUsage(String broker, LocalBrokerData localBrokerData, final double historyPercentage,
                                        final ServiceConfiguration conf) {
        double historyUsage =
                brokerAvgResourceUsage.getOrDefault(broker, 0.0);
        historyUsage = historyUsage * historyPercentage
                + (1 - historyPercentage) * localBrokerData.getMaxResourceUsageWithWeight(
                conf.getLoadBalancerCPUResourceWeight(),
                conf.getLoadBalancerMemoryResourceWeight(), conf.getLoadBalancerDirectMemoryResourceWeight(),
                conf.getLoadBalancerBandwithInResourceWeight(),
                conf.getLoadBalancerBandwithOutResourceWeight());
        brokerAvgResourceUsage.put(broker, historyUsage);
    }

}

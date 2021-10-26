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
package org.apache.pulsar.broker.loadbalance;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.broker.BrokerData;
import org.apache.pulsar.broker.BundleData;

//LoadData 实际上是包含所有的 broker 、 namespaceBundle 的性能采样数据。由此可以猜测，负载均衡器就是通过它通过设定要的资源阈值来执行负载均衡的。
//LoadData 实际上包含了每个 broker 、 namespaceBundle 的负载数据，还有最近被卸载的 namespaceBundle 数据集。
/**
 * This class represents all data that could be relevant when making a load management decision.
 */
public class LoadData {
    // broker name ---> brokerData 的映射
    /**
     * Map from broker names to their available data.
     */
    private final Map<String, BrokerData> brokerData;
    // bundle names ----> bundle 时间敏感的聚合数据的映射
    /**
     * Map from bundle names to their time-sensitive aggregated data.
     */
    private final Map<String, BundleData> bundleData;

    //最近卸载的 namespace ---> namespaceBundle 映射
    /**
     * Map from recently unloaded bundles to the timestamp of when they were last loaded.
     */
    private final Map<String, Long> recentlyUnloadedBundles;

    /**
     * Initialize a LoadData.
     */
    public LoadData() {
        this.brokerData = new ConcurrentHashMap<>();
        this.bundleData = new ConcurrentHashMap<>();
        this.recentlyUnloadedBundles = new ConcurrentHashMap<>();
    }

    public Map<String, BrokerData> getBrokerData() {
        return brokerData;
    }

    public Map<String, BundleData> getBundleData() {
        return bundleData;
    }

    public Map<String, Long> getRecentlyUnloadedBundles() {
        return recentlyUnloadedBundles;
    }
}

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

import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;

//负载管理器界面的新提议，它尝试使用更直观的方法名称并为新的负载管理器提议提供一个起点。
//相对老得接口LoadManager,名字起得更好
//Modular Load Manager 模块化负载管理器
/**
 * New proposal for a load manager interface which attempts to use more intuitive method names and provide a starting place for new load manager proposals.
 */
public interface ModularLoadManager {

    //与任何代理一样，禁用此管理器正在运行的代理
    //看起来是要在zk上写一个标记
    /**
     * As any broker, disable the broker this manager is running on.
     *
     * @throws PulsarServerException
     *             If ZooKeeper failed to disable the broker.
     */
    void disableBroker() throws PulsarServerException;

    //作为领导代理，选择要卸载的命名空间服务的包，以便将它们重新分配给新的代理
    /**
     * As the leader broker, select bundles for the namespace service to unload so that they may be reassigned to new
     * brokers.
     */
    void doLoadShedding();

    //作为领导者代理，尝试自动检测和拆分热命名空间包。
    /**
     * As the leader broker, attempt to automatically detect and split hot namespace bundles.
     */
    void checkNamespaceBundleSplit();

    //使用给定的脉冲星服务初始化此负载管理器。
    /**
     * Initialize this load manager using the given pulsar service.
     */
    void initialize(PulsarService pulsar);

    //作为leader broker，找到一个合适的broker来分配给定的bundle。
    /**
     * As the leader broker, find a suitable broker for the assignment of the given bundle.
     *
     * @param serviceUnit
     *            ServiceUnitId for the bundle.
     * @return The name of the selected broker, as it appears on ZooKeeper.
     */
    Optional<String> selectBrokerForAssignment(ServiceUnitId serviceUnit);

    //与任何代理一样，启动负载管理器。
    /**
     * As any broker, start the load manager.
     *
     * @throws PulsarServerException
     *             If an unexpected error prevented the load manager from being started.
     */
    void start() throws PulsarServerException;

    //与任何代理一样，停止负载管理器。
    /**
     * As any broker, stop the load manager.
     *
     * @throws PulsarServerException
     *             If an unexpected error occurred when attempting to stop the load manager.
     */
    void stop() throws PulsarServerException;

    //作为任何代理，检索名称空间包统计信息和系统资源使用情况以更新此代理的本地数据。
    /**
     * As any broker, retrieve the namespace bundle stats and system resource usage to update data local to this broker.
     */
    LocalBrokerData updateLocalBrokerData();

    //作为任何代理，将本地代理数据写入 ZooKeeper。
    /**
     * As any broker, write the local broker data to ZooKeeper.
     */
    void writeBrokerDataOnZooKeeper();

    //作为任何代理，将本地代理数据写入 ZooKeeper，无论是否强制。
    /**
     * As any broker, write the local broker data to ZooKeeper, forced or not.
     */
    default void writeBrokerDataOnZooKeeper(boolean force) {
        writeBrokerDataOnZooKeeper();
    }

    //作为leader broker，将所有broker聚合的bundle数据写入ZooKeeper。
    /**
     * As the leader broker, write bundle data aggregated from all brokers to ZooKeeper.
     */
    void writeBundleDataOnZooKeeper();

    //获取集群中可用的代理列表。
    /**
     * Get available broker list in cluster.
     *
     * @return
     */
    Set<String> getAvailableBrokers();

    //从负载管理器代理缓存中获取本地代理数据。
    /**
     * Fetch local-broker data from load-manager broker cache.
     *
     * @param broker load-balancer path
     * @return
     */
    LocalBrokerData getBrokerLocalData(String broker);

    //获取负载平衡指标
    /**
     * Fetch load balancing metrics.
     *
     * @return List of LoadBalancing Metrics
     */
    List<Metrics> getLoadBalancingMetrics();
}

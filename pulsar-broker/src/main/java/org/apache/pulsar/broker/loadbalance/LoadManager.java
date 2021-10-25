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
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerWrapper;
import org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LoadManager runs though set of load reports collected from different brokers and generates a recommendation of
 * namespace/ServiceUnit placement on machines/ResourceUnit. Each Concrete Load Manager will use different algorithms to
 * generate this mapping.
 *
 * Concrete Load Manager is also return the least loaded broker that should own the new namespace.
 */
public interface LoadManager {
    Logger LOG = LoggerFactory.getLogger(LoadManager.class);

    String LOADBALANCE_BROKERS_ROOT = "/loadbalance/brokers";

    void start() throws PulsarServerException;

    //是分配新捆绑的集中决策。
    /**
     * Is centralized decision making to assign a new bundle.
     */
    boolean isCentralized();

    // 返回由某些特定于实现的算法或标准决定的最小加载资源单元
    /**
     * Returns the Least Loaded Resource Unit decided by some algorithm or criteria which is implementation specific.
     */
    Optional<ResourceUnit> getLeastLoaded(ServiceUnitId su) throws Exception;

    // 生成负载报告
    /**
     * Generate the load report.
     */
    LoadManagerReport generateLoadReport() throws Exception;

    //设置标志以强制加载报告更新。
    /**
     * Set flag to force load report update.
     */
    void setLoadReportForceUpdateFlag();

    //把负载报告写到zk
    /**
     * Publish the current load report on ZK.
     */
    void writeLoadReportOnZookeeper() throws Exception;

    //是否强制把负载报告写到zk
    /**
     * Publish the current load report on ZK, forced or not.
     * By default rely on method writeLoadReportOnZookeeper().
     */
    default void writeLoadReportOnZookeeper(boolean force) throws Exception {
        writeLoadReportOnZookeeper();
    }

    //把分配的资源quota写到zk
    /**
     * Update namespace bundle resource quota on ZK.
     */
    void writeResourceQuotasToZooKeeper() throws Exception;

    //生成负载metric
    /**
     * Generate load balancing stats metrics.
     */
    List<Metrics> getLoadBalancingMetrics();

    //执行负载均衡操作（当一 broker 负载过高，强行将部分负载分配到负载较低的 broker 上）
    /**
     * Unload a candidate service unit to balance the load.
     */
    void doLoadShedding();

    //命名空间split
    /**
     * Namespace bundle split.
     */
    void doNamespaceBundleSplit() throws Exception;

    //从负载均衡器列表中删除当前代理的可见性，因此其他代理无法将任何请求重定向到此代理，并且此代理不会接受新的连接请求。
    /**
     * Removes visibility of current broker from loadbalancer list so, other brokers can't redirect any request to this
     * broker and this broker won't accept new connection requests.
     *
     * @throws Exception
     */
    void disableBroker() throws Exception;

    //获取可获得的broker列表
    /**
     * Get list of available brokers in cluster.
     *
     * @return
     * @throws Exception
     */
    Set<String> getAvailableBrokers() throws Exception;

    //停掉负载均衡管理器
    void stop() throws PulsarServerException;

    /**
     * Initialize this LoadManager.
     *
     * @param pulsar
     *            The service to initialize this with.
     */
    void initialize(PulsarService pulsar);

    //创建负载管理器:通过反射的方式来加载pulsar broker配置
    static LoadManager create(final PulsarService pulsar) {
        try {
            final ServiceConfiguration conf = pulsar.getConfiguration();
            final Class<?> loadManagerClass = Class.forName(conf.getLoadManagerClassName());
            // Assume there is a constructor with one argument of PulsarService.
            final Object loadManagerInstance = loadManagerClass.getDeclaredConstructor().newInstance();
            if (loadManagerInstance instanceof LoadManager) {
                final LoadManager casted = (LoadManager) loadManagerInstance;
                casted.initialize(pulsar);
                return casted;
            } else if (loadManagerInstance instanceof ModularLoadManager) {
                final LoadManager casted = new ModularLoadManagerWrapper((ModularLoadManager) loadManagerInstance);
                casted.initialize(pulsar);
                return casted;
            }
        } catch (Exception e) {
            LOG.warn("Error when trying to create load manager: ", e);
        }
        // If we failed to create a load manager, default to SimpleLoadManagerImpl.
        return new SimpleLoadManagerImpl(pulsar);
    }

}

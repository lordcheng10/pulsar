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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.pulsar.broker.BrokerData;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.TimeAverageBrokerData;
import org.apache.pulsar.broker.TimeAverageMessageData;
import org.apache.pulsar.broker.loadbalance.BrokerFilter;
import org.apache.pulsar.broker.loadbalance.BrokerFilterException;
import org.apache.pulsar.broker.loadbalance.BrokerHostUsage;
import org.apache.pulsar.broker.loadbalance.BundleSplitStrategy;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.LoadSheddingStrategy;
import org.apache.pulsar.broker.loadbalance.ModularLoadManager;
import org.apache.pulsar.broker.loadbalance.ModularLoadManagerStrategy;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared.BrokerTopicLoadingPredicate;
import org.apache.pulsar.broker.resources.ClusterResources;
import org.apache.pulsar.broker.stats.prometheus.metrics.Summary;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.policies.data.FailureDomainImpl;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.common.policies.data.ResourceQuota;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.coordination.LockManager;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 从以上可以看出，负载均衡器的职责非常清晰，其核心工作如下：
 *
 * 更新集群中所有 broker 负载数据
 * 更新集群中所有 broker 上 NamespaceBundle 集合负载数据
 * 基于以上负载数据与设定的阈值判断，是否有出现超载的 namespaceBundle ，如果有，则发送拆分 namespaceBundle 命令给 broker，执行拆分任务。
 * */
//默认使用的负载均衡器
public class ModularLoadManagerImpl implements ModularLoadManager, Consumer<Notification> {
    private static final Logger log = LoggerFactory.getLogger(ModularLoadManagerImpl.class);

    //ZNode 的路径，其子节点包含每个bundle的 BundleData json（ResourceQuota 的新 API 版本）:记录该bundle下有多少topic、以及该bundle的发送速率等
    // Path to ZNode whose children contain BundleData jsons for each bundle (new API version of ResourceQuota).
    public static final String BUNDLE_DATA_PATH = "/loadbalance/bundle-data";

    //为看不见的bundle假定的默认消息速率(msg/s)。
    // Default message rate to assume for unseen bundles.
    public static final double DEFAULT_MESSAGE_RATE = 50;

    //为看不见的bundle假定的默认吞吐量(bytes/s)。
    //请注意，默认消息大小被隐式定义为 DEFAULT_MESSAGE_THROUGHPUT / DEFAULT_MESSAGE_RATE。
    // Default message throughput to assume for unseen bundles.
    // Note that the default message size is implicitly defined as DEFAULT_MESSAGE_THROUGHPUT / DEFAULT_MESSAGE_RATE.
    public static final double DEFAULT_MESSAGE_THROUGHPUT = 50000;

    //为观察长期数据而保留的有效样本数。
    // The number of effective samples to keep for observing long term data.
    public static final int NUM_LONG_SAMPLES = 1000;

    //为观察短期数据而保留的有效样本数。
    // The number of effective samples to keep for observing short term data.
    public static final int NUM_SHORT_SAMPLES = 10;

    //其子节点包含 ResourceQuota json 的 ZNode 的路径。
    //bookie_ss_share/loadbalance/resource-quota/default命名空间下的quota配置默认是有设置的：看起来default命名空间的quota，写入速率只有40mgs/s
    //{"msgRateIn":40.0,"msgRateOut":120.0,"bandwidthIn":100000.0,"bandwidthOut":300000.0,"memory":80.0,"dynamic":true}
    // Path to ZNode whose children contain ResourceQuota jsons.
    public static final String RESOURCE_QUOTA_ZPATH = "/loadbalance/resource-quota/namespace";

    //包含每个代理的 TimeAverageBrokerData json 的 ZNode 的路径。
    //看起来该目录下记录了各个节点在一段时间内的平均数据：比如平均写入速率,eg:
    //{"shortTermMsgThroughputIn":4.709897361740486E7,"shortTermMsgThroughputOut":155282.16141480498,"shortTermMsgRateIn":45382.07492227784,"shortTermMsgRateOut":149.79520981173818,
    // "longTermMsgThroughputIn":5.2876187912063405E7,"longTermMsgThroughputOut":8399724.720383286,"longTermMsgRateIn":50947.373817837266,"longTermMsgRateOut":8093.283957073706}
    // Path to ZNode containing TimeAverageBrokerData jsons for each broker.
    public static final String TIME_AVERAGE_BROKER_ZPATH = "/loadbalance/broker-time-average";

    //要重用的broker候选集合，以避免创建对象。
    // Set of broker candidates to reuse so that object creation is avoided.
    private final Set<String> brokerCandidateCache;

    //本地代理数据的缓存，存储在 LoadManager.LOADBALANCE_BROKERs_ROOT 中。
    // Cache of the local broker data, stored in LoadManager.LOADBALANCE_BROKERS_ROOT.
    private LockManager<LocalBrokerData> brokersData;
    private ResourceLock<LocalBrokerData> brokerDataLock;
    //各个bundle中的数据
    private MetadataCache<BundleData> bundlesCache;
    private MetadataCache<ResourceQuota> resourceQuotaCache;
    private MetadataCache<TimeAverageBrokerData> timeAverageBrokerDataCache;

    //计算broker系统资源的使用率
    // Broker host usage object used to calculate system resource usage.
    private BrokerHostUsage brokerHostUsage;

    //从代理到命名空间映射到分配给该代理的命名空间中的包范围。
    //用于在一个命名空间内跨代理均匀分布包
    // Map from brokers to namespaces to the bundle ranges in that namespace assigned to that broker.
    // Used to distribute bundles within a namespace evenly across brokers.
    private final ConcurrentOpenHashMap<String, ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<String>>>
            brokerToNamespaceToBundleRange;

    //包含此代理的 LocalBrokerData json 的 ZNode 的路径。
    // Path to the ZNode containing the LocalBrokerData json for this broker.
    private String brokerZnodePath;

    //用于拆分捆绑包的策略。
    // Strategy to use for splitting bundles.
    private BundleSplitStrategy bundleSplitStrategy;
    //属于脉冲星服务的服务配置。
    // Service configuration belonging to the pulsar service.
    private ServiceConfiguration conf;

    // 用于初始化历史数据的默认包统计信息。
    // 在 bundle 收到它的第一个样本后，这个数据被覆盖。
    // The default bundle stats which are used to initialize historic data.
    // This data is overridden after the bundle receives its first sample.
    private final NamespaceBundleStats defaultStats;

    // 用于过滤经纪人不被选中进行分配。
    // Used to filter brokers from being selected for assignment.
    private final List<BrokerFilter> filterPipeline;
    // 上次调用 updateBundleData 的时间戳。
    // Timestamp of last invocation of updateBundleData.
    private long lastBundleDataUpdate;
    // LocalBrokerData 在最近更新之前可用。
    // LocalBrokerData available before most recent update.
    private LocalBrokerData lastData;

    //用于确定应该卸载哪些命名空间（如果有）的管道。
    // Pipeline used to determine what namespaces, if any, should be unloaded.
    private final List<LoadSheddingStrategy> loadSheddingPipeline;

    //正在运行的代理的本地数据。
    // Local data for the broker this is running on.
    private LocalBrokerData localData;

    //加载包含每个代理可用数据的数据。
    // Load data comprising data available for each broker.
    private final LoadData loadData;
    //用于确定捆绑包是否已预先分配。
    // Used to determine whether a bundle is preallocated.
    private final Map<String, String> preallocatedBundleToBroker;
    //用于确定新主题放置位置的策略。
    // Strategy used to determine where new topics should be placed.
    private ModularLoadManagerStrategy placementStrategy;
    //用于确定哪些代理可用于特定命名空间的策略。
    // Policies used to determine which brokers are available for particular namespaces.
    private SimpleResourceAllocationPolicies policies;
    //Pulsar 服务用于初始化它。
    // Pulsar service used to initialize this.
    private PulsarService pulsar;
    //Executor 服务用于定期更新代理数据。
    // Executor service used to regularly update broker data.
    private final ScheduledExecutorService scheduler;
    //检查给定的代理是否可以加载持久/非持久主题
    // check if given broker can load persistent/non-persistent topic
    private final BrokerTopicLoadingPredicate brokerTopicLoadingPredicate;
    //代理故障域映射
    private Map<String, String> brokerToFailureDomainMap;

    //记录负载平衡指标
    // record load balancing metrics
    private AtomicReference<List<Metrics>> loadBalancingMetrics = new AtomicReference<>();
    //记录捆绑卸载指标
    // record bundle unload metrics
    private AtomicReference<List<Metrics>> bundleUnloadMetrics = new AtomicReference<>();
    //记录捆绑拆分指标
    // record bundle split metrics
    private AtomicReference<List<Metrics>> bundleSplitMetrics = new AtomicReference<>();

    private long bundleSplitCount = 0;
    private long unloadBrokerCount = 0;
    private long unloadBundleCount = 0;

    private final Lock lock = new ReentrantLock();

    /**
     * Initializes fields which do not depend on PulsarService. initialize(PulsarService) should subsequently be called.
     */
    public ModularLoadManagerImpl() {
        brokerCandidateCache = new HashSet<>();
        brokerToNamespaceToBundleRange = new ConcurrentOpenHashMap<>();
        defaultStats = new NamespaceBundleStats();
        filterPipeline = new ArrayList<>();
        loadData = new LoadData();
        loadSheddingPipeline = new ArrayList<>();
        preallocatedBundleToBroker = new ConcurrentHashMap<>();
        //单线程进行数据上报
        scheduler = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("pulsar-modular-load-manager"));
        this.brokerToFailureDomainMap = Maps.newHashMap();

        this.brokerTopicLoadingPredicate = new BrokerTopicLoadingPredicate() {
            @Override
            public boolean isEnablePersistentTopics(String brokerUrl) {
                final BrokerData brokerData = loadData.getBrokerData().get(brokerUrl.replace("http://", ""));
                return brokerData != null && brokerData.getLocalData() != null
                        && brokerData.getLocalData().isPersistentTopicsEnabled();
            }

            @Override
            public boolean isEnableNonPersistentTopics(String brokerUrl) {
                final BrokerData brokerData = loadData.getBrokerData().get(brokerUrl.replace("http://", ""));
                return brokerData != null && brokerData.getLocalData() != null
                        && brokerData.getLocalData().isNonPersistentTopicsEnabled();
            }
        };
    }

    /**
     * Initialize this load manager using the given PulsarService. Should be called only once, after invoking the
     * default constructor.
     *
     * @param pulsar The service to initialize with.
     */
    @Override
    public void initialize(final PulsarService pulsar) {
        this.pulsar = pulsar;
        brokersData = pulsar.getCoordinationService().getLockManager(LocalBrokerData.class);
        bundlesCache = pulsar.getLocalMetadataStore().getMetadataCache(BundleData.class);
        resourceQuotaCache = pulsar.getLocalMetadataStore().getMetadataCache(ResourceQuota.class);
        timeAverageBrokerDataCache = pulsar.getLocalMetadataStore().getMetadataCache(TimeAverageBrokerData.class);
        //在这里就注册了监听器，监听器就是该类，因为它实现了 Consumer<Notification>接口,在这个接口的accept方法中会进行回调处理
        pulsar.getLocalMetadataStore().registerListener(this);

        //3. 判断是 *nix系还是win系操作系统，根据不同的操作系统环境来计算主机资源数据服务，服务定时来更新主机资源负载数据，以便使用时直接读取数据，下文将会提到使用场景
        if (SystemUtils.IS_OS_LINUX) {
            brokerHostUsage = new LinuxBrokerHostUsageImpl(pulsar);
        } else {
            brokerHostUsage = new GenericBrokerHostUsageImpl(pulsar);
        }

        //4. 创建 bundle 分割任务
        bundleSplitStrategy = new BundleSplitterTask();

        conf = pulsar.getConfiguration();

        // Initialize the default stats to assume for unseen bundles (hard-coded for now).
        defaultStats.msgThroughputIn = DEFAULT_MESSAGE_THROUGHPUT;
        defaultStats.msgThroughputOut = DEFAULT_MESSAGE_THROUGHPUT;
        defaultStats.msgRateIn = DEFAULT_MESSAGE_RATE;
        defaultStats.msgRateOut = DEFAULT_MESSAGE_RATE;
        //6. 创建唯一的 LeastLongTermMessageRate 实现
        placementStrategy = ModularLoadManagerStrategy.create(conf);
        //7. 创建简单资源分配策略服务
        policies = new SimpleResourceAllocationPolicies(pulsar);
        // 新增 broker 版本过滤器
        filterPipeline.add(new BrokerVersionFilter());
        //8. 刷新各个 broker 分配到不同的故障域
        refreshBrokerToFailureDomainMap();
        //9. 注册故障域中主机变更事件，一旦变更，则执行 refreshBrokerToFailureDomainMap 方法
        // register listeners for domain changes
        pulsar.getPulsarResources().getClusterResources().getFailureDomainResources()
                .registerListener(__ -> {
                    scheduler.execute(() -> refreshBrokerToFailureDomainMap());
                });


        //负载超载时，执行动作策略
        loadSheddingPipeline.add(createLoadSheddingStrategy());
    }

    //当/loadbalance/brokers目录下变更时，就会触发更新，也就是说上报的负载数据，是通过监听回调的方式来更新到leader的缓存中的
    @Override
    public void accept(Notification t) {
        //1. 监听 zookeeper 上 /loadbalance/brokers 的路径，并注册事件回调
        // 主要用于监听 broker 增加或减少事件，以便执行如下操作
        if (t.getPath().startsWith(LoadManager.LOADBALANCE_BROKERS_ROOT)) {
            brokersData.listLocks(LoadManager.LOADBALANCE_BROKERS_ROOT)
                    .thenAccept(brokers -> {
                        // 1. 回收已经停机或宕机的 broker 的任何预分配资源
                        reapDeadBrokerPreallocations(brokers);
                    });

            try {
                // 2. 提交一个任务重新计算负载（updateAll方法下节将详细分析）
                scheduler.submit(ModularLoadManagerImpl.this::updateAll);
            } catch (RejectedExecutionException e) {
                // Executor is shutting down
            }
        }
    }

    private LoadSheddingStrategy createLoadSheddingStrategy() {
        try {
            Class<?> loadSheddingClass = Class.forName(conf.getLoadBalancerLoadSheddingStrategy());
            Object loadSheddingInstance = loadSheddingClass.getDeclaredConstructor().newInstance();
            if (loadSheddingInstance instanceof LoadSheddingStrategy) {
                return (LoadSheddingStrategy) loadSheddingInstance;
            } else {
                log.error("create load shedding strategy failed. using OverloadShedder instead.");
                return new OverloadShedder();
            }
        } catch (Exception e) {
            log.error("Error when trying to create load shedding strategy: ", e);
        }

        return new OverloadShedder();
    }

    /**
     * Initialize this load manager.
     *
     * @param pulsar
     *            Client to construct this manager from.
     */
    public ModularLoadManagerImpl(final PulsarService pulsar) {
        this();
        initialize(pulsar);
    }

    //当 broker 存活数变更时，将会回调此方法，回收预分配资源
    // For each broker that we have a recent load report, see if they are still alive
    private void reapDeadBrokerPreallocations(List<String> aliveBrokers) {
        for (String broker : loadData.getBrokerData().keySet()) {
            //表明 broker 已停机或宕机（当然也可能是broker与zookeeper通讯中断）
            if (!aliveBrokers.contains(broker)) {
                if (log.isDebugEnabled()) {
                    log.debug("Broker {} appears to have stopped; now reclaiming any preallocations", broker);
                }
                final Iterator<Map.Entry<String, String>> iterator = preallocatedBundleToBroker.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, String> entry = iterator.next();
                    final String preallocatedBundle = entry.getKey();
                    final String preallocatedBroker = entry.getValue();
                    if (broker.equals(preallocatedBroker)) {
                        if (log.isDebugEnabled()) {
                            log.debug("Removing old preallocation on dead broker {} for bundle {}",
                                    preallocatedBroker, preallocatedBundle);
                        }
                        //移除在已经宕机 broker 上老预分配资源
                        iterator.remove();
                    }
                }
            }
        }
    }

    @Override
    public Set<String> getAvailableBrokers() {
        try {
            return new HashSet<>(brokersData.listLocks(LoadManager.LOADBALANCE_BROKERS_ROOT).join());
        } catch (Exception e) {
            log.warn("Error when trying to get active brokers", e);
            return loadData.getBrokerData().keySet();
        }
    }

    // Attempt to local the data for the given bundle in metadata store
    // If it cannot be found, return the default bundle data.
    private BundleData getBundleDataOrDefault(final String bundle) {
        BundleData bundleData = null;
        try {
            Optional<BundleData> optBundleData = bundlesCache.get(getBundleDataPath(bundle)).join();
            if (optBundleData.isPresent()) {
                return optBundleData.get();
            }

            Optional<ResourceQuota> optQuota = resourceQuotaCache
                    .get(String.format("%s/%s", RESOURCE_QUOTA_ZPATH, bundle)).join();
            if (optQuota.isPresent()) {
                ResourceQuota quota = optQuota.get();
                bundleData = new BundleData(NUM_SHORT_SAMPLES, NUM_LONG_SAMPLES);
                // Initialize from existing resource quotas if new API ZNodes do not exist.
                final TimeAverageMessageData shortTermData = bundleData.getShortTermData();
                final TimeAverageMessageData longTermData = bundleData.getLongTermData();

                shortTermData.setMsgRateIn(quota.getMsgRateIn());
                shortTermData.setMsgRateOut(quota.getMsgRateOut());
                shortTermData.setMsgThroughputIn(quota.getBandwidthIn());
                shortTermData.setMsgThroughputOut(quota.getBandwidthOut());

                longTermData.setMsgRateIn(quota.getMsgRateIn());
                longTermData.setMsgRateOut(quota.getMsgRateOut());
                longTermData.setMsgThroughputIn(quota.getBandwidthIn());
                longTermData.setMsgThroughputOut(quota.getBandwidthOut());

                // Assume ample history.
                shortTermData.setNumSamples(NUM_SHORT_SAMPLES);
                longTermData.setNumSamples(NUM_LONG_SAMPLES);
            }
        } catch (Exception e) {
            log.warn("Error when trying to find bundle {} on metadata store: {}", bundle, e);
        }
        if (bundleData == null) {
            bundleData = new BundleData(NUM_SHORT_SAMPLES, NUM_LONG_SAMPLES, defaultStats);
        }
        return bundleData;
    }

    // Get the metadata store path for the given bundle full name.
    private static String getBundleDataPath(final String bundle) {
        return BUNDLE_DATA_PATH + "/" + bundle;
    }

    // Use the Pulsar client to acquire the namespace bundle stats.
    private Map<String, NamespaceBundleStats> getBundleStats() {
        return pulsar.getBrokerService().getBundleStats();
    }

    private double percentChange(final double oldValue, final double newValue) {
        if (oldValue == 0) {
            if (newValue == 0) {
                // Avoid NaN
                return 0;
            }
            return Double.POSITIVE_INFINITY;
        }
        return 100 * Math.abs((oldValue - newValue) / oldValue);
    }

    // Determine if the broker data requires an update by delegating to the update condition.
    private boolean needBrokerDataUpdate() {
        final long updateMaxIntervalMillis = TimeUnit.MINUTES
                .toMillis(conf.getLoadBalancerReportUpdateMaxIntervalMinutes());
        long timeSinceLastReportWrittenToStore = System.currentTimeMillis() - localData.getLastUpdate();
        if (timeSinceLastReportWrittenToStore > updateMaxIntervalMillis) {
            log.info("Writing local data to metadata store because time since last"
                            + " update exceeded threshold of {} minutes",
                    conf.getLoadBalancerReportUpdateMaxIntervalMinutes());
            // Always update after surpassing the maximum interval.
            return true;
        }
        final double maxChange = Math
                .max(100.0 * (Math.abs(lastData.getMaxResourceUsage() - localData.getMaxResourceUsage())),
                        Math.max(percentChange(lastData.getMsgRateIn() + lastData.getMsgRateOut(),
                                localData.getMsgRateIn() + localData.getMsgRateOut()),
                                Math.max(
                                        percentChange(lastData.getMsgThroughputIn() + lastData.getMsgThroughputOut(),
                                                localData.getMsgThroughputIn() + localData.getMsgThroughputOut()),
                                        percentChange(lastData.getNumBundles(), localData.getNumBundles()))));
        if (maxChange > conf.getLoadBalancerReportUpdateThresholdPercentage()) {
            log.info("Writing local data to metadata store because maximum change {}% exceeded threshold {}%; "
                            + "time since last report written is {} seconds", maxChange,
                    conf.getLoadBalancerReportUpdateThresholdPercentage(),
                    timeSinceLastReportWrittenToStore / 1000.0);
            return true;
        }
        return false;
    }

    //更新 broker 和 NamespaceBundle 负载数据
    // Update both the broker data and the bundle data.
    public void updateAll() {
        if (log.isDebugEnabled()) {
            log.debug("Updating broker and bundle data for loadreport");
        }
        //这个方法实际就是更新loadData中的brokerData
        updateAllBrokerData();
        //这个方法实际就是更新loadData中的bundleData
        updateBundleData();
        //作为领导者代理，尝试自动检测和拆分热命名空间包。
        //此时broker有最新的负载报告：检查是否有任何包需要拆分
        // broker has latest load-report: check if any bundle requires split
        checkNamespaceBundleSplit();
    }

    // 作为领导者（leader） broker，需要通过从 zookeeper 上查询所有的 broker 负载数据，
    // 而这负载数据是每个 broker 通过 updateLocalBrokerData 方法定时上报给 zookeeper 的，
    // 然后更新到 brokerDataMap 中
    // 作为领导者（leader） broker，需要通过从 zookeeper 上查询所有的 broker 负载数据，
    // 而这负载数据是每个 broker 通过 updateLocalBrokerData 方法定时上报给 zookeeper 的，然后更新到 brokerDataMap 中
    // As the leader broker, update the broker data map in loadData by querying metadata store for the broker data put
    // there by each broker via updateLocalBrokerData.
    private void updateAllBrokerData() {
        //从 zookeeper 上获取当前所有存活的 broker
        final Set<String> activeBrokers = getAvailableBrokers();
        //这里实际就在更新loadData里的数据了,这个方法周期性调度
        final Map<String, BrokerData> brokerDataMap = loadData.getBrokerData();
        for (String broker : activeBrokers) {
            try {
                String key = String.format("%s/%s", LoadManager.LOADBALANCE_BROKERS_ROOT, broker);
                //从 zookeeper 缓存brokersData中读取（每个） broker 上负载数据（LocalBrokerData），如果没有 key(这个key实际是一个zk路径)，则从缓存brokerDataMap中移除该broker数据，然后跳过
                Optional<LocalBrokerData> localData = brokersData.readLock(key).get();
                if (!localData.isPresent()) {
                    // 仅当在使用最新数据刷新 availableBrokerCache 之前更新 update-brokerData 并且 broker 的delete-znode监视事件未更新 availableBrokerCache 时，
                    // 才会发生（简单的说，当前已经获取到availableBrokerCache数据，
                    // 但是此时刚好有个 broker 挂掉，这时候，availableBrokerCache数据已经过期，然后在从 brokerDataCache 中获取缓存数据，可能就出现此种情况）
                    brokerDataMap.remove(broker);
                    log.info("[{}] Broker load report is not present", broker);
                    continue;
                }
                //是否包含此 broker 节点负载数据
                if (brokerDataMap.containsKey(broker)) {
                    // 如果包含，则替换最新的负载数据
                    // Replace previous local broker data.
                    brokerDataMap.get(broker).setLocalData(localData.get());
                } else {
                    // 否则，用当前数据创建新的 brokerData
                    // Initialize BrokerData object for previously unseen
                    // brokers.
                    brokerDataMap.put(broker, new BrokerData(localData.get()));
                }
            } catch (Exception e) {
                log.warn("Error reading broker data from cache for broker - [{}], [{}]", broker, e.getMessage());
            }
        }
        // Remove obsolete brokers.
        for (final String broker : brokerDataMap.keySet()) {
            if (!activeBrokers.contains(broker)) {
                brokerDataMap.remove(broker);
            }
        }
    }

    //这里是更新 NamespaceBundle 负载数据
    // As the leader broker, use the local broker data saved on metadata store to update the bundle stats so that better
    // load management decisions may be made.
    private void updateBundleData() {
        final Map<String, BundleData> bundleData = loadData.getBundleData();
        // 迭代 broker 负载数据
        // Iterate over the broker data.
        for (Map.Entry<String, BrokerData> brokerEntry : loadData.getBrokerData().entrySet()) {
            final String broker = brokerEntry.getKey();
            final BrokerData brokerData = brokerEntry.getValue();
            //从最新更新的 broker 负载中读取 NamespaceBundle 负载数据
            //所以上面upDateAll方法里面调用的顺序不能变，得先调用updateAllBrokerData，不然这里就用不到最新的数据了
            final Map<String, NamespaceBundleStats> statsMap = brokerData.getLocalData().getLastStats();

            // 迭代当前最新可用的 bundle 状态数据更新到 bundleData 中（注：这里bundle指的是NamespaceBundle的全限定名，下同）
            // Iterate over the last bundle stats available to the current
            // broker to update the bundle data.
            for (Map.Entry<String, NamespaceBundleStats> entry : statsMap.entrySet()) {
                final String bundle = entry.getKey();
                final NamespaceBundleStats stats = entry.getValue();
                if (bundleData.containsKey(bundle)) {
                    // 如果原 bundleData 已有此 bundle，则直接更新新的采样数据
                    // If we recognize the bundle, add these stats as a new sample.
                    bundleData.get(bundle).update(stats);
                } else {
                    //否则，尝试从 zookeeper 中读取此 bundle 的采样数据，如果没有找到，这返回默认采样数据，并且用最新的采样数据更新。
                    // Otherwise, attempt to find the bundle data on metadata store.
                    // If it cannot be found, use the latest stats as the first sample.
                    BundleData currentBundleData = getBundleDataOrDefault(bundle);
                    currentBundleData.update(stats);
                    bundleData.put(bundle, currentBundleData);
                }
            }

            // 从 预分配的 bundleData 中移除所有已加载的 bundle
            // Remove all loaded bundles from the preallocated maps.
            final Map<String, BundleData> preallocatedBundleData = brokerData.getPreallocatedBundleData();
            synchronized (preallocatedBundleData) {
                for (String preallocatedBundleName : brokerData.getPreallocatedBundleData().keySet()) {
                    if (brokerData.getLocalData().getBundles().contains(preallocatedBundleName)) {
                        final Iterator<Map.Entry<String, BundleData>> preallocatedIterator =
                                preallocatedBundleData.entrySet()
                                        .iterator();
                        while (preallocatedIterator.hasNext()) {
                            final String bundle = preallocatedIterator.next().getKey();

                            if (bundleData.containsKey(bundle)) {
                                preallocatedIterator.remove();
                                preallocatedBundleToBroker.remove(bundle);
                            }
                        }
                    }

                    // This is needed too in case a broker which was assigned a bundle dies and comes back up.
                    preallocatedBundleToBroker.remove(preallocatedBundleName);
                }
            }

            // Using the newest data, update the aggregated time-average data for the current broker.
            brokerData.getTimeAverageData().reset(statsMap.keySet(), bundleData, defaultStats);
            final ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<String>> namespaceToBundleRange =
                    brokerToNamespaceToBundleRange
                            .computeIfAbsent(broker, k -> new ConcurrentOpenHashMap<>());
            synchronized (namespaceToBundleRange) {
                namespaceToBundleRange.clear();
                LoadManagerShared.fillNamespaceToBundlesMap(statsMap.keySet(), namespaceToBundleRange);
                LoadManagerShared.fillNamespaceToBundlesMap(preallocatedBundleData.keySet(), namespaceToBundleRange);
            }
        }
    }

    /**
     * As any broker, disable the broker this manager is running on.
     *
     * @throws PulsarServerException
     *             If there's a failure when disabling broker on metadata store.
     */
    @Override
    public void disableBroker() throws PulsarServerException {
        if (StringUtils.isNotEmpty(brokerZnodePath)) {
            try {
                brokerDataLock.release().join();
            } catch (CompletionException e) {
                if (e.getCause() instanceof NotFoundException) {
                    throw new PulsarServerException.NotFoundException(MetadataStoreException.unwrap(e));
                } else {
                    throw new PulsarServerException(MetadataStoreException.unwrap(e));
                }
            }
        }
    }

    //作为领导代理，选择要卸载的命名空间服务包，以便将它们重新分配给新代理。
    /**
     * As the leader broker, select bundles for the namespace service to unload so that they may be reassigned to new
     * brokers.
     */
    @Override
    public synchronized void doLoadShedding() {
        //是否启用自动namespaceBundle分配服务
        if (!LoadManagerShared.isLoadSheddingEnabled(pulsar)) {
            return;
        }
        //如果 broker 个数少于等于1，则直接返回
        if (getAvailableBrokers().size() <= 1) {
            log.info("Only 1 broker available: no load shedding will be performed");
            return;
        }
        // 移除近来已在卸载过处于 GracePeriod 时间的 namespaceBundle（简单的说是，上一次卸载还没超过GracePeriod 时间，保护期不再进行二次卸载的 NamespaceBundle）
        // Remove bundles who have been unloaded for longer than the grace period from the recently unloaded map.
        final long timeout = System.currentTimeMillis()
                - TimeUnit.MINUTES.toMillis(conf.getLoadBalancerSheddingGracePeriodMinutes());
        final Map<String, Long> recentlyUnloadedBundles = loadData.getRecentlyUnloadedBundles();
        recentlyUnloadedBundles.keySet().removeIf(e -> recentlyUnloadedBundles.get(e) < timeout);

        for (LoadSheddingStrategy strategy : loadSheddingPipeline) {
            final Multimap<String, String> bundlesToUnload = strategy.findBundlesForUnloading(loadData, conf);

            bundlesToUnload.asMap().forEach((broker, bundles) -> {
                bundles.forEach(bundle -> {
                    final String namespaceName = LoadManagerShared.getNamespaceNameFromBundleName(bundle);
                    final String bundleRange = LoadManagerShared.getBundleRangeFromBundleName(bundle);
                    if (!shouldAntiAffinityNamespaceUnload(namespaceName, bundleRange, broker)) {
                        return;
                    }

                    log.info("[Overload shedder] Unloading bundle: {} from broker {}", bundle, broker);
                    try {
                        pulsar.getAdminClient().namespaces().unloadNamespaceBundle(namespaceName, bundleRange);
                        loadData.getRecentlyUnloadedBundles().put(bundle, System.currentTimeMillis());
                    } catch (PulsarServerException | PulsarAdminException e) {
                        log.warn("Error when trying to perform load shedding on {} for broker {}", bundle, broker, e);
                    }
                });
            });

            updateBundleUnloadingMetrics(bundlesToUnload);
        }
    }

    /**
     * As leader broker, update bundle unloading metrics.
     *
     * @param bundlesToUnload
     */
    private void updateBundleUnloadingMetrics(Multimap<String, String> bundlesToUnload) {
        unloadBrokerCount += bundlesToUnload.keySet().size();
        unloadBundleCount += bundlesToUnload.values().size();

        List<Metrics> metrics = Lists.newArrayList();
        Map<String, String> dimensions = new HashMap<>();

        dimensions.put("metric", "bundleUnloading");

        Metrics m = Metrics.create(dimensions);
        m.put("brk_lb_unload_broker_count", unloadBrokerCount);
        m.put("brk_lb_unload_bundle_count", unloadBundleCount);
        metrics.add(m);
        this.bundleUnloadMetrics.set(metrics);
    }

    public boolean shouldAntiAffinityNamespaceUnload(String namespace, String bundle, String currentBroker) {
        try {
            Optional<LocalPolicies> nsPolicies = pulsar.getPulsarResources().getLocalPolicies()
                    .getLocalPolicies(NamespaceName.get(namespace));
            if (!nsPolicies.isPresent() || StringUtils.isBlank(nsPolicies.get().namespaceAntiAffinityGroup)) {
                return true;
            }

            synchronized (brokerCandidateCache) {
                brokerCandidateCache.clear();
                ServiceUnitId serviceUnit = pulsar.getNamespaceService().getNamespaceBundleFactory()
                        .getBundle(namespace, bundle);
                LoadManagerShared.applyNamespacePolicies(serviceUnit, policies, brokerCandidateCache,
                        getAvailableBrokers(), brokerTopicLoadingPredicate);
                return LoadManagerShared.shouldAntiAffinityNamespaceUnload(namespace, bundle, currentBroker, pulsar,
                        brokerToNamespaceToBundleRange, brokerCandidateCache);
            }

        } catch (Exception e) {
            log.warn("Failed to check anti-affinity namespace ownership for {}/{}/{}, {}", namespace, bundle,
                    currentBroker, e.getMessage());

        }
        return true;
    }

    //作为领导者代理，尝试自动检测和拆分热命名空间包。
    /**
     * As the leader broker, attempt to automatically detect and split hot namespace bundles.
     */
    @Override
    public void checkNamespaceBundleSplit() {

        if (!conf.isLoadBalancerAutoBundleSplitEnabled() || pulsar.getLeaderElectionService() == null
                || !pulsar.getLeaderElectionService().isLeader()) {
            return;
        }
        final boolean unloadSplitBundles = pulsar.getConfiguration().isLoadBalancerAutoUnloadSplitBundlesEnabled();
        synchronized (bundleSplitStrategy) {
            //基于特定阈值来决定哪些bundle需要被拆分，返回需要拆分的 bundle 集
            final Set<String> bundlesToBeSplit = bundleSplitStrategy.findBundlesToSplit(loadData, pulsar);
            NamespaceBundleFactory namespaceBundleFactory = pulsar.getNamespaceService().getNamespaceBundleFactory();
            for (String bundleName : bundlesToBeSplit) {
                try {
                    final String namespaceName = LoadManagerShared.getNamespaceNameFromBundleName(bundleName);
                    final String bundleRange = LoadManagerShared.getBundleRangeFromBundleName(bundleName);
                    if (!namespaceBundleFactory
                            .canSplitBundle(namespaceBundleFactory.getBundle(namespaceName, bundleRange))) {
                        continue;
                    }

                    // Make sure the same bundle is not selected again.
                    loadData.getBundleData().remove(bundleName);
                    localData.getLastStats().remove(bundleName);
                    // Clear namespace bundle-cache
                    this.pulsar.getNamespaceService().getNamespaceBundleFactory()
                            .invalidateBundleCache(NamespaceName.get(namespaceName));
                    deleteBundleDataFromMetadataStore(bundleName);

                    //从这里可以看出，只是发一个namespaceBundle拆分请求给 broker，其请求URL为:http(s)://brokerip:port/{tenant}/{namespace}/{bundle}/split
                    log.info("Load-manager splitting bundle {} and unloading {}", bundleName, unloadSplitBundles);
                    pulsar.getAdminClient().namespaces().splitNamespaceBundle(namespaceName, bundleRange,
                        unloadSplitBundles, null);

                    log.info("Successfully split namespace bundle {}", bundleName);
                } catch (Exception e) {
                    log.error("Failed to split namespace bundle {}", bundleName, e);
                }
            }

            updateBundleSplitMetrics(bundlesToBeSplit);
        }

    }

    /**
     * As leader broker, update bundle split metrics.
     *
     * @param bundlesToBeSplit
     */
    private void updateBundleSplitMetrics(Set<String> bundlesToBeSplit) {
        bundleSplitCount += bundlesToBeSplit.size();

        List<Metrics> metrics = Lists.newArrayList();
        Map<String, String> dimensions = new HashMap<>();

        dimensions.put("metric", "bundlesSplit");

        Metrics m = Metrics.create(dimensions);
        m.put("brk_lb_bundles_split_count", bundleSplitCount);
        metrics.add(m);
        this.bundleSplitMetrics.set(metrics);
    }

    private static final Summary selectBrokerForAssignment = Summary.build(
            "pulsar_broker_load_manager_bundle_assigment", "-")
            .quantile(0.50)
            .quantile(0.99)
            .quantile(0.999)
            .quantile(1.0)
            .register();

    /**
     * As the leader broker, find a suitable broker for the assignment of the given bundle.
     *
     * @param serviceUnit
     *            ServiceUnitId for the bundle.
     * @return The name of the selected broker, as it appears on metadata store.
     */
    @Override
    public Optional<String> selectBrokerForAssignment(final ServiceUnitId serviceUnit) {
        // Use brokerCandidateCache as a lock to reduce synchronization.
        long startTime = System.nanoTime();

        try {
            synchronized (brokerCandidateCache) {
                final String bundle = serviceUnit.toString();
                if (preallocatedBundleToBroker.containsKey(bundle)) {
                    // If the given bundle is already in preallocated, return the selected broker.
                    return Optional.of(preallocatedBundleToBroker.get(bundle));
                }
                final BundleData data = loadData.getBundleData().computeIfAbsent(bundle,
                        key -> getBundleDataOrDefault(bundle));
                brokerCandidateCache.clear();
                LoadManagerShared.applyNamespacePolicies(serviceUnit, policies, brokerCandidateCache,
                        getAvailableBrokers(),
                        brokerTopicLoadingPredicate);

                // filter brokers which owns topic higher than threshold
                LoadManagerShared.filterBrokersWithLargeTopicCount(brokerCandidateCache, loadData,
                        conf.getLoadBalancerBrokerMaxTopics());

                // distribute namespaces to domain and brokers according to anti-affinity-group
                LoadManagerShared.filterAntiAffinityGroupOwnedBrokers(pulsar, serviceUnit.toString(),
                        brokerCandidateCache,
                        brokerToNamespaceToBundleRange, brokerToFailureDomainMap);
                // distribute bundles evenly to candidate-brokers

                LoadManagerShared.removeMostServicingBrokersForNamespace(serviceUnit.toString(), brokerCandidateCache,
                        brokerToNamespaceToBundleRange);
                log.info("{} brokers being considered for assignment of {}", brokerCandidateCache.size(), bundle);

                // Use the filter pipeline to finalize broker candidates.
                try {
                    for (BrokerFilter filter : filterPipeline) {
                        filter.filter(brokerCandidateCache, data, loadData, conf);
                    }
                } catch (BrokerFilterException x) {
                    // restore the list of brokers to the full set
                    LoadManagerShared.applyNamespacePolicies(serviceUnit, policies, brokerCandidateCache,
                            getAvailableBrokers(),
                            brokerTopicLoadingPredicate);
                }

                if (brokerCandidateCache.isEmpty()) {
                    // restore the list of brokers to the full set
                    LoadManagerShared.applyNamespacePolicies(serviceUnit, policies, brokerCandidateCache,
                            getAvailableBrokers(),
                            brokerTopicLoadingPredicate);
                }

                // Choose a broker among the potentially smaller filtered list, when possible
                Optional<String> broker = placementStrategy.selectBroker(brokerCandidateCache, data, loadData, conf);
                if (log.isDebugEnabled()) {
                    log.debug("Selected broker {} from candidate brokers {}", broker, brokerCandidateCache);
                }

                if (!broker.isPresent()) {
                    // No brokers available
                    return broker;
                }

                final double overloadThreshold = conf.getLoadBalancerBrokerOverloadedThresholdPercentage() / 100.0;
                final double maxUsage = loadData.getBrokerData().get(broker.get()).getLocalData().getMaxResourceUsage();
                if (maxUsage > overloadThreshold) {
                    // All brokers that were in the filtered list were overloaded, so check if there is a better broker
                    LoadManagerShared.applyNamespacePolicies(serviceUnit, policies, brokerCandidateCache,
                            getAvailableBrokers(),
                            brokerTopicLoadingPredicate);
                    broker = placementStrategy.selectBroker(brokerCandidateCache, data, loadData, conf);
                }

                // Add new bundle to preallocated.
                loadData.getBrokerData().get(broker.get()).getPreallocatedBundleData().put(bundle, data);
                preallocatedBundleToBroker.put(bundle, broker.get());

                final String namespaceName = LoadManagerShared.getNamespaceNameFromBundleName(bundle);
                final String bundleRange = LoadManagerShared.getBundleRangeFromBundleName(bundle);
                final ConcurrentOpenHashMap<String, ConcurrentOpenHashSet<String>> namespaceToBundleRange =
                        brokerToNamespaceToBundleRange
                                .computeIfAbsent(broker.get(), k -> new ConcurrentOpenHashMap<>());
                synchronized (namespaceToBundleRange) {
                    namespaceToBundleRange.computeIfAbsent(namespaceName, k -> new ConcurrentOpenHashSet<>())
                            .add(bundleRange);
                }
                return broker;
            }
        } finally {
            selectBrokerForAssignment.observe(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        }
    }

    //与任何代理一样，启动负载管理器。
    /**
     * As any broker, start the load manager.
     *
     * @throws PulsarServerException
     *             If an unexpected error prevented the load manager from being started.
     */
    @Override
    public void start() throws PulsarServerException {
        try {
            // 此时，端口将更新为服务器分配的真实端口号
            // At this point, the ports will be updated with the real port number that the server was assigned
            Map<String, String> protocolData = pulsar.getProtocolDataToAdvertise();

            //初始化负载数据
            lastData = new LocalBrokerData(pulsar.getSafeWebServiceAddress(), pulsar.getWebServiceAddressTls(),
                    pulsar.getSafeBrokerServiceUrl(), pulsar.getBrokerServiceUrlTls(), pulsar.getAdvertisedListeners());
            lastData.setProtocols(protocolData);
            // configure broker-topic mode
            lastData.setPersistentTopicsEnabled(pulsar.getConfiguration().isEnablePersistentTopics());
            lastData.setNonPersistentTopicsEnabled(pulsar.getConfiguration().isEnableNonPersistentTopics());

            localData = new LocalBrokerData(pulsar.getSafeWebServiceAddress(), pulsar.getWebServiceAddressTls(),
                    pulsar.getSafeBrokerServiceUrl(), pulsar.getBrokerServiceUrlTls(), pulsar.getAdvertisedListeners());
            localData.setProtocols(protocolData);
            localData.setBrokerVersionString(pulsar.getBrokerVersion());
            // configure broker-topic mode
            localData.setPersistentTopicsEnabled(pulsar.getConfiguration().isEnablePersistentTopics());
            localData.setNonPersistentTopicsEnabled(pulsar.getConfiguration().isEnableNonPersistentTopics());

            //获取lookup地址（这个也就是暴露给客户端使用的地址）
            String lookupServiceAddress = pulsar.getAdvertisedAddress() + ":"
                    + (conf.getWebServicePort().isPresent() ? conf.getWebServicePort().get()
                            : conf.getWebServicePortTls().get());
            //broker的负载数据上报目录
            brokerZnodePath = LoadManager.LOADBALANCE_BROKERS_ROOT + "/" + lookupServiceAddress;
            final String timeAverageZPath = TIME_AVERAGE_BROKER_ZPATH + "/" + lookupServiceAddress;
            //更新该broker的本地负载数据缓存localData
            updateLocalBrokerData();
            //这里实际就是为了获取ResourceLockImpl对象，该对象可以操作zookeeper,创建的过程中也把zookeeper上的目录创建了，并在上面写了数据
            brokerDataLock = brokersData.acquireLock(brokerZnodePath, localData).join();
            //这里也是会创建zk目录
            timeAverageBrokerDataCache.readModifyUpdateOrCreate(timeAverageZPath,
                    __ -> new TimeAverageBrokerData()).join();
            //这里会更新下bundle数据和broker数据
            updateAll();
            //更新时间
            lastBundleDataUpdate = System.currentTimeMillis();
        } catch (Exception e) {
            log.error("Unable to acquire lock for broker: [{}]", brokerZnodePath, e);
            throw new PulsarServerException(e);
        }
    }

    /**
     * As any broker, stop the load manager.
     *
     * @throws PulsarServerException
     *             If an unexpected error occurred when attempting to stop the load manager.
     */
    @Override
    public void stop() throws PulsarServerException {
        scheduler.shutdownNow();

        try {
            brokersData.close();
        } catch (Exception e) {
            log.warn("Failed to release broker lock: {}", e.getMessage());
        }
    }

    //核心在于，更新当前 broker 所在主机以及 namespaceBundle 的负载数据
    //作为任何代理，检索名称空间包统计信息和系统资源使用情况以更新此代理的本地数据。
    /**
     * As any broker, retrieve the namespace bundle stats and system resource usage to update data local to this broker.
     * @return
     */
    @Override
    public LocalBrokerData updateLocalBrokerData() {
        lock.lock();
        try {
            //获取系统负载
            final SystemResourceUsage systemResourceUsage = LoadManagerShared.getSystemResourceUsage(brokerHostUsage);
            //更新本地的系统负载和bundle数据
            localData.update(systemResourceUsage, getBundleStats());
            //更新对应的metric
            updateLoadBalancingMetrics(systemResourceUsage);
        } catch (Exception e) {
            log.warn("Error when attempting to update local broker data", e);
            if (e instanceof ConcurrentModificationException) {
                throw (ConcurrentModificationException) e;
            }
        } finally {
            lock.unlock();
        }
        return localData;
    }

    /**
     * As any broker, update System Resource Usage Percentage.
     *
     * @param systemResourceUsage
     */
    private void updateLoadBalancingMetrics(final SystemResourceUsage systemResourceUsage) {
        List<Metrics> metrics = Lists.newArrayList();
        Map<String, String> dimensions = new HashMap<>();

        dimensions.put("broker", ServiceConfigurationUtils.getAppliedAdvertisedAddress(conf, true));
        dimensions.put("metric", "loadBalancing");

        Metrics m = Metrics.create(dimensions);
        m.put("brk_lb_cpu_usage", systemResourceUsage.getCpu().percentUsage());
        m.put("brk_lb_memory_usage", systemResourceUsage.getMemory().percentUsage());
        m.put("brk_lb_directMemory_usage", systemResourceUsage.getDirectMemory().percentUsage());
        m.put("brk_lb_bandwidth_in_usage", systemResourceUsage.getBandwidthIn().percentUsage());
        m.put("brk_lb_bandwidth_out_usage", systemResourceUsage.getBandwidthOut().percentUsage());
        metrics.add(m);
        this.loadBalancingMetrics.set(metrics);
    }

    /**
     * As any broker, write the local broker data to metadata store.
     */
    @Override
    public void writeBrokerDataOnZooKeeper() {
        writeBrokerDataOnZooKeeper(false);
    }

    @Override
    public void writeBrokerDataOnZooKeeper(boolean force) {
        lock.lock();
        try {
            updateLocalBrokerData();
            if (needBrokerDataUpdate() || force) {
                localData.setLastUpdate(System.currentTimeMillis());
                //写到zk上
                brokerDataLock.updateValue(localData).join();

                // Clear deltas.
                localData.cleanDeltas();

                // Update previous data.
                lastData.update(localData);
            }
        } catch (Exception e) {
            log.warn("Error writing broker data on metadata store", e);
            if (e instanceof ConcurrentModificationException) {
                throw (ConcurrentModificationException) e;
            }
        } finally {
            lock.unlock();
        }
    }

    //作为领导代理，将从所有代理聚合的包数据写入元数据存储。
    /**
     * As the leader broker, write bundle data aggregated from all brokers to metadata store.
     */
    @Override
    public void writeBundleDataOnZooKeeper() {
        updateBundleData();
        // Write the bundle data to metadata store.
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (Map.Entry<String, BundleData> entry : loadData.getBundleData().entrySet()) {
            final String bundle = entry.getKey();
            final BundleData data = entry.getValue();
            futures.add(bundlesCache.readModifyUpdateOrCreate(getBundleDataPath(bundle), __ -> data)
                    .thenApply(__ -> null));
        }

        // Write the time average broker data to metadata store.
        for (Map.Entry<String, BrokerData> entry : loadData.getBrokerData().entrySet()) {
            final String broker = entry.getKey();
            final TimeAverageBrokerData data = entry.getValue().getTimeAverageData();
            futures.add(timeAverageBrokerDataCache.readModifyUpdateOrCreate(
                    TIME_AVERAGE_BROKER_ZPATH + "/" + broker, __ -> data)
                    .thenApply(__ -> null));
        }

        try {
            FutureUtil.waitForAll(futures).join();
        } catch (Exception e) {
            log.warn("Error when writing metadata data to store", e);
        }
    }

    private void deleteBundleDataFromMetadataStore(String bundle) {
        try {
            bundlesCache.delete(getBundleDataPath(bundle)).join();
        } catch (Exception e) {
            if (!(e.getCause() instanceof NotFoundException)) {
                log.warn("Failed to delete bundle-data {} from metadata store", bundle, e);
            }
        }
    }

    private void refreshBrokerToFailureDomainMap() {
        if (!pulsar.getConfiguration().isFailureDomainsEnabled()) {
            return;
        }
        ClusterResources.FailureDomainResources fdr =
                pulsar.getPulsarResources().getClusterResources().getFailureDomainResources();
        String clusterName = pulsar.getConfiguration().getClusterName();
        try {
            synchronized (brokerToFailureDomainMap) {
                Map<String, String> tempBrokerToFailureDomainMap = Maps.newHashMap();
                for (String domainName : fdr.listFailureDomains(clusterName)) {
                    try {
                        Optional<FailureDomainImpl> domain = fdr.getFailureDomain(clusterName, domainName);
                        if (domain.isPresent()) {
                            for (String broker : domain.get().brokers) {
                                tempBrokerToFailureDomainMap.put(broker, domainName);
                            }
                        }
                    } catch (Exception e) {
                        log.warn("Failed to get domain {}", domainName, e);
                    }
                }
                this.brokerToFailureDomainMap = tempBrokerToFailureDomainMap;
            }
            log.info("Cluster domain refreshed {}", brokerToFailureDomainMap);
        } catch (Exception e) {
            log.warn("Failed to get domain-list for cluster {}", e.getMessage());
        }
    }

    @Override
    public LocalBrokerData getBrokerLocalData(String broker) {
        String key = String.format("%s/%s", LoadManager.LOADBALANCE_BROKERS_ROOT, broker);
        try {
            return brokersData.readLock(key).join().orElse(null);
        } catch (Exception e) {
            log.warn("Failed to get local-broker data for {}", broker, e);
            return null;
        }
    }

    @Override
    public List<Metrics> getLoadBalancingMetrics() {
        List<Metrics> metricsCollection = new ArrayList<>();

        if (this.loadBalancingMetrics.get() != null) {
            metricsCollection.addAll(this.loadBalancingMetrics.get());
        }

        if (this.bundleUnloadMetrics.get() != null) {
            metricsCollection.addAll(this.bundleUnloadMetrics.get());
        }

        if (this.bundleSplitMetrics.get() != null) {
            metricsCollection.addAll(this.bundleSplitMetrics.get());
        }

        return metricsCollection;
    }
}

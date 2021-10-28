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
package org.apache.pulsar.broker.namespace;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import io.netty.channel.EventLoopGroup;
import io.prometheus.client.Counter;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LeaderBroker;
import org.apache.pulsar.broker.loadbalance.LeaderElectionService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.ResourceUnit;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.stats.prometheus.metrics.Summary;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.lookup.data.LookupData;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundleSplitAlgorithm;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.NamespaceIsolationPolicy;
import org.apache.pulsar.common.policies.data.BrokerAssignment;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.common.policies.data.NamespaceOwnershipStatus;
import org.apache.pulsar.common.policies.impl.NamespaceIsolationPolicies;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <code>NamespaceService</code> provides resource ownership lookup as well as resource ownership claiming services
 * for the <code>PulsarService</code>.
 * <p/>
 * The <code>PulsarService</code> relies on this service for resource ownership operations.
 * <p/>
 * The focus of this phase is to bring up the system and be able to iterate and improve the services effectively.
 * <p/>
 *
 * @see org.apache.pulsar.broker.PulsarService
 */
public class NamespaceService implements AutoCloseable {

    public enum AddressType {
        BROKER_URL, LOOKUP_URL
    }

    private static final Logger LOG = LoggerFactory.getLogger(NamespaceService.class);

    private final ServiceConfiguration config;

    private final MetadataCache<LocalPolicies> localPoliciesCache;

    private final AtomicReference<LoadManager> loadManager;

    private final PulsarService pulsar;

    private final OwnershipCache ownershipCache;
    private final MetadataCache<LocalBrokerData> localBrokerDataCache;

    private final NamespaceBundleFactory bundleFactory;

    private int uncountedNamespaces;

    private final String host;

    private static final int BUNDLE_SPLIT_RETRY_LIMIT = 7;

    public static final String SLA_NAMESPACE_PROPERTY = "sla-monitor";
    public static final Pattern HEARTBEAT_NAMESPACE_PATTERN = Pattern.compile("pulsar/[^/]+/([^:]+:\\d+)");
    public static final Pattern HEARTBEAT_NAMESPACE_PATTERN_V2 = Pattern.compile("pulsar/([^:]+:\\d+)");
    public static final Pattern SLA_NAMESPACE_PATTERN = Pattern.compile(SLA_NAMESPACE_PROPERTY + "/[^/]+/([^:]+:\\d+)");
    public static final String HEARTBEAT_NAMESPACE_FMT = "pulsar/%s/%s:%s";
    public static final String HEARTBEAT_NAMESPACE_FMT_V2 = "pulsar/%s:%s";
    public static final String SLA_NAMESPACE_FMT = SLA_NAMESPACE_PROPERTY + "/%s/%s:%s";

    private final ConcurrentOpenHashMap<ClusterDataImpl, PulsarClientImpl> namespaceClients;

    private final List<NamespaceBundleOwnershipListener> bundleOwnershipListeners;


    private static final Counter lookupRedirects = Counter.build("pulsar_broker_lookup_redirects", "-").register();
    private static final Counter lookupFailures = Counter.build("pulsar_broker_lookup_failures", "-").register();
    private static final Counter lookupAnswers = Counter.build("pulsar_broker_lookup_answers", "-").register();

    private static final Summary lookupLatency = Summary.build("pulsar_broker_lookup", "-")
            .quantile(0.50)
            .quantile(0.99)
            .quantile(0.999)
            .quantile(1.0)
            .register();


    /**
     * Default constructor.
     *
     * @throws PulsarServerException
     */
    public NamespaceService(PulsarService pulsar) {
        this.pulsar = pulsar;
        host = pulsar.getAdvertisedAddress();
        this.config = pulsar.getConfiguration();
        this.loadManager = pulsar.getLoadManager();
        this.bundleFactory = new NamespaceBundleFactory(pulsar, Hashing.crc32());
        this.ownershipCache = new OwnershipCache(pulsar, bundleFactory, this);
        this.namespaceClients = new ConcurrentOpenHashMap<>();
        this.bundleOwnershipListeners = new CopyOnWriteArrayList<>();
        this.localBrokerDataCache = pulsar.getLocalMetadataStore().getMetadataCache(LocalBrokerData.class);
        this.localPoliciesCache = pulsar.getLocalMetadataStore().getMetadataCache(LocalPolicies.class);
    }

    public void initialize() {
        if (!getOwnershipCache().refreshSelfOwnerInfo()) {
            throw new RuntimeException("Failed to refresh self owner info.");
        }
    }

    public CompletableFuture<Optional<LookupResult>> getBrokerServiceUrlAsync(TopicName topic, LookupOptions options) {
        long startTime = System.nanoTime();

        CompletableFuture<Optional<LookupResult>> future = getBundleAsync(topic)//topic名根据hash算法找bundle
                .thenCompose(bundle -> findBrokerServiceUrl(bundle, options));//根据bundle找对应的broker

        future.thenAccept(optResult -> {
            lookupLatency.observe(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
            if (optResult.isPresent()) {
                if (optResult.get().isRedirect()) {
                    lookupRedirects.inc();
                } else {
                    lookupAnswers.inc();
                }
            }
        }).exceptionally(ex -> {
            lookupFailures.inc();
            return null;
        });

        return future;
    }
    //其核心为 findBundle 方法，前面的章节已经分析过了，这里不再赘述，简单说就是 topic 会落在 namespace 哪个 namespaceBundle
    public CompletableFuture<NamespaceBundle> getBundleAsync(TopicName topic) {
        return bundleFactory.getBundlesAsync(topic.getNamespaceObject())
                .thenApply(bundles -> bundles.findBundle(topic));
    }

    public Optional<NamespaceBundle> getBundleIfPresent(TopicName topicName) {
        Optional<NamespaceBundles> bundles = bundleFactory.getBundlesIfPresent(topicName.getNamespaceObject());
        return bundles.map(b -> b.findBundle(topicName));
    }

    public NamespaceBundle getBundle(TopicName topicName) {
        return bundleFactory.getBundles(topicName.getNamespaceObject()).findBundle(topicName);
    }

    public int getBundleCount(NamespaceName namespace) throws Exception {
        return bundleFactory.getBundles(namespace).size();
    }

    private NamespaceBundle getFullBundle(NamespaceName fqnn) throws Exception {
        return bundleFactory.getFullBundle(fqnn);
    }

    private CompletableFuture<NamespaceBundle> getFullBundleAsync(NamespaceName fqnn) {
        return bundleFactory.getFullBundleAsync(fqnn);
    }

    // 服务单位ID（上段代码请求的是 TopicName 实例）请求 broker 地址，这里注意readOnly 参数，
    // 这个参数不是 true，而是false，这很重要，从这个方法可以看出点题了文章开头的资源
    /**
     * Return the URL of the broker who's owning a particular service unit in asynchronous way.
     *
     * If the service unit is not owned, return a CompletableFuture with empty optional.
     */
    public CompletableFuture<Optional<URL>> getWebServiceUrlAsync(ServiceUnitId suName, LookupOptions options) {
        if (suName instanceof TopicName) {
            TopicName name = (TopicName) suName;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Getting web service URL of topic: {} - options: {}", name, options);
            }
            //走这条分支，通过 topicName 定位 namespaceBundle
            return getBundleAsync(name)
                    .thenCompose(namespaceBundle -> internalGetWebServiceUrl(namespaceBundle, options));
        }

        if (suName instanceof NamespaceName) {
            return getFullBundleAsync((NamespaceName) suName)
                    .thenCompose(namespaceBundle -> internalGetWebServiceUrl(namespaceBundle, options));
        }

        if (suName instanceof NamespaceBundle) {
            return internalGetWebServiceUrl((NamespaceBundle) suName, options);
        }

        throw new IllegalArgumentException("Unrecognized class of NamespaceBundle: " + suName.getClass().getName());
    }

    /**
     * Return the URL of the broker who's owning a particular service unit.
     *
     * If the service unit is not owned, return an empty optional
     */
    public Optional<URL> getWebServiceUrl(ServiceUnitId suName, LookupOptions options) throws Exception {
        return getWebServiceUrlAsync(suName, options)
                .get(pulsar.getConfiguration().getZooKeeperOperationTimeoutSeconds(), SECONDS);
    }

    //这个是内部的web service 地址
    private CompletableFuture<Optional<URL>> internalGetWebServiceUrl(NamespaceBundle bundle, LookupOptions options) {
        //到达这里，终于知道这个方法的用意了，每次，就是通过 namespaceBundle 找 broker 位置，类似于服务发现的意思
        return findBrokerServiceUrl(bundle, options).thenApply(lookupResult -> {
            if (lookupResult.isPresent()) {
                try {
                    LookupData lookupData = lookupResult.get().getLookupData();
                    final String redirectUrl = options.isRequestHttps()
                            ? lookupData.getHttpUrlTls() : lookupData.getHttpUrl();
                    return Optional.of(new URL(redirectUrl));
                } catch (Exception e) {
                    // just log the exception, nothing else to do
                    LOG.warn("internalGetWebServiceUrl [{}]", e.getMessage(), e);
                }
            }
            return Optional.empty();
        });
    }

    /**
     * Register all the bootstrap name spaces including the heartbeat namespace.
     *
     * @return
     * @throws PulsarServerException
     */
    public void registerBootstrapNamespaces() throws PulsarServerException {

        // ensure that we own the heartbeat namespace
        if (registerNamespace(getHeartbeatNamespace(host, config), true)) {
            this.uncountedNamespaces++;
            LOG.info("added heartbeat namespace name in local cache: ns={}", getHeartbeatNamespace(host, config));
        }

        // ensure that we own the heartbeat namespace
        if (registerNamespace(getHeartbeatNamespaceV2(host, config), true)) {
            this.uncountedNamespaces++;
            LOG.info("added heartbeat namespace name in local cache: ns={}", getHeartbeatNamespaceV2(host, config));
        }

        // we may not need strict ownership checking for bootstrap names for now
        for (String namespace : config.getBootstrapNamespaces()) {
            if (registerNamespace(NamespaceName.get(namespace), false)) {
                LOG.info("added bootstrap namespace name in local cache: ns={}", namespace);
            }
        }
    }

    /**
     * Tried to registers a namespace to this instance.
     *
     * @param nsname
     * @param ensureOwned
     * @return
     * @throws PulsarServerException
     * @throws Exception
     */
    public boolean registerNamespace(NamespaceName nsname, boolean ensureOwned) throws PulsarServerException {

        String myUrl = pulsar.getSafeBrokerServiceUrl();

        try {
            String otherUrl = null;
            NamespaceBundle nsFullBundle = null;

            // all pre-registered namespace is assumed to have bundles disabled
            nsFullBundle = bundleFactory.getFullBundle(nsname);
            // v2 namespace will always use full bundle object
            otherUrl = ownershipCache.tryAcquiringOwnership(nsFullBundle).get().getNativeUrl();

            if (myUrl.equals(otherUrl)) {
                if (nsFullBundle != null) {
                    // preload heartbeat namespace
                    pulsar.loadNamespaceTopics(nsFullBundle);
                }
                return true;
            }

            String msg = String.format("namespace already owned by other broker : ns=%s expected=%s actual=%s",
                    nsname, myUrl, otherUrl);

            // ignore if not be owned for now
            if (!ensureOwned) {
                LOG.info(msg);
                return false;
            }

            // should not happen
            throw new IllegalStateException(msg);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new PulsarServerException(e);
        }
    }

    private final ConcurrentOpenHashMap<NamespaceBundle, CompletableFuture<Optional<LookupResult>>>
            findingBundlesAuthoritative = new ConcurrentOpenHashMap<>();
    private final ConcurrentOpenHashMap<NamespaceBundle, CompletableFuture<Optional<LookupResult>>>
            findingBundlesNotAuthoritative = new ConcurrentOpenHashMap<>();

    //激动人心的时刻，通过 namespaceBundle 寻找合适的 broker
    /**
     * Main internal method to lookup and setup ownership of service unit to a broker.
     *
     * @param bundle
     * @param options
     * @return
     * @throws PulsarServerException
     */
    private CompletableFuture<Optional<LookupResult>> findBrokerServiceUrl(
            NamespaceBundle bundle, LookupOptions options) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("findBrokerServiceUrl: {} - options: {}", bundle, options);
        }

        ConcurrentOpenHashMap<NamespaceBundle, CompletableFuture<Optional<LookupResult>>> targetMap;
        //是否需要认证，认证和非认证的分开
        if (options.isAuthoritative()) {
            targetMap = findingBundlesAuthoritative;
        } else {
            targetMap = findingBundlesNotAuthoritative;
        }
        // 首先检查当前 broker 或其他 broker 已经所属了 namespaceBundle
        return targetMap.computeIfAbsent(bundle, (k) -> {
            CompletableFuture<Optional<LookupResult>> future = new CompletableFuture<>();

            // First check if we or someone else already owns the bundle
            ownershipCache.getOwnerAsync(bundle).thenAccept(nsData -> {
                //表示没有查到 namespaceBundle 分配 broker 信息
                if (!nsData.isPresent()) {
                    // No one owns this bundle
                    // 表示 namespaceBundle 无 broker 拥有（就是当前没分配给任一 broker）
                    if (options.isReadOnly()) {
                        //如果 readOnly 为 true，意味着不尝试把 namespaceBundle 分配 broker 上
                        // Do not attempt to acquire ownership
                        future.complete(Optional.empty());
                    } else {
                        // 现在，无 broker 拥有 namespaceBundle，这时候，尝试动态分配给候选 broker（这时候，上一章节更新的候选 broker 集终于要派上用场了）
                        // Now, no one owns the namespace yet. Hence, we will try to dynamically assign it
                        pulsar.getExecutor().execute(() -> {
                            //未bundle寻找对应broker，主要逻辑在这个
                            searchForCandidateBroker(bundle, future, options);
                        });
                    }
                } else if (nsData.get().isDisabled()) {
                    // 表示 namespaceBundle 正在被卸载中，抛异常
                    future.completeExceptionally(
                            new IllegalStateException(String.format("Namespace bundle %s is being unloaded", bundle)));
                } else {
                    //这表明 namespaceBundle 已经被分配了，broker 信息为 nsData
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Namespace bundle {} already owned by {} ", bundle, nsData);
                    }
                    // find the target
                    if (options.hasAdvertisedListenerName()) {
                        AdvertisedListener listener =
                                nsData.get().getAdvertisedListeners().get(options.getAdvertisedListenerName());
                        if (listener == null) {
                            future.completeExceptionally(
                                    new PulsarServerException("the broker do not have "
                                            + options.getAdvertisedListenerName() + " listener"));
                        } else {
                            URI urlTls = listener.getBrokerServiceUrlTls();
                            future.complete(Optional.of(new LookupResult(nsData.get(),
                                    listener.getBrokerServiceUrl().toString(),
                                    urlTls == null ? null : urlTls.toString())));
                        }
                        return;
                    } else {
                        future.complete(Optional.of(new LookupResult(nsData.get())));
                    }
                }
            }).exceptionally(exception -> {
                LOG.warn("Failed to check owner for bundle {}: {}", bundle, exception.getMessage(), exception);
                future.completeExceptionally(exception);
                return null;
            });
            //如果执行完，则移除相应的bundle，这里为什么要移除啊  部署刚加入吗 就要移除
            future.whenComplete((r, t) -> pulsar.getExecutor().execute(
                () -> targetMap.remove(bundle)
            ));

            return future;
        });
    }

    //给被卸载的 namespaceBundle 选择候选的 broker（前面章节知道：候选 broker 集是经过负载管理器筛选过的）
    private void searchForCandidateBroker(NamespaceBundle bundle,
                                          CompletableFuture<Optional<LookupResult>> lookupFuture,
                                          LookupOptions options) {
        if (null == pulsar.getLeaderElectionService()) {
            LOG.warn("The leader election has not yet been completed! NamespaceBundle[{}]", bundle);
            lookupFuture.completeExceptionally(
                    new IllegalStateException("The leader election has not yet been completed!"));
            return;
        }
        String candidateBroker = null;

        LeaderElectionService les = pulsar.getLeaderElectionService();
        if (les == null) {
            // The leader election service was not initialized yet. This can happen because the broker service is
            // initialized first and it might start receiving lookup requests before the leader election service is
            // fully initialized.
            lookupFuture.complete(Optional.empty());
            return;
        }

        //author这个字段，就是通过判断是否是leader来设置true的，当转发给了leader进行决策的时候，leader返回客户端授权一点是true，这样客户端再把该字段发给目标broker(在look up请求中携带)，
        //目标broker就知道是leader分配给我了，然后我在写到zk，触发全局回调，从而更新集群中所有broker的元数据信息
        boolean authoritativeRedirect = les.isLeader();

        try {
            // 先检查 namespaceBundle 所属的 namespace 是不是 Heartbeat 或 SLAMonitor namespace
            // check if this is Heartbeat or SLAMonitor namespace
            candidateBroker = checkHeartbeatNamespace(bundle);
            if (candidateBroker == null) {
                candidateBroker = checkHeartbeatNamespaceV2(bundle);
            }
            if (candidateBroker == null) {
                String broker = getSLAMonitorBrokerName(bundle);
                // 如果 broker 不为空，检查 broker 是不是存活的
                // checking if the broker is up and running
                if (broker != null && isBrokerActive(broker)) {
                    candidateBroker = broker;
                }
            }
            //还没找到候选 broker
            if (candidateBroker == null) {
                Optional<LeaderBroker> currentLeader = pulsar.getLeaderElectionService().getCurrentLeader();

                if (options.isAuthoritative()) {
                    //通过options.isAuthoritative来判断，该请求是否是被leader节点进行转发过来的，如果是，那么这里就不用再找了，直接用当前节点来返回，也就是说正常一个lookup请求最多会请求3个节点，转发两次请求
                    //其中options.isAuthoritative是从请求中出来的
                    //领导经纪人已将当前经纪人指定为所有者
                    // leader broker already assigned the current broker as owner
                    candidateBroker = pulsar.getSafeWebServiceAddress();
                } else if (!this.loadManager.get().isCentralized() //判断是否是集中式决策
                        || pulsar.getLeaderElectionService().isLeader()//当前 broker 是否领导者
                        // 如果领导者不是存活的，则回退到用当前 broker 负载均衡器拿到最小负载 broker（简单的说，如果领导者没选举出来或者宕机了，
                        //当前的 broker 也执行分配 namespaceBundle 任务）
                        || !currentLeader.isPresent()

                        // If leader is not active, fallback to pick the least loaded from current broker loadmanager
                        || !isBrokerActive(currentLeader.get().getServiceUrl())
                ) {
                    // 通过 namespaceBundle 从负载均衡器拿到最小负载可用 broker
                    Optional<String> availableBroker = getLeastLoadedFromLoadManager(bundle);
                    if (!availableBroker.isPresent()) {
                        //可用为空，返回空
                        lookupFuture.complete(Optional.empty());
                        return;
                    }
                    candidateBroker = availableBroker.get();
                    authoritativeRedirect = true;
                } else {
                    //如果是集中式的分配，那么久转发给leader来进行决策
                    // forward to leader broker to make assignment
                    candidateBroker = currentLeader.get().getServiceUrl();
                }
            }
        } catch (Exception e) {
            LOG.warn("Error when searching for candidate broker to acquire {}: {}", bundle, e.getMessage(), e);
            lookupFuture.completeExceptionally(e);
            return;
        }

        try {
            //预防性编码
            checkNotNull(candidateBroker);
            //发现选出的候选 broker 与当前 broker 一致
            if (candidateBroker.equals(pulsar.getSafeWebServiceAddress())) {
                // 负载管理器决定让当前 broker 试着把 bundle 收归自己拥有
                // Load manager decided that the local broker should try to become the owner
                ownershipCache.tryAcquiringOwnership(bundle).thenAccept(ownerInfo -> {
                    // bundle 正在卸载，抛异常
                    if (ownerInfo.isDisabled()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Namespace bundle {} is currently being unloaded", bundle);
                        }
                        lookupFuture.completeExceptionally(new IllegalStateException(
                                String.format("Namespace bundle %s is currently being unloaded", bundle)));
                    } else {
                        // 已成功把 bundle 收归当前 broker 所有，那么现在开启一个任务进行预加载 topic 集（原先分配 namespaceBundle 上的 topic 集）
                        // Found owner for the namespace bundle

                        if (options.isLoadTopicsInBundle()) {
                            // Schedule the task to pre-load topics
                            pulsar.loadNamespaceTopics(bundle);
                        }
                        // find the target
                        if (options.hasAdvertisedListenerName()) {
                            AdvertisedListener listener =
                                    ownerInfo.getAdvertisedListeners().get(options.getAdvertisedListenerName());
                            if (listener == null) {
                                lookupFuture.completeExceptionally(
                                        new PulsarServerException("the broker do not have "
                                                + options.getAdvertisedListenerName() + " listener"));
                                return;
                            } else {
                                URI urlTls = listener.getBrokerServiceUrlTls();
                                lookupFuture.complete(Optional.of(
                                        new LookupResult(ownerInfo, listener.getBrokerServiceUrl().toString(),
                                                urlTls == null ? null : urlTls.toString())));
                                return;
                            }
                        } else {
                            //返回当前 broker 的地址信息
                            lookupFuture.complete(Optional.of(new LookupResult(ownerInfo)));
                            return;
                        }
                    }
                }).exceptionally(exception -> {
                    LOG.warn("Failed to acquire ownership for namespace bundle {}: {}", bundle, exception);
                    lookupFuture.completeExceptionally(new PulsarServerException(
                            "Failed to acquire ownership for namespace bundle " + bundle, exception));
                    return null;
                });

            } else {
                // 负载管理器决定把 bundle 重定向其他的 broker 上
                // Load managed decider some other broker should try to acquire ownership

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Redirecting to broker {} to acquire ownership of bundle {}", candidateBroker, bundle);
                }

                // 获取 candidateBroker 详细地址信息，返回
                // Now setting the redirect url
                createLookupResult(candidateBroker, authoritativeRedirect, options.getAdvertisedListenerName())
                        .thenAccept(lookupResult -> lookupFuture.complete(Optional.of(lookupResult))) //lookupFuture中有一些动作信息，在这里会触发
                        .exceptionally(ex -> {
                            lookupFuture.completeExceptionally(ex);
                            return null;
                        });

            }
        } catch (Exception e) {
            LOG.warn("Error in trying to acquire namespace bundle ownership for {}: {}", bundle, e.getMessage(), e);
            lookupFuture.completeExceptionally(e);
        }
    }
    //这个方法就是，查找给定broker的url，然后返回，其中对应的url是 Type.RedirectUrl重定向类型
    protected CompletableFuture<LookupResult> createLookupResult(String candidateBroker, boolean authoritativeRedirect,
                                                                 final String advertisedListenerName)
            throws Exception {

        CompletableFuture<LookupResult> lookupFuture = new CompletableFuture<>();
        try {
            checkArgument(StringUtils.isNotBlank(candidateBroker), "Lookup broker can't be null " + candidateBroker);
            URI uri = new URI(candidateBroker);
            String path = String.format("%s/%s:%s", LoadManager.LOADBALANCE_BROKERS_ROOT, uri.getHost(),
                    uri.getPort());

            //这里localBrokerDataCache.get(path)实际会去读对应zk上的数据
            localBrokerDataCache.get(path).thenAccept(reportData -> {//reportData是从zk上获取到的数据
                if (reportData.isPresent()) {//如果数据存在
                    //lookupData名字取得太让人误会了
                    LocalBrokerData lookupData = (LocalBrokerData) reportData.get();
                    if (StringUtils.isNotBlank(advertisedListenerName)) {//看是否配置了advertisedListenerName
                        //如果配置了就获取对外保留的的advertisedListenerName名字
                        AdvertisedListener listener = lookupData.getAdvertisedListeners().get(advertisedListenerName);
                        if (listener == null) {
                            //从读取到的数据中没有，就抛异常
                            lookupFuture.completeExceptionally(
                                    new PulsarServerException(
                                            "the broker do not have " + advertisedListenerName + " listener"));
                        } else {
                            //构建url地址然后返回
                            URI urlTls = listener.getBrokerServiceUrlTls();
                            lookupFuture.complete(new LookupResult(lookupData.getWebServiceUrl(),
                                    lookupData.getWebServiceUrlTls(), listener.getBrokerServiceUrl().toString(),
                                    urlTls == null ? null : urlTls.toString(), authoritativeRedirect));
                        }
                    } else {
                        lookupFuture.complete(new LookupResult(lookupData.getWebServiceUrl(),
                                lookupData.getWebServiceUrlTls(), lookupData.getPulsarServiceUrl(),
                                lookupData.getPulsarServiceUrlTls(), authoritativeRedirect));
                    }
                } else {
                    //否则的话就是不存在该路径
                    lookupFuture.completeExceptionally(new MetadataStoreException.NotFoundException(path));
                }
            }).exceptionally(ex -> {
                lookupFuture.completeExceptionally(ex);
                return null;
            });
        } catch (Exception e) {
            lookupFuture.completeExceptionally(e);
        }
        return lookupFuture;
    }

    private boolean isBrokerActive(String candidateBroker) {
        List<String> brokers = pulsar.getLocalMetadataStore().getChildren(LoadManager.LOADBALANCE_BROKERS_ROOT).join();

        for (String brokerHostPort : brokers) {
            if (candidateBroker.equals("http://" + brokerHostPort)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Broker {} found for SLA Monitoring Namespace", brokerHostPort);
                }
                return true;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Broker not found for SLA Monitoring Namespace {}",
                    candidateBroker + ":" + config.getWebServicePort());
        }
        return false;
    }

    /**
     * Helper function to encapsulate the logic to invoke between old and new load manager.
     *
     * @return
     * @throws Exception
     */
    private Optional<String> getLeastLoadedFromLoadManager(ServiceUnitId serviceUnit) throws Exception {
        Optional<ResourceUnit> leastLoadedBroker = loadManager.get().getLeastLoaded(serviceUnit);
        if (!leastLoadedBroker.isPresent()) {
            LOG.warn("No broker is available for {}", serviceUnit);
            return Optional.empty();
        }

        String lookupAddress = leastLoadedBroker.get().getResourceId();
        if (LOG.isDebugEnabled()) {
            LOG.debug("{} : redirecting to the least loaded broker, lookup address={}",
                    pulsar.getSafeWebServiceAddress(),
                    lookupAddress);
        }
        return Optional.of(lookupAddress);
    }

    public CompletableFuture<Void> unloadNamespaceBundle(NamespaceBundle bundle) {
        // unload namespace bundle
        return unloadNamespaceBundle(bundle, 5, TimeUnit.MINUTES);
    }

    public CompletableFuture<Void> unloadNamespaceBundle(NamespaceBundle bundle, long timeout, TimeUnit timeoutUnit) {
        // unload namespace bundle
        OwnedBundle ob = ownershipCache.getOwnedBundle(bundle);
        if (ob == null) {
            return FutureUtil.failedFuture(new IllegalStateException("Bundle " + bundle + " is not currently owned"));
        } else {
            return ob.handleUnloadRequest(pulsar, timeout, timeoutUnit);
        }
    }

    public CompletableFuture<Boolean> isNamespaceBundleOwned(NamespaceBundle bundle) {
        return pulsar.getLocalMetadataStore().exists(ServiceUnitUtils.path(bundle));
    }

    public Map<String, NamespaceOwnershipStatus> getOwnedNameSpacesStatus() throws Exception {
        NamespaceIsolationPolicies nsIsolationPolicies = this.getLocalNamespaceIsolationPolicies();
        Map<String, NamespaceOwnershipStatus> ownedNsStatus = new HashMap<String, NamespaceOwnershipStatus>();
        for (OwnedBundle nsObj : this.ownershipCache.getOwnedBundles().values()) {
            NamespaceOwnershipStatus nsStatus = this.getNamespaceOwnershipStatus(nsObj,
                    nsIsolationPolicies.getPolicyByNamespace(nsObj.getNamespaceBundle().getNamespaceObject()));
            ownedNsStatus.put(nsObj.getNamespaceBundle().toString(), nsStatus);
        }

        return ownedNsStatus;
    }

    private NamespaceOwnershipStatus getNamespaceOwnershipStatus(OwnedBundle nsObj,
            NamespaceIsolationPolicy nsIsolationPolicy) {
        NamespaceOwnershipStatus nsOwnedStatus = new NamespaceOwnershipStatus(BrokerAssignment.shared, false,
                nsObj.isActive());
        if (nsIsolationPolicy == null) {
            // no matching policy found, this namespace must be an uncontrolled one and using shared broker
            return nsOwnedStatus;
        }
        // found corresponding policy, set the status to controlled
        nsOwnedStatus.is_controlled = true;
        if (nsIsolationPolicy.isPrimaryBroker(pulsar.getAdvertisedAddress())) {
            nsOwnedStatus.broker_assignment = BrokerAssignment.primary;
        } else if (nsIsolationPolicy.isSecondaryBroker(pulsar.getAdvertisedAddress())) {
            nsOwnedStatus.broker_assignment = BrokerAssignment.secondary;
        }

        return nsOwnedStatus;
    }

    private NamespaceIsolationPolicies getLocalNamespaceIsolationPolicies() throws Exception {
        String localCluster = pulsar.getConfiguration().getClusterName();
        return pulsar.getPulsarResources().getNamespaceResources().getIsolationPolicies()
                .getIsolationDataPolicies(localCluster)
                .orElseGet(() -> {
                    // the namespace isolation policies are empty/undefined = an empty object
                    return new NamespaceIsolationPolicies();
                });
    }

    public boolean isNamespaceBundleDisabled(NamespaceBundle bundle) throws Exception {
        try {
            // Does ZooKeeper says that the namespace is disabled?
            CompletableFuture<Optional<NamespaceEphemeralData>> nsDataFuture = ownershipCache.getOwnerAsync(bundle);
            if (nsDataFuture != null) {
                Optional<NamespaceEphemeralData> nsData = nsDataFuture.getNow(null);
                if (nsData != null && nsData.isPresent()) {
                    return nsData.get().isDisabled();
                } else {
                    return false;
                }
            } else {
                // if namespace is not owned, it is not considered disabled
                return false;
            }
        } catch (Exception e) {
            LOG.warn("Exception in getting ownership info for service unit {}: {}", bundle, e.getMessage(), e);
        }

        return false;
    }

    //分割属于自己的 namespaceBundle，包装方法
    /**
     *
     * 1. 将给定的包分成两个包
     * 2. 将两个捆绑包的所有权分配给当前经纪人
     * 3. 使用新创建的包更新策略到 LocalZK
     * 4.禁用原始包并刷新缓存。
     *
     * 它将调用 splitAndOwnBundleOnceAndRetry 来做真正的重试工作，这将重试“retryTimes”。
     *
     * 1. split the given bundle into two bundles 2. assign ownership of both the bundles to current broker 3. update
     * policies with newly created bundles into LocalZK 4. disable original bundle and refresh the cache.
     *
     * It will call splitAndOwnBundleOnceAndRetry to do the real retry work, which will retry "retryTimes".
     *
     * @param bundle
     * @return
     * @throws Exception
     */
    public CompletableFuture<Void> splitAndOwnBundle(NamespaceBundle bundle, boolean unload,
                                                     NamespaceBundleSplitAlgorithm splitAlgorithm) {

        final CompletableFuture<Void> unloadFuture = new CompletableFuture<>();
        //最大重试次数
        final AtomicInteger counter = new AtomicInteger(BUNDLE_SPLIT_RETRY_LIMIT);
        //它将调用 splitAndOwnBundleOnceAndRetry 来做真正的重试工作，这将重试“retryTimes”。
        splitAndOwnBundleOnceAndRetry(bundle, unload, counter, unloadFuture, splitAlgorithm);

        return unloadFuture;
    }

    void splitAndOwnBundleOnceAndRetry(NamespaceBundle bundle,
                                       boolean unload,
                                       AtomicInteger counter,
                                       CompletableFuture<Void> completionFuture,
                                       NamespaceBundleSplitAlgorithm splitAlgorithm) {
        //调用split分割算法进行分割，这里默认都是使用的按照数字范围均分成两个bundle的方式:RangeEquallyDivideBundleSplitAlgorithm
        splitAlgorithm.getSplitBoundary(this, bundle).whenComplete((splitBoundary, ex) -> {
            //splitBoundary这个是中间位置数字
            CompletableFuture<List<NamespaceBundle>> updateFuture = new CompletableFuture<>();
            if (ex == null) {
                try {
                    //默认把当前的bundle分割成2个bundle（分割算法见下）
                    bundleFactory.splitBundles(bundle,
                        2 /* by default split into 2 */, splitBoundary)
                            .thenAccept(splittedBundles -> {//splittedBundles命名不太好,比如应该取名为：splittedBundlesPair
                                // 这是拆分后的NamespaceBundles，因为并发写入 zookeeper，更新可能失败
                                // Split and updateNamespaceBundles. Update may fail because of concurrent write to
                                // Zookeeper.
                                if (splittedBundles == null) {//对拆分后的进行非空校验
                                    String msg = format("bundle %s not found under namespace", bundle.toString());
                                    LOG.warn(msg);
                                    updateFuture.completeExceptionally(new ServiceUnitNotReadyException(msg));
                                    return;
                                }

                                checkNotNull(splittedBundles.getLeft());
                                checkNotNull(splittedBundles.getRight());
                                checkArgument(splittedBundles.getRight().size() == 2,
                                        "bundle has to be split in two bundles");
                                NamespaceName nsname = bundle.getNamespaceObject();
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("[{}] splitAndOwnBundleOnce: {}, counter: {},  2 bundles: {}, {}",
                                            nsname.toString(), bundle.getBundleRange(), counter.get(),
                                            splittedBundles.getRight().get(0).getBundleRange(),
                                            splittedBundles.getRight().get(1).getBundleRange());
                                }
                                try {
                                    //分割出来的bundle
                                    // 试着把拆分后新产生的分区分配给当前 broker
                                    // take ownership of newly split bundles
                                    for (NamespaceBundle sBundle : splittedBundles.getRight()) {
                                        checkNotNull(ownershipCache.tryAcquiringOwnership(sBundle));
                                    }
                                    // 把拆分后的完整 NamespaceBundles 更新到 local zookeeper。
                                    updateNamespaceBundles(nsname, splittedBundles.getLeft())
                                            .thenRun(() -> {
                                                // 如果更新成功，则把本地缓存更新
                                                bundleFactory.invalidateBundleCache(bundle.getNamespaceObject());
                                                updateFuture.complete(splittedBundles.getRight());
                                            }).exceptionally(ex1 -> {
                                        //更新 NamespaceName 策略失败
                                        String msg = format("failed to update namespace policies [%s], "
                                                        + "NamespaceBundle: %s due to %s",
                                                nsname.toString(), bundle.getBundleRange(), ex1.getMessage());
                                        LOG.warn(msg);
                                        updateFuture.completeExceptionally(
                                                new ServiceUnitNotReadyException(msg, ex1.getCause()));
                                        return null;
                                    });
                                } catch (Exception e) {
                                    String msg = format(
                                            "failed to acquire ownership of split bundle for namespace [%s], %s",
                                            nsname.toString(), e.getMessage());
                                    LOG.warn(msg, e);
                                    updateFuture.completeExceptionally(new ServiceUnitNotReadyException(msg, e));
                                }
                            });
                } catch (Exception e) {
                    updateFuture.completeExceptionally(e);
                }
            } else {
                updateFuture.completeExceptionally(ex);
            }

            //更新动作完成，执行回调
            // If success updateNamespaceBundles, then do invalidateBundleCache and unload.
            // Else retry splitAndOwnBundleOnceAndRetry.
            updateFuture.whenCompleteAsync((r, t)-> {//r是新增的bundle
                if (t != null) {
                    //因版本导致异常和重试次数未达到7次，则继续尝试分割bundle任务
                    // retry several times on BadVersion
                    if ((t.getCause() instanceof MetadataStoreException.BadVersionException)
                            && (counter.decrementAndGet() >= 0)) {
                        pulsar.getOrderedExecutor()
                                .execute(() -> splitAndOwnBundleOnceAndRetry(
                                        bundle, unload, counter, completionFuture, splitAlgorithm));
                    } else if (t instanceof IllegalArgumentException) {
                        completionFuture.completeExceptionally(t);
                    } else {
                        // 达到重试次数，可能是其他原因导致的拆分bundle失败
                        // Retry enough, or meet other exception
                        String msg2 = format(" %s not success update nsBundles, counter %d, reason %s",
                            bundle.toString(), counter.get(), t.getMessage());
                        LOG.warn(msg2);
                        completionFuture.completeExceptionally(new ServiceUnitNotReadyException(msg2));
                    }
                    return;
                }

                // 拆分bundle任务成功，执行如下操作
                // success updateNamespaceBundles
                // disable old bundle in memory
                getOwnershipCache().updateBundleState(bundle, false)
                        .thenRun(() -> {
                            //更新 bundle 中的 topic
                            // update bundled_topic cache for load-report-generation
                            pulsar.getBrokerService().refreshTopicToStatsMaps(bundle);
                            //设置负载数据强制更新标记
                            loadManager.get().setLoadReportForceUpdateFlag();
                            completionFuture.complete(null);
                            if (unload) {  //根据开关来决定是否卸载拆分后生成的 namespaceBundles（这样，就可以把拆分后多出来的 namespaceBundle 附加到其他的 broker 上）
                                // Unload new split bundles, in background. This will not
                                // affect the split operation which is already safely completed
                                r.forEach(this::unloadNamespaceBundle);
                            }
                        })
                        .exceptionally(e -> {
                            String msg1 = format(
                                    "failed to disable bundle %s under namespace [%s] with error %s",
                                    bundle.getNamespaceObject().toString(), bundle.toString(), ex.getMessage());
                            LOG.warn(msg1, e);
                            completionFuture.completeExceptionally(new ServiceUnitNotReadyException(msg1));
                            return null;
                        });
            }, pulsar.getOrderedExecutor());
        });
    }

    /**
     * Update new bundle-range to LocalZk (create a new node if not present).
     * Update may fail because of concurrent write to Zookeeper.
     *
     * @param nsname
     * @param nsBundles
     * @throws Exception
     */
    private CompletableFuture<Void> updateNamespaceBundles(NamespaceName nsname, NamespaceBundles nsBundles) {
        checkNotNull(nsname);
        checkNotNull(nsBundles);

        LocalPolicies localPolicies = nsBundles.toLocalPolicies();

        return pulsar.getPulsarResources().getLocalPolicies()
                .setLocalPoliciesWithVersion(nsname, localPolicies, nsBundles.getVersion());
    }

    public OwnershipCache getOwnershipCache() {
        return ownershipCache;
    }

    public int getTotalServiceUnitsLoaded() {
        return ownershipCache.getOwnedBundles().size() - this.uncountedNamespaces;
    }

    public Set<NamespaceBundle> getOwnedServiceUnits() {
        return ownershipCache.getOwnedBundles().values().stream().map(OwnedBundle::getNamespaceBundle)
                .collect(Collectors.toSet());
    }

    public boolean isServiceUnitOwned(ServiceUnitId suName) throws Exception {
        if (suName instanceof TopicName) {
            return isTopicOwnedAsync((TopicName) suName).get();
        }

        if (suName instanceof NamespaceName) {
            return isNamespaceOwned((NamespaceName) suName);
        }

        if (suName instanceof NamespaceBundle) {
            return ownershipCache.isNamespaceBundleOwned((NamespaceBundle) suName);
        }

        throw new IllegalArgumentException("Invalid class of NamespaceBundle: " + suName.getClass().getName());
    }

    public CompletableFuture<Boolean> isServiceUnitOwnedAsync(ServiceUnitId suName) {
        if (suName instanceof TopicName) {
            return isTopicOwnedAsync((TopicName) suName);
        }

        if (suName instanceof NamespaceName) {
            return isNamespaceOwnedAsync((NamespaceName) suName);
        }

        if (suName instanceof NamespaceBundle) {
            return CompletableFuture.completedFuture(
                    ownershipCache.isNamespaceBundleOwned((NamespaceBundle) suName));
        }

        return FutureUtil.failedFuture(
                new IllegalArgumentException("Invalid class of NamespaceBundle: " + suName.getClass().getName()));
    }

    public boolean isServiceUnitActive(TopicName topicName) {
        try {
            return ownershipCache.getOwnedBundle(getBundle(topicName)).isActive();
        } catch (Exception e) {
            LOG.warn("Unable to find OwnedBundle for topic - [{}]", topicName);
            return false;
        }
    }

    private boolean isNamespaceOwned(NamespaceName fqnn) throws Exception {
        return ownershipCache.getOwnedBundle(getFullBundle(fqnn)) != null;
    }

    private CompletableFuture<Boolean> isNamespaceOwnedAsync(NamespaceName fqnn) {
        return getFullBundleAsync(fqnn)
                .thenApply(bundle -> ownershipCache.getOwnedBundle(bundle) != null);
    }

    private CompletableFuture<Boolean> isTopicOwnedAsync(TopicName topic) {
        return getBundleAsync(topic).thenApply(bundle -> ownershipCache.isNamespaceBundleOwned(bundle));
    }

    private boolean isTopicOwned(TopicName topicName) {
        Optional<NamespaceBundle> bundle = getBundleIfPresent(topicName);
        if (bundle.isPresent()) {
            return ownershipCache.getOwnedBundle(bundle.get()) != null;
        } else {
            // Calling `getBundle(TopicName)` here can cause a deadlock.
            // cf. https://github.com/apache/pulsar/pull/4190
            //
            // This method returns false once if the bundle metadata is not cached, but gets the metadata asynchronously
            // to cache it. Otherwise, the clients will never be able to connect
            // to the topic due to ServiceUnitNotReadyException.
            // cf. https://github.com/apache/pulsar/pull/5919
            getBundleAsync(topicName).thenAccept(bundle2 -> {
                LOG.info("Succeeded in getting bundle {} for topic - [{}]", bundle2, topicName);
            }).exceptionally(ex -> {
                LOG.warn("Failed to get bundle for topic - [{}] {}", topicName, ex.getMessage());
                return null;
            });
            return false;
        }
    }

    public CompletableFuture<Boolean> checkTopicOwnership(TopicName topicName) {
        return getBundleAsync(topicName)
                .thenApply(ownershipCache::checkOwnership);
    }

    public void removeOwnedServiceUnit(NamespaceBundle nsBundle) throws Exception {
        ownershipCache.removeOwnership(nsBundle).get(pulsar.getConfiguration().getZooKeeperOperationTimeoutSeconds(),
                SECONDS);
        bundleFactory.invalidateBundleCache(nsBundle.getNamespaceObject());
    }

    protected void onNamespaceBundleOwned(NamespaceBundle bundle) {
        for (NamespaceBundleOwnershipListener bundleOwnedListener : bundleOwnershipListeners) {
            notifyNamespaceBundleOwnershipListener(bundle, bundleOwnedListener);
        }
    }

    protected void onNamespaceBundleUnload(NamespaceBundle bundle) {
        for (NamespaceBundleOwnershipListener bundleOwnedListener : bundleOwnershipListeners) {
            try {
                if (bundleOwnedListener.test(bundle)) {
                    bundleOwnedListener.unLoad(bundle);
                }
            } catch (Throwable t) {
                LOG.error("Call bundle {} ownership lister error", bundle, t);
            }
        }
    }

    public void addNamespaceBundleOwnershipListener(NamespaceBundleOwnershipListener... listeners) {
        checkNotNull(listeners);
        for (NamespaceBundleOwnershipListener listener : listeners) {
            if (listener != null) {
                bundleOwnershipListeners.add(listener);
            }
        }
        getOwnedServiceUnits().forEach(bundle -> notifyNamespaceBundleOwnershipListener(bundle, listeners));
    }

    private void notifyNamespaceBundleOwnershipListener(NamespaceBundle bundle,
                    NamespaceBundleOwnershipListener... listeners) {
        if (listeners != null) {
            for (NamespaceBundleOwnershipListener listener : listeners) {
                try {
                    if (listener.test(bundle)) {
                        listener.onLoad(bundle);
                    }
                } catch (Throwable t) {
                    LOG.error("Call bundle {} ownership lister error", bundle, t);
                }
            }
        }
    }

    public NamespaceBundleFactory getNamespaceBundleFactory() {
        return bundleFactory;
    }

    public ServiceUnitId getServiceUnitId(TopicName topicName) throws Exception {
        return getBundle(topicName);
    }

    public CompletableFuture<List<String>> getFullListOfTopics(NamespaceName namespaceName) {
        return getListOfPersistentTopics(namespaceName)
                .thenCombine(getListOfNonPersistentTopics(namespaceName),
                        (persistentTopics, nonPersistentTopics) -> {
                            return ListUtils.union(persistentTopics, nonPersistentTopics);
                        });
    }

    public CompletableFuture<List<String>> getOwnedTopicListForNamespaceBundle(NamespaceBundle bundle) {
        return getFullListOfTopics(bundle.getNamespaceObject()).thenCompose(topics ->
                CompletableFuture.completedFuture(
                        topics.stream()
                                .filter(topic -> bundle.includes(TopicName.get(topic)))
                                .collect(Collectors.toList())))
                .thenCombine(getAllPartitions(bundle.getNamespaceObject()).thenCompose(topics ->
                        CompletableFuture.completedFuture(
                                topics.stream().filter(topic -> bundle.includes(TopicName.get(topic)))
                                        .collect(Collectors.toList()))), (left, right) -> {
                    for (String topic : right) {
                        if (!left.contains(topic)) {
                            left.add(topic);
                        }
                    }
                    return left;
                });
    }

    public CompletableFuture<Boolean> checkTopicExists(TopicName topic) {
        if (topic.isPersistent()) {
            return pulsar.getPulsarResources().getTopicResources().persistentTopicExists(topic);
        } else {
            return pulsar.getBrokerService()
                    .getTopicIfExists(topic.toString())
                    .thenApply(optTopic -> optTopic.isPresent());
        }
    }

    public CompletableFuture<List<String>> getListOfTopics(NamespaceName namespaceName, Mode mode) {
        switch (mode) {
            case ALL:
                return getFullListOfTopics(namespaceName);
            case NON_PERSISTENT:
                return getListOfNonPersistentTopics(namespaceName);
            case PERSISTENT:
            default:
                return getListOfPersistentTopics(namespaceName);
        }
    }

    public CompletableFuture<List<String>> getAllPartitions(NamespaceName namespaceName) {
        return getPartitions(namespaceName, TopicDomain.persistent)
                .thenCombine(getPartitions(namespaceName, TopicDomain.non_persistent),
                        ListUtils::union);
    }


    public CompletableFuture<List<String>> getPartitions(NamespaceName namespaceName, TopicDomain topicDomain) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting children from partitioned-topics now: {} - {}", namespaceName, topicDomain);
        }

        return pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .listPartitionedTopicsAsync(namespaceName, topicDomain)
                .thenCompose(topics -> {
            CompletableFuture<List<String>> result = new CompletableFuture<>();
            List<String> resultPartitions = Collections.synchronizedList(Lists.newArrayList());
            if (CollectionUtils.isNotEmpty(topics)) {
                List<CompletableFuture<List<String>>> futures = Lists.newArrayList();
                for (String topic : topics) {
                    CompletableFuture<List<String>> future = getPartitionsForTopic(TopicName.get(topic));
                    futures.add(future);
                    future.thenAccept(resultPartitions::addAll);
                }
                FutureUtil.waitForAll(futures).whenComplete((v, ex) -> {
                    if (ex != null) {
                        result.completeExceptionally(ex);
                    } else {
                        result.complete(resultPartitions);
                    }
                });
            } else {
                result.complete(resultPartitions);
            }
            return result;
        });
    }

    private CompletableFuture<List<String>> getPartitionsForTopic(TopicName topicName) {
        return pulsar.getBrokerService().fetchPartitionedTopicMetadataAsync(topicName).thenCompose(meta -> {
            List<String> result = Lists.newArrayList();
            for (int i = 0; i < meta.partitions; i++) {
                result.add(topicName.getPartition(i).toString());
            }
            return CompletableFuture.completedFuture(result);
        });
    }

    public CompletableFuture<List<String>> getListOfPersistentTopics(NamespaceName namespaceName) {
        return pulsar.getPulsarResources().getTopicResources().listPersistentTopicsAsync(namespaceName);
    }

    public CompletableFuture<List<String>> getListOfNonPersistentTopics(NamespaceName namespaceName) {

        return PulsarWebResource.checkLocalOrGetPeerReplicationCluster(pulsar, namespaceName)
                .thenCompose(peerClusterData -> {
                    // if peer-cluster-data is present it means namespace is owned by that peer-cluster and request
                    // should be redirect to the peer-cluster
                    if (peerClusterData != null) {
                        return getNonPersistentTopicsFromPeerCluster(peerClusterData, namespaceName);
                    } else {
                        // Non-persistent topics don't have managed ledgers so we have to retrieve them from local
                        // cache.
                        List<String> topics = Lists.newArrayList();
                        synchronized (pulsar.getBrokerService().getMultiLayerTopicMap()) {
                            if (pulsar.getBrokerService().getMultiLayerTopicMap()
                                    .containsKey(namespaceName.toString())) {
                                pulsar.getBrokerService().getMultiLayerTopicMap().get(namespaceName.toString())
                                        .forEach((__, bundle) -> {
                                            bundle.forEach((topicName, topic) -> {
                                                if (topic instanceof NonPersistentTopic
                                                        && ((NonPersistentTopic) topic).isActive()) {
                                                    topics.add(topicName);
                                                }
                                            });
                                        });
                            }
                        }

                        topics.sort(null);
                        return CompletableFuture.completedFuture(topics);
                    }
                });
    }

    private CompletableFuture<List<String>> getNonPersistentTopicsFromPeerCluster(ClusterDataImpl peerClusterData,
                                                                                  NamespaceName namespace) {
        PulsarClientImpl client = getNamespaceClient(peerClusterData);
        return client.getLookup().getTopicsUnderNamespace(namespace, Mode.NON_PERSISTENT);
    }

    public PulsarClientImpl getNamespaceClient(ClusterDataImpl cluster) {
        PulsarClientImpl client = namespaceClients.get(cluster);
        if (client != null) {
            return client;
        }

        return namespaceClients.computeIfAbsent(cluster, key -> {
            try {
                ClientBuilder clientBuilder = PulsarClient.builder()
                    .enableTcpNoDelay(false)
                    .statsInterval(0, TimeUnit.SECONDS);

                if (pulsar.getConfiguration().isAuthenticationEnabled()) {
                    clientBuilder.authentication(pulsar.getConfiguration().getBrokerClientAuthenticationPlugin(),
                        pulsar.getConfiguration().getBrokerClientAuthenticationParameters());
                }

                if (pulsar.getConfiguration().isTlsEnabled()) {
                    clientBuilder
                        .serviceUrl(isNotBlank(cluster.getBrokerServiceUrlTls())
                            ? cluster.getBrokerServiceUrlTls() : cluster.getServiceUrlTls())
                        .enableTls(true)
                        .tlsTrustCertsFilePath(pulsar.getConfiguration().getBrokerClientTrustCertsFilePath())
                        .allowTlsInsecureConnection(pulsar.getConfiguration().isTlsAllowInsecureConnection());
                } else {
                    clientBuilder.serviceUrl(isNotBlank(cluster.getBrokerServiceUrl())
                        ? cluster.getBrokerServiceUrl() : cluster.getServiceUrl());
                }

                // Share all the IO threads across broker and client connections
                ClientConfigurationData conf = ((ClientBuilderImpl) clientBuilder).getClientConfigurationData();
                return new PulsarClientImpl(conf, (EventLoopGroup) pulsar.getBrokerService().executor());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public Optional<NamespaceEphemeralData> getOwner(NamespaceBundle bundle) throws Exception {
        // if there is no znode for the service unit, it is not owned by any broker
        return getOwnerAsync(bundle).get(pulsar.getConfiguration().getZooKeeperOperationTimeoutSeconds(), SECONDS);
    }

    public CompletableFuture<Optional<NamespaceEphemeralData>> getOwnerAsync(NamespaceBundle bundle) {
        return ownershipCache.getOwnerAsync(bundle);
    }

    public void unloadSLANamespace() throws Exception {
        PulsarAdmin adminClient = null;
        NamespaceName namespaceName = getSLAMonitorNamespace(host, config);

        LOG.info("Checking owner for SLA namespace {}", namespaceName);

        NamespaceBundle nsFullBundle = getFullBundle(namespaceName);
        if (!getOwner(nsFullBundle).isPresent()) {
            // No one owns the namespace so no point trying to unload it
            // Next lookup will assign the bundle to this broker.
            return;
        }

        LOG.info("Trying to unload SLA namespace {}", namespaceName);
        adminClient = pulsar.getAdminClient();
        adminClient.namespaces().unload(namespaceName.toString());
        LOG.info("Namespace {} unloaded successfully", namespaceName);
    }

    public static NamespaceName getHeartbeatNamespace(String host, ServiceConfiguration config) {
        Integer port = null;
        if (config.getWebServicePort().isPresent()) {
            port = config.getWebServicePort().get();
        } else if (config.getWebServicePortTls().isPresent()) {
            port = config.getWebServicePortTls().get();
        }
        return NamespaceName.get(String.format(HEARTBEAT_NAMESPACE_FMT, config.getClusterName(), host, port));
    }

    public static NamespaceName getHeartbeatNamespaceV2(String host, ServiceConfiguration config) {
        Integer port = null;
        if (config.getWebServicePort().isPresent()) {
            port = config.getWebServicePort().get();
        } else if (config.getWebServicePortTls().isPresent()) {
            port = config.getWebServicePortTls().get();
        }
        return NamespaceName.get(String.format(HEARTBEAT_NAMESPACE_FMT_V2, host, port));
    }

    public static NamespaceName getSLAMonitorNamespace(String host, ServiceConfiguration config) {
        Integer port = null;
        if (config.getWebServicePort().isPresent()) {
            port = config.getWebServicePort().get();
        } else if (config.getWebServicePortTls().isPresent()) {
            port = config.getWebServicePortTls().get();
        }
        return NamespaceName.get(String.format(SLA_NAMESPACE_FMT, config.getClusterName(), host, port));
    }

    public static String checkHeartbeatNamespace(ServiceUnitId ns) {
        Matcher m = HEARTBEAT_NAMESPACE_PATTERN.matcher(ns.getNamespaceObject().toString());
        if (m.matches()) {
            LOG.debug("SLAMonitoring namespace matched the lookup namespace {}", ns.getNamespaceObject().toString());
            return String.format("http://%s", m.group(1));
        } else {
            return null;
        }
    }

    public static String checkHeartbeatNamespaceV2(ServiceUnitId ns) {
        Matcher m = HEARTBEAT_NAMESPACE_PATTERN_V2.matcher(ns.getNamespaceObject().toString());
        if (m.matches()) {
            LOG.debug("SLAMonitoring namespace matched the lookup namespace {}", ns.getNamespaceObject().toString());
            return String.format("http://%s", m.group(1));
        } else {
            return null;
        }
    }

    public static String getSLAMonitorBrokerName(ServiceUnitId ns) {
        Matcher m = SLA_NAMESPACE_PATTERN.matcher(ns.getNamespaceObject().toString());
        if (m.matches()) {
            return String.format("http://%s", m.group(1));
        } else {
            return null;
        }
    }

    public boolean registerSLANamespace() throws PulsarServerException {
        boolean isNameSpaceRegistered = registerNamespace(getSLAMonitorNamespace(host, config), false);
        if (isNameSpaceRegistered) {
            this.uncountedNamespaces++;

            if (LOG.isDebugEnabled()) {
                LOG.debug("Added SLA Monitoring namespace name in local cache: ns={}",
                        getSLAMonitorNamespace(host, config));
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("SLA Monitoring not owned by the broker: ns={}", getSLAMonitorNamespace(host, config));
        }
        return isNameSpaceRegistered;
    }

    @Override
    public void close() {
        namespaceClients.forEach((cluster, client) -> {
            try {
                client.shutdown();
            } catch (PulsarClientException e) {
                LOG.warn("Error shutting down namespace client for cluster {}", cluster, e);
            }
        });
    }
}

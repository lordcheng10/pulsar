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
package org.apache.pulsar.client.impl.conf;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Sets;

import io.swagger.annotations.ApiModelProperty;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.client.api.ServiceUrlProvider;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.util.Secret;

/**
 * This is a simple holder of the client configuration values.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClientConfigurationData implements Serializable, Cloneable {
    private static final long serialVersionUID = 1L;

    //broker或discovery-service或pulsar-proxy地址
    @ApiModelProperty(
            name = "serviceUrl",
            value = "Pulsar cluster HTTP URL to connect to a broker."
    )
    //这里可以根据实际需求，动态提供serviceUrl
    private String serviceUrl;
    @ApiModelProperty(
            name = "serviceUrlProvider",
            value = "The implementation class of ServiceUrlProvider used to generate ServiceUrl."
    )
    @JsonIgnore
    private transient ServiceUrlProvider serviceUrlProvider;

    //认证接口，默认是禁用的
    @ApiModelProperty(
            name = "authentication",
            value = "Authentication settings of the client."
    )
    @JsonIgnore
    private Authentication authentication;

    @ApiModelProperty(
            name = "authPluginClassName",
            value = "Class name of authentication plugin of the client."
    )
    private String authPluginClassName;

    @ApiModelProperty(
            name = "authParams",
            value = "Authentication parameter of the client."
    )
    @Secret
    private String authParams;

    @ApiModelProperty(
            name = "authParamMap",
            value = "Authentication map of the client."
    )
    @Secret
    private Map<String, String> authParamMap;

    //操作超时时间
    @ApiModelProperty(
            name = "operationTimeoutMs",
            value = "Client operation timeout (in millisecond)."
    )
    private long operationTimeoutMs = 30000;

    @ApiModelProperty(
            name = "lookupTimeoutMs",
            value = "Client lookup timeout (in millisecond)."
    )
    private long lookupTimeoutMs = -1;

    //状态收集时间间隔
    //状态收集是什么意思
    @ApiModelProperty(
            name = "statsIntervalSeconds",
            value = " Interval to print client stats (in second)."
    )
    private long statsIntervalSeconds = 60;

    //生产者网络IO线程数
    @ApiModelProperty(
            name = "numIoThreads",
            value = "Number of IO threads."
    )
    private int numIoThreads = 1;

    //消费者订阅线程数
    @ApiModelProperty(
            name = "numListenerThreads",
            value = "Number of consumer listener threads."
    )
    private int numListenerThreads = 1;

    //每个broker（与客户端）连接数
    @ApiModelProperty(
            name = "connectionsPerBroker",
            value = "Number of connections established between the client and each Broker."
                    + "A value of 0 means to disable connection pooling."
    )
    private int connectionsPerBroker = 1;

    //是否延迟发送（Nagle算法，不延迟，意味着立即发送）
    @ApiModelProperty(
            name = "useTcpNoDelay",
            value = "Whether to use TCP NoDelay option."
    )
    private boolean useTcpNoDelay = true;

    //是否用TLS
    @ApiModelProperty(
            name = "useTls",
            value = "Whether to use TLS."
    )
    private boolean useTls = false;

    //TLS受信任证书路径
    @ApiModelProperty(
            name = "tlsTrustCertsFilePath",
            value = "Path to the trusted TLS certificate file."
    )
    private String tlsTrustCertsFilePath = "";

    //TLS是否允许不安全连接
    @ApiModelProperty(
            name = "tlsAllowInsecureConnection",
            value = "Whether the client accepts untrusted TLS certificates from the broker."
    )
    private boolean tlsAllowInsecureConnection = false;

    //TLS是否启用主机名校验
    @ApiModelProperty(
            name = "tlsHostnameVerificationEnable",
            value = "Whether the hostname is validated when the proxy creates a TLS connection with brokers."
    )
    private boolean tlsHostnameVerificationEnable = false;

    //Lookup请求并发数
    @ApiModelProperty(
            name = "concurrentLookupRequest",
            value = "The number of concurrent lookup requests that can be sent on each broker connection. "
                    + "Setting a maximum prevents overloading a broker."
    )
    private int concurrentLookupRequest = 5000;

    //最大Lookup请求数
    @ApiModelProperty(
            name = "maxLookupRequest",
            value = "Maximum number of lookup requests allowed on "
                    + "each broker connection to prevent overloading a broker."
    )
    private int maxLookupRequest = 50000;

    @ApiModelProperty(
            name = "maxLookupRedirects",
            value = "Maximum times of redirected lookup requests."
    )
    private int maxLookupRedirects = 20;

    //每个连接最大拒绝请求数（？是不是每个连接最大请求数超过N时，则拒绝发送）
    @ApiModelProperty(
            name = "maxNumberOfRejectedRequestPerConnection",
            value = "Maximum number of rejected requests of a broker in a certain time frame (30 seconds) "
                    + "after the current connection is closed and the client "
                    + "creating a new connection to connect to a different broker."
    )
    private int maxNumberOfRejectedRequestPerConnection = 50;

    //保活间隔时间
    @ApiModelProperty(
            name = "keepAliveIntervalSeconds",
            value = "Seconds of keeping alive interval for each client broker connection."
    )
    private int keepAliveIntervalSeconds = 30;

    //连接超时时间
    @ApiModelProperty(
            name = "connectionTimeoutMs",
            value = "Duration of waiting for a connection to a broker to be established."
                    + "If the duration passes without a response from a broker, the connection attempt is dropped."
    )
    private int connectionTimeoutMs = 10000;
    @ApiModelProperty(
            name = "requestTimeoutMs",
            value = "Maximum duration for completing a request."
    )
    private int requestTimeoutMs = 60000;

    @ApiModelProperty(
            name = "initialBackoffIntervalNanos",
            value = "Initial backoff interval (in nanosecond)."
    )
    private long initialBackoffIntervalNanos = TimeUnit.MILLISECONDS.toNanos(100);

    @ApiModelProperty(
            name = "maxBackoffIntervalNanos",
            value = "Max backoff interval (in nanosecond)."
    )
    private long maxBackoffIntervalNanos = TimeUnit.SECONDS.toNanos(60);

    @ApiModelProperty(
            name = "enableBusyWait",
            value = "Whether to enable BusyWait for EpollEventLoopGroup."
    )
    private boolean enableBusyWait = false;

    @ApiModelProperty(
            name = "listenerName",
            value = "Listener name for lookup. Clients can use listenerName to choose one of the listeners "
                    + "as the service URL to create a connection to the broker as long as the network is accessible."
                    + "\"advertisedListeners\" must enabled in broker side."
    )
    private String listenerName;

    @ApiModelProperty(
            name = "useKeyStoreTls",
            value = "Set TLS using KeyStore way."
    )
    private boolean useKeyStoreTls = false;
    @ApiModelProperty(
            name = "sslProvider",
            value = "The TLS provider used by an internal client to authenticate with other Pulsar brokers."
    )
    private String sslProvider = null;

    @ApiModelProperty(
            name = "tlsTrustStoreType",
            value = "TLS TrustStore type configuration. You need to set this configuration when client authentication is required."
    )
    private String tlsTrustStoreType = "JKS";

    @ApiModelProperty(
            name = "tlsTrustStorePath",
            value = "Path of TLS TrustStore."
    )
    private String tlsTrustStorePath = null;

    @ApiModelProperty(
            name = "tlsTrustStorePassword",
            value = "Password of TLS TrustStore."
    )
    private String tlsTrustStorePassword = null;

    @ApiModelProperty(
            name = "tlsCiphers",
            value = "Set of TLS Ciphers."
    )
    private Set<String> tlsCiphers = Sets.newTreeSet();

    @ApiModelProperty(
            name = "tlsProtocols",
            value = "Protocols of TLS."
    )
    private Set<String> tlsProtocols = Sets.newTreeSet();

    //这个的默认值我觉得不应该是0
    @ApiModelProperty(
            name = "memoryLimitBytes",
            value = "Limit of client memory usage (in byte)."
    )
    private long memoryLimitBytes = 0;

    @ApiModelProperty(
            name = "proxyServiceUrl",
            value = "URL of proxy service. proxyServiceUrl and proxyProtocol must be mutually inclusive."
    )
    private String proxyServiceUrl;

    @ApiModelProperty(
            name = "proxyProtocol",
            value = "Protocol of proxy service. proxyServiceUrl and proxyProtocol must be mutually inclusive."
    )
    private ProxyProtocol proxyProtocol;

    @ApiModelProperty(
            name = "enableTransaction",
            value = "Whether to enable transaction."
    )
    private boolean enableTransaction = false;

    @JsonIgnore
    private Clock clock = Clock.systemDefaultZone();

    // socks5
    @ApiModelProperty(
            name = "socks5ProxyAddress",
            value = "Address of SOCKS5 proxy."
    )
    private InetSocketAddress socks5ProxyAddress;

    @ApiModelProperty(
            name = "socks5ProxyUsername",
            value = "User name of SOCKS5 proxy."
    )
    private String socks5ProxyUsername;

    @ApiModelProperty(
            name = "socks5ProxyUsername",
            value = "Password of SOCKS5 proxy."
    )
    private String socks5ProxyPassword;

    public Authentication getAuthentication() {
        if (authentication == null) {
            this.authentication = AuthenticationDisabled.INSTANCE;
        }
        return authentication;
    }

    public void setAuthentication(Authentication authentication) {
        this.authentication = authentication;
    }
    public boolean isUseTls() {
        if (useTls)
            return true;
        if (getServiceUrl() != null && (this.getServiceUrl().startsWith("pulsar+ssl") || this.getServiceUrl().startsWith("https"))) {
            this.useTls = true;
            return true;
        }
        return false;
    }

    public long getLookupTimeoutMs() {
        if (lookupTimeoutMs >= 0) {
            return lookupTimeoutMs;
        } else {
            return operationTimeoutMs;
        }
    }

    public ClientConfigurationData clone() {
        try {
            return (ClientConfigurationData) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Failed to clone ClientConfigurationData");
        }
    }

    public InetSocketAddress getSocks5ProxyAddress() {
        if (Objects.nonNull(socks5ProxyAddress)) {
            return socks5ProxyAddress;
        }
        String proxyAddress = System.getProperty("socks5Proxy.address");
        return Optional.ofNullable(proxyAddress).map(address -> {
            try {
                URI uri = URI.create(address);
                return new InetSocketAddress(uri.getHost(), uri.getPort());
            } catch (Exception e) {
                throw new RuntimeException("Invalid config [socks5Proxy.address]", e);
            }
        }).orElse(null);
    }

    public String getSocks5ProxyUsername() {
        return Objects.nonNull(socks5ProxyUsername) ? socks5ProxyUsername : System.getProperty("socks5Proxy.username");
    }

    public String getSocks5ProxyPassword() {
        return Objects.nonNull(socks5ProxyPassword) ? socks5ProxyPassword : System.getProperty("socks5Proxy.password");
    }
}

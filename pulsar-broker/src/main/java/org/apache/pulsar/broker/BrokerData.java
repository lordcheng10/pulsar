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
package org.apache.pulsar.broker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Data;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;


// BrokerData 实际上包含一个 broker 负载数据，一个时间聚合负载数据，一个预先分配的 namespaceBundle 数据。
//包含三个组件的数据类，其中包含有关其他代理的领导代理可用的所有数据：
//- 由每个单独的代理 (LocalBrokerData) 写入 ZooKeeper 的本地代理数据。
//  - 由领导代理 (TimeAverageBrokerData) 写入 ZooKeeper 的时间平均包数据。
//- 未写入 ZooKeeper 但由领导代理维护的预分配包 (Map<String, BundleData>)。
/**
 * Data class containing three components comprising all the data available for the leader broker about other brokers: -
 * The local broker data which is written to ZooKeeper by each individual broker (LocalBrokerData). - The time average
 * bundle data which is written to ZooKeeper by the leader broker (TimeAverageBrokerData). - The preallocated bundles
 * which are not written to ZooKeeper but are maintained by the leader broker (Map<String, BundleData>).
 */
@Data
public class BrokerData {
    private LocalBrokerData localData;
    private TimeAverageBrokerData timeAverageData;
    //预分配就是当客户端发送lookup请求到leader时候，发现一个未分配的bundle，此时leader会为其找到一个broker，
    //然后放到预分配的map中，此时因为还没有实际分配，所以叫做预分配，只有当客户端收到leader的分配结果，通过再次重定向发送lookup请求给目标broker后，
    //目标broker通过请求中的author字段来判断是否是leader给我分配的，是的话，那么就直接负责该bundle并在zk上记录好，从而触发全局broker回调，同步集群整体元数据信息。
    //但是这个结构有个问题，考虑到Leader将分配结果返回给客户端后，客户端这个时候挂了，那么再次重启后，当再次发送或重定向lookup请求到leader后，还是采用的老的分配策略（可能是几天前的，结合当时负载算出来的broker，此时还用它就不太合理了）
    //当然这种case很少，即便遇到，产生不均衡后，最后也可以通过自动负载均衡给修复过来，当然最好是避免这种情况
    private Map<String, BundleData> preallocatedBundleData;

    /**
     * Initialize this BrokerData using the most recent local data.
     *
     * @param localData
     *            The data local for the broker.
     */
    public BrokerData(final LocalBrokerData localData) {
        this.localData = localData;
        timeAverageData = new TimeAverageBrokerData();
        preallocatedBundleData = new ConcurrentHashMap<>();
    }
}

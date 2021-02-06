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
package org.apache.pulsar.common.allocator;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.allocator.ByteBufAllocatorBuilder;
import org.apache.bookkeeper.common.allocator.LeakDetectionPolicy;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;

/**
 * 下面这段话的理解是：持有ByteBuffer分配器。
 * 似乎这个类是用来分配byte buffer的
 *
 * Holder of a ByteBuf allocator.
 */
@UtilityClass
@Slf4j
public class PulsarByteBufAllocator {
    /**
     * PULSAR_ALLOCATOR_POOLED ： 似乎意思是 pulsar 分配 池
     * PULSAR_ALLOCATOR_EXIT_ON_OOM：似乎是pulsar 在oom的时候 是否要退出的开关
     * PULSAR_ALLOCATOR_LEAK_DETECTION： pulsar 分配器泄漏探测
     * */
    public static final String PULSAR_ALLOCATOR_POOLED = "pulsar.allocator.pooled";
    public static final String PULSAR_ALLOCATOR_EXIT_ON_OOM = "pulsar.allocator.exit_on_oom";
    public static final String PULSAR_ALLOCATOR_LEAK_DETECTION = "pulsar.allocator.leak_detection";

    /**
     * 默认的byte buffer分配器.
     * 这个到底是干啥的呢？
     * */
    public static final ByteBufAllocator DEFAULT;

    /**
     * 似乎是out of memory 错误的监听器
     * */
    private static final List<Consumer<OutOfMemoryError>> LISTENERS = new CopyOnWriteArrayList<>();

    /**
     * 注册OM监听器
     * */
    public static void registerOOMListener(Consumer<OutOfMemoryError> listener) {
        LISTENERS.add(listener);
    }

    /**
     * 这个应该是OM是否要退出的开关。
     * */
    private static final boolean EXIT_ON_OOM;

    static {
    /**
     * 下面这个写法好奇怪。到底是什么意思？ isPooled这个变量字面意思似乎是：是否要用"池" ， 默认是true，要用池
     *
     * <p>equalsIgnoreCase方法是忽略大小写进行比较。
     *
     * <p>(System.getProperty这个方法又是什么意思呢 ？ 我们通常不都是从配置文件去读配置的吗 怎么这里似乎是从系统去读？难道是系统环境变量？那么问题来了
     * ，pulsar是怎么设置进去的呢？ 全局搜索了下，是这样设置的 （写在pom文件里的）：
     * <argLine> -Xmx1G -XX:+UseG1GC
     * -Dpulsar.allocator.pooled=false -Dpulsar.allocator.leak_detection=Advanced
     * -Dpulsar.allocator.exit_on_oom=false
     * </argLine>
     * 这个又是啥呢 似乎是用了一个maven插件：maven-surefire-plugin
     *
     * 总的来说：System.getProperty方法是获取系统某个属性，这里PULSAR_ALLOCATOR_POOLED不是系统自带的属性，是我们通过maven插件在pom文件里配置后自动set进系统的。
     *
     * 这里说的系统属性，就和我们设置jvm堆大小一样，pom里面设置的只是用来给测试用例用的，如果正式服务要启动的话，需要和jvm堆大小一样设置，
     * 加-Dpulsar.allocator.pooled=true   来开启，当然默认就是开启的，所以全局搜索除了pom里面设置为false给测试用外，没搜到其他地方用，
     * 在启动脚本里也没有，如果正式服务启动要关闭pool的话，那么需要和jvm堆大小一样加-Dpulsar.allocator.pooled=false来关闭。
     *
     * 这里我学到了设置系统变量或者叫属性的方法，就是加-D后面接变量名和value值。
     *
     */
        boolean isPooled = "true".equalsIgnoreCase(System.getProperty(PULSAR_ALLOCATOR_POOLED, "true"));
        /**
         * EXIT_ON_OOM ： 看变量名意思是宕OM的时候，是否退出，默认是false 不退出。
         * */
        EXIT_ON_OOM = "true".equalsIgnoreCase(System.getProperty(PULSAR_ALLOCATOR_EXIT_ON_OOM, "false"));

        /**
         * LeakDetectionPolicy 类名解释：Leak（泄漏） Detection （探测） Policy （策略）。
         * 一共有四种策略：Disabled（关闭）、Simple（简单）、Advanced（高级）、Paranoid（偏执严格）
         * 默认是处于关闭状态；
         * */
        LeakDetectionPolicy leakDetectionPolicy = LeakDetectionPolicy
                .valueOf(System.getProperty(PULSAR_ALLOCATOR_LEAK_DETECTION, "Disabled"));

        /***
         * log.isDebugEnabled方法是用来判断是否开启debug的。
         * */
        if (log.isDebugEnabled()) {
            log.debug("Is Pooled: {} -- Exit on OOM: {}", isPooled, EXIT_ON_OOM);
        }

        /**
         * 下面这个方法是创建一个ByteBuffer分配器的构造类对象.
         * 那么问题来了，ByteBufAllocatorBuilder是用来干啥的？具体有啥作用？
         *
         * 这里创建builder构造器对象的目的，只是为了构建一个默认的byte buffer分配器，因为builder只是一个临时变量，并未弄成类变量。
         * */
        ByteBufAllocatorBuilder builder = ByteBufAllocatorBuilder.create()
                .leakDetectionPolicy(leakDetectionPolicy)
                .pooledAllocator(PooledByteBufAllocator.DEFAULT)
                .outOfMemoryListener(oomException -> {
                    // First notify all listeners
                    LISTENERS.forEach(c -> {
                        try {
                            c.accept(oomException);
                        } catch (Throwable t) {
                            log.warn("Exception during OOM listener: {}", t.getMessage(), t);
                        }
                    });

                    if (EXIT_ON_OOM) {
                        log.info("Exiting JVM process for OOM error: {}", oomException.getMessage(), oomException);
                        Runtime.getRuntime().halt(1);
                    }
                });

        /**
         * 判断是否用池，从而以此来设置builder不同的池化策略：builder.poolingPolicy
         * */
        if (isPooled) {
            builder.poolingPolicy(PoolingPolicy.PooledDirect);
        } else {
            builder.poolingPolicy(PoolingPolicy.UnpooledHeap);
        }

        /**
         * 最后通过builder构建一个默认的byte buffer分配器。
         * */
        DEFAULT = builder.build();
    }
}

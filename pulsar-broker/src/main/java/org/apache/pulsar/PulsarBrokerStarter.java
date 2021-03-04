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
package org.apache.pulsar;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.common.configuration.PulsarConfigurationLoader.create;
import static org.apache.pulsar.common.configuration.PulsarConfigurationLoader.isComplete;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.FileInputStream;
import java.net.MalformedURLException;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import org.apache.bookkeeper.common.util.ReflectionUtils;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.AutoRecoveryMain;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.util.DirectMemoryUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.naming.NamespaceBundleSplitAlgorithm;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.service.WorkerServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

public class PulsarBrokerStarter {

    /**
     * 这里load配置的方法逻辑  没太明白.
     *
     * 这里就简单加载一个配置 怎么感觉写得好麻烦
     * */
    private static ServiceConfiguration loadConfig(String configFile) throws Exception {
        /**
         * 这里为啥要先remove handler ，然后在install里又加上呢？
         *
         * 可以看到removeHandlersForRootLogger方法上的注释：
         * Invoking this method removes/unregisters/detaches all handlers currently attached to the root logger（调用此方法将删除/取消注册/分离当前附加到根记录器的所有处理程序）
         *
         * SLF4JBridgeHandler到底是干啥的？https://www.javaer101.com/article/572277.html
         * SLF4JBridgeHandler是java.util.logging（JUL）日志记录网桥，它将“拦截” JUL日志记录语句并将其路由到SLF4J。
         *
         * 看起来SLF4JBridgeHandler的作用是：将原本Java Logging API的日志输出重定向到SLF4J日志框架。参考：https://blog.csdn.net/ITzhangdaopin/article/details/88018589
         * 这里为啥要重定向呢？我理解是一个项目中有多重日志框架，为了统一才需要使用SLF4J来重定向,为啥这里也需要呢？
         *
         * 其实还是没理解为啥下面要加这两行代码？作用是啥？
         * */
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        /**
         * 直接通过文件输入流读取文件到Properties，然后通过Properties来构建ServiceConfiguration对象。
         * */
        ServiceConfiguration config = create((new FileInputStream(configFile)), ServiceConfiguration.class);
        // it validates provided configuration is completed
        isComplete(config);
        return config;
    }

    @VisibleForTesting
    private static class StarterArguments {
        @Parameter(names = {"-c", "--broker-conf"}, description = "Configuration file for Broker")
        private String brokerConfigFile =
                Paths.get("").toAbsolutePath().normalize().toString() + "/conf/broker.conf";

        @Parameter(names = {"-rb", "--run-bookie"}, description = "Run Bookie together with Broker")
        private boolean runBookie = false;

        @Parameter(names = {"-ra", "--run-bookie-autorecovery"},
                description = "Run Bookie Autorecovery together with broker")
        private boolean runBookieAutoRecovery = false;

        @Parameter(names = {"-bc", "--bookie-conf"}, description = "Configuration file for Bookie")
        private String bookieConfigFile =
                Paths.get("").toAbsolutePath().normalize().toString() + "/conf/bookkeeper.conf";

        @Parameter(names = {"-rfw", "--run-functions-worker"}, description = "Run functions worker with Broker")
        private boolean runFunctionsWorker = false;

        @Parameter(names = {"-fwc", "--functions-worker-conf"}, description = "Configuration file for Functions Worker")
        private String fnWorkerConfigFile =
                Paths.get("").toAbsolutePath().normalize().toString() + "/conf/functions_worker.yml";


        /**
         * 这里就使用到了JCommander的注解获取参数，并赋值的功能。
         * 如果程序启动时，传入了--h 为true那么，这里就直接把help设置为true。这样在启动的时候就会走到输出使用方法说明
         * */
        @Parameter(names = {"-h", "--help"}, description = "Show this help message")
        private boolean help = false;
    }

    private static ServerConfiguration readBookieConfFile(String bookieConfigFile) throws IllegalArgumentException {
        ServerConfiguration bookieConf = new ServerConfiguration();
        try {
            bookieConf.loadConf(new File(bookieConfigFile).toURI().toURL());
            bookieConf.validate();
            log.info("Using bookie configuration file {}", bookieConfigFile);
        } catch (MalformedURLException e) {
            log.error("Could not open configuration file: {}", bookieConfigFile, e);
            throw new IllegalArgumentException("Could not open configuration file");
        } catch (ConfigurationException e) {
            log.error("Malformed configuration file: {}", bookieConfigFile, e);
            throw new IllegalArgumentException("Malformed configuration file");
        }

        if (bookieConf.getMaxPendingReadRequestPerThread() < bookieConf.getRereplicationEntryBatchSize()) {
            throw new IllegalArgumentException(
                "rereplicationEntryBatchSize should be smaller than " + "maxPendingReadRequestPerThread");
        }
        return bookieConf;
    }

    private static boolean argsContains(String[] args, String arg) {
        return Arrays.asList(args).contains(arg);
    }

    private static class BrokerStarter {
        private final ServiceConfiguration brokerConfig;
        private final PulsarService pulsarService;
        private final BookieServer bookieServer;
        private final AutoRecoveryMain autoRecoveryMain;
        private final StatsProvider bookieStatsProvider;
        private final ServerConfiguration bookieConfig;
        private final WorkerService functionsWorkerService;
        private final WorkerConfig workerConfig;

        BrokerStarter(String[] args) throws Exception{
            StarterArguments starterArguments = new StarterArguments();
            /**
             * JCommander是干嘛的呢?
             * 查了下网上的说法，是这样解释的：非常小的java框架，用于解析命令行参数
             * 参考：https://www.cnblogs.com/zhangshihai1232/articles/6027573.html
             *
             * 看了下上面的博客，有个模糊理解：JCommander是专门解析main函数传参的。
             *
             * 可以再看看这个博文：http://kangkona.github.io/jcommander-using-example/
             *
             * 一句话：JCommander是一个命令行参数解析工具
             *
             *
             * 这个博客也注意到了pulsar源码使用到了JCommander,参考：https://cloud.tencent.com/developer/article/1366912
             *
             * JCommander框架源码见：https://github.com/cbeust/jcommander
             *
             * 通过标签就可以自动把java程序启动传入的-D参数赋值给对应变量,而不用自己写代码去解析并赋值了。
             * */
            JCommander jcommander = new JCommander(starterArguments);
            /**
             * jcommander.setProgramName这个方法到底是干啥的？看名字似乎是设置程序启动后的进程名的， 但似乎又不是。做了个测试程序，验证了下，并未改变进程名。
             * */
            jcommander.setProgramName("PulsarBrokerStarter");

            // parse args by JCommander
            jcommander.parse(args);

          /**
           * 这里starterArguments.help变量默认为false，我理解是如果jcommander.parse解析有问题得话，会把jcommander.help设置为true，从而使得走到if里面，打印该程序的使用方法，并退出。
           * 上面jcommander.parse做参数校验，starterArguments.help是一个boolean类型变量，作为参数校验结果。使用了JCommander的注解传参解析，并赋值的功能。
           *
           * <p>不是这样的，上面有行代码 @Parameter(names = {"-h", "--help"}, description = "Show this help
           * message") private boolean help = false;
           */
            if (starterArguments.help) {
                jcommander.usage();
                System.exit(-1);
            }

          /**
           * 参数校验，检查传入的参数--broker-conf 是否合理, 如果是下面的几种情况，都会认为不合理，然后退出。
           *
           * <pre>
           *      * StringUtils.isBlank(null)      = true
           *      * StringUtils.isBlank("")        = true
           *      * StringUtils.isBlank(" ")       = true
           *      * StringUtils.isBlank("bob")     = false
           *      * StringUtils.isBlank("  bob  ") = false
           *      * </pre>
           */
          // init broker config
            if (isBlank(starterArguments.brokerConfigFile)) {
                jcommander.usage();
                throw new IllegalArgumentException("Need to specify a configuration file for broker");
            } else {
                /**
                 * 如果是合理的配置，那么这里就加载配置。
                 * */
                brokerConfig = loadConfig(starterArguments.brokerConfigFile);
            }

            int maxFrameSize = brokerConfig.getMaxMessageSize() + Commands.MESSAGE_SIZE_FRAME_PADDING;
            if (maxFrameSize >= DirectMemoryUtils.maxDirectMemory()) {
                throw new IllegalArgumentException("Max message size need smaller than jvm directMemory");
            }

            if (!NamespaceBundleSplitAlgorithm.AVAILABLE_ALGORITHMS.containsAll(
                    brokerConfig.getSupportedNamespaceBundleSplitAlgorithms())) {
                throw new IllegalArgumentException(
                        "The given supported namespace bundle split algorithm has unavailable algorithm. "
                                + "Available algorithms are " + NamespaceBundleSplitAlgorithm.AVAILABLE_ALGORITHMS);
            }

            if (!brokerConfig.getSupportedNamespaceBundleSplitAlgorithms().contains(
                    brokerConfig.getDefaultNamespaceBundleSplitAlgorithm())) {
                throw new IllegalArgumentException("Supported namespace bundle split algorithms "
                        + "must contains the default namespace bundle split algorithm");
            }

            // init functions worker
            if (starterArguments.runFunctionsWorker || brokerConfig.isFunctionsWorkerEnabled()) {
                workerConfig = PulsarService.initializeWorkerConfigFromBrokerConfig(
                    brokerConfig, starterArguments.fnWorkerConfigFile
                );
                functionsWorkerService = WorkerServiceLoader.load(workerConfig);
            } else {
                workerConfig = null;
                functionsWorkerService = null;
            }

            // init pulsar service
            pulsarService = new PulsarService(brokerConfig,
                                              workerConfig,
                                              Optional.ofNullable(functionsWorkerService),
                                              (exitCode) -> {
                                                  log.info("Halting broker process with code {}",
                                                           exitCode);
                                                  Runtime.getRuntime().halt(exitCode);
                                              });

            // if no argument to run bookie in cmd line, read from pulsar config
            if (!argsContains(args, "-rb") && !argsContains(args, "--run-bookie")) {
                checkState(!starterArguments.runBookie,
                        "runBookie should be false if has no argument specified");
                starterArguments.runBookie = brokerConfig.isEnableRunBookieTogether();
            }
            if (!argsContains(args, "-ra") && !argsContains(args, "--run-bookie-autorecovery")) {
                checkState(!starterArguments.runBookieAutoRecovery,
                        "runBookieAutoRecovery should be false if has no argument specified");
                starterArguments.runBookieAutoRecovery = brokerConfig.isEnableRunBookieAutoRecoveryTogether();
            }

            if ((starterArguments.runBookie || starterArguments.runBookieAutoRecovery)
                && isBlank(starterArguments.bookieConfigFile)) {
                jcommander.usage();
                throw new IllegalArgumentException("No configuration file for Bookie");
            }

            // init stats provider
            if (starterArguments.runBookie || starterArguments.runBookieAutoRecovery) {
                checkState(isNotBlank(starterArguments.bookieConfigFile),
                    "No configuration file for Bookie");
                bookieConfig = readBookieConfFile(starterArguments.bookieConfigFile);
                Class<? extends StatsProvider> statsProviderClass = bookieConfig.getStatsProviderClass();
                bookieStatsProvider = ReflectionUtils.newInstance(statsProviderClass);
            } else {
                bookieConfig = null;
                bookieStatsProvider = null;
            }

            // init bookie server
            if (starterArguments.runBookie) {
                checkNotNull(bookieConfig, "No ServerConfiguration for Bookie");
                checkNotNull(bookieStatsProvider, "No Stats Provider for Bookie");
                bookieServer = new BookieServer(
                        bookieConfig, bookieStatsProvider.getStatsLogger(""), null);
            } else {
                bookieServer = null;
            }

            // init bookie AutorecoveryMain
            if (starterArguments.runBookieAutoRecovery) {
                checkNotNull(bookieConfig, "No ServerConfiguration for Bookie Autorecovery");
                autoRecoveryMain = new AutoRecoveryMain(bookieConfig);
            } else {
                autoRecoveryMain = null;
            }
        }

        public void start() throws Exception {
            /***
             * bookieStatsProvider这个是啥
             * */
            if (bookieStatsProvider != null) {
                bookieStatsProvider.start(bookieConfig);
                log.info("started bookieStatsProvider.");
            }

            /**
             * bookieServer启动bookieServer
             * */
            if (bookieServer != null) {
                bookieServer.start();
                log.info("started bookieServer.");
            }

            /**
             * autoRecoveryMain 启动autoRecoveryMain，这个也是bookeepier的。
             * */
            if (autoRecoveryMain != null) {
                autoRecoveryMain.start();
                log.info("started bookie autoRecoveryMain.");
            }


            /**
             * 上面全部是启动bookeeper相关组件，这里才是启动pulsar 的.
             * PulsarService是干啥的
             * */
            pulsarService.start();
            log.info("PulsarService started.");
        }

        public void join() throws InterruptedException {
            pulsarService.waitUntilClosed();

            try {
                pulsarService.close();
            } catch (PulsarServerException e) {
                throw new RuntimeException();
            }

            if (bookieServer != null) {
                bookieServer.join();
            }
            if (autoRecoveryMain != null) {
                autoRecoveryMain.join();
            }
        }

        public void shutdown() {
            if (null != functionsWorkerService) {
                functionsWorkerService.stop();
                log.info("Shut down functions worker service successfully.");
            }

            pulsarService.getShutdownService().run();
            log.info("Shut down broker service successfully.");

            if (bookieStatsProvider != null) {
                bookieStatsProvider.stop();
                log.info("Shut down bookieStatsProvider successfully.");
            }
            if (bookieServer != null) {
                bookieServer.shutdown();
                log.info("Shut down bookieServer successfully.");
            }
            if (autoRecoveryMain != null) {
                autoRecoveryMain.shutdown();
                log.info("Shut down autoRecoveryMain successfully.");
            }
        }
    }

    /**
     * 这里是服务端代码一切的开始
     * */
    public static void main(String[] args) throws Exception {
        /**
         * 日期用的DateFormat，但记得阿里java开发手册上这样说了：
         * 5.【强制】SimpleDateFormat 是线程不安全的类，一般不要定义为 static 变量，如果定义为
         * static， 必须加锁，或者使用 DateUtils 工具类。
         * 正例:注意线程安全，使用 DateUtils。亦推荐如下处理:
         * private static final ThreadLocal<DateFormat> df = new ThreadLocal<DateFormat>() {
         *      @Override
         *      protected DateFormat initialValue() {
         *          return new SimpleDateFormat("yyyy-MM-dd");
         *      }
         * };
         * 说明:如果是 JDK8的应用，可以使用 Instant 代替 Date，LocalDateTime 代替 Calendar， DateTimeFormatter 代替
         * SimpleDateFormat，官方给出的解释:simple beautiful strong immutable thread-safe。
         *
         *
         * 上面这段话怎么理解呢？ 首先ThreadLocal是什么意思  有什么作用？ 关于ThradLocal可以看：http://note.youdao.com/s/WBhvQ2SU
         * 另外注意这句话 如果是 JDK8的应用， DateTimeFormatter 代替SimpleDateFormat。
         * 从这个上看似乎应该使用DateTimeFormatter更好。而且似乎是jdk官方给定的建议. 那么为什么要这么建议呢？只是出于线程安全考虑吗？这里使用需要考虑线程安全吗？
         *
         * Thread.setDefaultUncaughtExceptionHandler是一个什么方法？代表什么意思？看了下Thread源码注释，有这样一句话：
         * Set the default handler invoked when a thread abruptly terminates due to an uncaught exception, and no other handler has been defined for that thread.
         * 意思是当一个线程由于一个未捕获的异常突然挂掉时，会调用默认的handler处理方法，这里这个方法就是用来设置所有线程在这种情况下的默认处理方法的。
         */
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS");
        /**
         * Thread.setDefaultUncaughtExceptionHandler是一个什么方法？代表什么意思？看了下Thread源码注释，有这样一句话：
         *     Set the default handler invoked when a thread abruptly terminates due to an uncaught exception, and no other handler has been defined for that thread.
         *     意思是当一个线程由于一个未捕获的异常突然挂掉时，会调用默认的handler处理方法，这里这个方法就是用来设置所有线程在这种情况下的默认处理方法的。
         *
         * Thread.setDefaultUncaughtExceptionHandler方法是传入一个函数接口，我们知道函数接口的入参，可以直接传入一个函数，也就是java的函数式编程。
         *
         * 这里的方法主要是打个日志，把出问题的线程名和异常message、日期打印出来。
         *
         * 那这里有个问题啊 ，如果多个线程同时走到这个handler，同时打印日志，但是这里的日期dateFormat用的是SimpleDateFormat，是非线程安全的，会不会有问题？
         * 这里只是读，没有修改操作，所以应该没有什么问题。那么SimpleDateFormat 类指的线程安全问题到底是什么场景下发生的？是多个线程修改这个类吗
         *
         * 这里SimpleDateFormat感觉真有问题，参考：https://blog.csdn.net/csdn_ds/article/details/72984646
         *
         * 确实是有线程安全问题，但是似乎这个问题不大，而且要出现抛异常的概率不大。 TODO-chenlin patch SimpleDateFormat线程不安全问题
         *
         * 我从https://github.com/apache/pulsar/commit/32e7f337a6c0a889ace58890996d53179123d813中发现了这三个类：PulsarBrokerStarter.java 、DiscoveryServiceStarter.java、ProxyServiceStarter.java
         * 这三个类有什么关系呢？ 另外，ambition119建议这里用log4j 重定向到控制台输出，但是为啥没有采纳这个建议呢？而且这个patch是为了fix一个bug，然后有人建议他把日期打出来，才顺道岛上日期的，只不过我觉得这个日志不安全。
         * pulsar社区罗列了目前的bug list：https://github.com/apache/pulsar/labels/type%2Fbug  这个patch只是解决了其中的一个。
         *
         * 这里format()方法线程不安全主要体现在该方法里面这行代码：
         *  calendar.setTime(date);
         */
        Thread.setDefaultUncaughtExceptionHandler((thread, exception) -> {
            System.out.println(String.format("%s [%s] error Uncaught exception in thread %s: %s",
                    dateFormat.format(new Date()), thread.getContextClassLoader(),
                    thread.getName(), exception.getMessage()));
        });


        /**
         * 构建broker启动实例。
         * 并构建一个退出时的钩子：程序退出时，会调用BrokerStarter的shutdown方法。
         * */
        BrokerStarter starter = new BrokerStarter(args);
        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> {
                starter.shutdown();
            })
        );

        /**
         * 这里有事干啥呢？处理OM？OM监听器？好像很高级的样子。
         * PulsarByteBufAllocator 这个是干啥的？
         *
         * */
        PulsarByteBufAllocator.registerOOMListener(oomException -> {
            if (starter.brokerConfig.isSkipBrokerShutdownOnOOM()) {
                log.error("-- Received OOM exception: {}", oomException.getMessage(), oomException);
            } else {
                log.error("-- Shutting down - Received OOM exception: {}", oomException.getMessage(), oomException);
                starter.shutdown();
            }
        });


        /**
         * 启动BrokerStart，遇到异常后调用 Runtime.getRuntime().halt(1);是为了干啥
         * 这个方法是干嘛的？然后在finally里面等待shutdown完成，喔  难道 Runtime.getRuntime().halt(1);是为了触发钩子用的？不然为啥在finally中会join等待呢？
         * */
        try {
            starter.start();
        } catch (Throwable t) {
            log.error("Failed to start pulsar service.", t);
            Runtime.getRuntime().halt(1);
        } finally {
            starter.join();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(PulsarBrokerStarter.class);
}

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
import com.beust.jcommander.Parameters;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.util.datetime.FixedDateFormat;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.naming.NamespaceBundleSplitAlgorithm;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.CmdGenerateDocs;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.service.WorkerServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

public class PulsarBrokerStarter {

    private static ServiceConfiguration loadConfig(String configFile) throws Exception {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            ServiceConfiguration config = create(inputStream, ServiceConfiguration.class);
            // it validates provided configuration is completed
            isComplete(config);
            return config;
        }
    }

    //命令列表，这里就是支持的所有命令
    @VisibleForTesting
    @Parameters(commandDescription = "Options")
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

        @Parameter(names = {"-h", "--help"}, description = "Show this help message")
        private boolean help = false;

        @Parameter(names = {"-g", "--generate-docs"}, description = "Generate docs")
        private boolean generateDocs = false;
    }

    //通用配置加载器
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
        //服务配置文件类
        private final ServiceConfiguration brokerConfig;
        //Pulsar 服务类
        private final PulsarService pulsarService;
        //Bookkeeper 客户端
        private final BookieServer bookieServer;
        //Bookeeper 自动恢复服务
        private final AutoRecoveryMain autoRecoveryMain;
        //Bookeeper 状态提供服务
        private final StatsProvider bookieStatsProvider;
        //服务器配置类
        private final ServerConfiguration bookieConfig;
        //函数式工作者服务
        private final WorkerService functionsWorkerService;
        private final WorkerConfig workerConfig;

        BrokerStarter(String[] args) throws Exception {
            StarterArguments starterArguments = new StarterArguments();
            JCommander jcommander = new JCommander(starterArguments);
            jcommander.setProgramName("PulsarBrokerStarter");

            // JCommander 组件解析命令行
            // parse args by JCommander
            jcommander.parse(args);
            if (starterArguments.help) {
                jcommander.usage();
                System.exit(-1);
            }

            if (starterArguments.generateDocs) {
                CmdGenerateDocs cmd = new CmdGenerateDocs("pulsar");
                cmd.addCommand("broker", starterArguments);
                cmd.run(null);
                System.exit(-1);
            }

            // 初始化 broker 配置文件
            // init broker config
            if (isBlank(starterArguments.brokerConfigFile)) {
                jcommander.usage();
                throw new IllegalArgumentException("Need to specify a configuration file for broker");
            } else {
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

            // 初始化 functions worker ，判定是否启用函数工作者服务
            // 默认是不开启的
            // init functions worker
            if (starterArguments.runFunctionsWorker || brokerConfig.isFunctionsWorkerEnabled()) {
                //初始化函数工作者配置文件
                workerConfig = PulsarService.initializeWorkerConfigFromBrokerConfig(
                    brokerConfig, starterArguments.fnWorkerConfigFile
                );
                // 配置工作者与本地 broker 通信
                functionsWorkerService = WorkerServiceLoader.load(workerConfig);
            } else {
                //意味着不启用
                workerConfig = null;
                functionsWorkerService = null;
            }

            // 初始化 pulsar 服务
            // init pulsar service
            pulsarService = new PulsarService(brokerConfig,
                                              workerConfig,
                                              Optional.ofNullable(functionsWorkerService),
                                              (exitCode) -> {
                                                  log.info("Halting broker process with code {}",
                                                           exitCode);
                                                  Runtime.getRuntime().halt(exitCode);
                                              });

            // 如果在命令行中没有运行 bookie （命令），则尝试从 pulsar 配置文件中读取
            // if no argument to run bookie in cmd line, read from pulsar config
            if (!argsContains(args, "-rb") && !argsContains(args, "--run-bookie")) {
                checkState(!starterArguments.runBookie,
                        "runBookie should be false if has no argument specified");
                starterArguments.runBookie = brokerConfig.isEnableRunBookieTogether();
            }
            // 加载顺序同上，表示是否启用 bookie 自动恢复服务
            if (!argsContains(args, "-ra") && !argsContains(args, "--run-bookie-autorecovery")) {
                checkState(!starterArguments.runBookieAutoRecovery,
                        "runBookieAutoRecovery should be false if has no argument specified");
                starterArguments.runBookieAutoRecovery = brokerConfig.isEnableRunBookieAutoRecoveryTogether();
            }

            // 从这里可以看出，要么运行 bookie（读写） 服务，要么运行 bookie 自动恢复服务
            if ((starterArguments.runBookie || starterArguments.runBookieAutoRecovery)
                && isBlank(starterArguments.bookieConfigFile)) {
                jcommander.usage();
                throw new IllegalArgumentException("No configuration file for Bookie");
            }
            // 初始化 bookie 状态提供者
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

            // 如果配置文件设置当前服务器运行 bookie 服务，则初始化 bookie 服务器
            // init bookie server
            if (starterArguments.runBookie) {
                checkNotNull(bookieConfig, "No ServerConfiguration for Bookie");
                checkNotNull(bookieStatsProvider, "No Stats Provider for Bookie");
                bookieServer = new BookieServer(
                        bookieConfig, bookieStatsProvider.getStatsLogger(""), null);
            } else {
                bookieServer = null;
            }

            // 如果设置 bookie 自动恢复标志，则初始化 bookie 自动恢复服务
            // init bookie AutorecoveryMain
            if (starterArguments.runBookieAutoRecovery) {
                checkNotNull(bookieConfig, "No ServerConfiguration for Bookie Autorecovery");
                autoRecoveryMain = new AutoRecoveryMain(bookieConfig);
            } else {
                autoRecoveryMain = null;
            }
        }

        //bookieStatsProvider 、bookieServer 、autoRecoveryMain 三个组件，如果不为空，则将会启动， pulsarService 作为核心服务必须启动。具体各个组件服务的初始化和启动过程
        public void start() throws Exception {
            if (bookieStatsProvider != null) {
                bookieStatsProvider.start(bookieConfig);
                log.info("started bookieStatsProvider.");
            }
            if (bookieServer != null) {
                bookieServer.start();
                log.info("started bookieServer.");
            }
            if (autoRecoveryMain != null) {
                autoRecoveryMain.start();
                log.info("started bookie autoRecoveryMain.");
            }

            pulsarService.start();
            log.info("PulsarService started.");
        }

        //此方法主线程有提到，用于阻塞主线程，防止主线程退出，实现主要是信号量，等待JVM触发关闭方法，然后信号被触发， 与此同时，bookieServer 与 autoRecoveryMain 都将会关闭，这样主线程不再阻塞，执行完即退出。
        //BrokerStarter 阻塞方法（即等待关闭指令）
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

        //此方法主线程同样有提到，其在JVM关闭钩子中调用的，确保所有正使用的组件优雅有序退出。
        public void shutdown() throws Exception {
            if (null != functionsWorkerService) {
                functionsWorkerService.stop();
                log.info("Shut down functions worker service successfully.");
            }

            pulsarService.close();
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


    //构造方法主要执行如下功能：
    //命令解析
    //配置文件加载与验证
    //服务初始化

    //主要是 BrokerStarter 唱主角，而主线程只做了如下操作：
    //设置线程异常退出时执行的方法（这里是打印日志）
    //创建 BrokerStarter 实例，并把命令行参数放入（解析命令）
    //安装当JVM退出时钩子，即 BrokerStarter 关闭
    //调用 BrokerStarter 开始方法
    //等待 BrokerStarter 退出
    public static void main(String[] args) throws Exception {
        DateFormat dateFormat = new SimpleDateFormat(
            FixedDateFormat.FixedFormat.ISO8601_OFFSET_DATE_TIME_HHMM.getPattern());
        //设置线程异常未catch情况下默认处理方法
        Thread.setDefaultUncaughtExceptionHandler((thread, exception) -> {
            System.out.println(String.format("%s [%s] error Uncaught exception in thread %s: %s",
                    dateFormat.format(new Date()), thread.getContextClassLoader(),
                    thread.getName(), exception.getMessage()));
            exception.printStackTrace(System.out);
        });
        //创建 broker 启动器实例
        BrokerStarter starter = new BrokerStarter(args);
        //安装JVM退出时钩子，即调用 broker 启动器关闭
        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> {
                try {
                    starter.shutdown();
                } catch (Throwable t) {
                    log.error("Error while shutting down Pulsar service", t);
                }
            }, "pulsar-service-shutdown")
        );

        PulsarByteBufAllocator.registerOOMListener(oomException -> {
            if (starter.brokerConfig.isSkipBrokerShutdownOnOOM()) {
                log.error("-- Received OOM exception: {}", oomException.getMessage(), oomException);
            } else {
                log.error("-- Shutting down - Received OOM exception: {}", oomException.getMessage(), oomException);
                starter.pulsarService.shutdownNow();
            }
        });

        try {
            //调用启动器启动
            starter.start();
        } catch (Throwable t) {
            log.error("Failed to start pulsar service.", t);
            LogManager.shutdown();
            Runtime.getRuntime().halt(1);
        } finally {
            //等待启动器退出
            starter.join();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(PulsarBrokerStarter.class);
}

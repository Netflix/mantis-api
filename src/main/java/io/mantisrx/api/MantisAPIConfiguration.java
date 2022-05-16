/*
 * Copyright 2018 Netflix, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 */

package io.mantisrx.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.discovery.AbstractDiscoveryClientOptionalArgs;
import com.netflix.netty.common.accesslog.AccessLogPublisher;
import com.netflix.netty.common.metrics.EventLoopGroupMetrics;
import com.netflix.netty.common.status.ServerStatusManager;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.ThreadPoolMonitor;
import com.netflix.zuul.BasicRequestCompleteHandler;
import com.netflix.zuul.DefaultFilterFactory;
import com.netflix.zuul.DynamicCodeCompiler;
import com.netflix.zuul.DynamicFilterLoader;
import com.netflix.zuul.ExecutionStatus;
import com.netflix.zuul.FilterFactory;
import com.netflix.zuul.FilterLoader;
import com.netflix.zuul.FilterUsageNotifier;
import com.netflix.zuul.RequestCompleteHandler;
import com.netflix.zuul.context.SessionContextDecorator;
import com.netflix.zuul.context.ZuulSessionContextDecorator;
import com.netflix.zuul.filters.FilterRegistry;
import com.netflix.zuul.filters.MutableFilterRegistry;
import com.netflix.zuul.filters.ZuulFilter;
import com.netflix.zuul.groovy.GroovyCompiler;
import com.netflix.zuul.netty.server.ClientRequestReceiver;
import com.netflix.zuul.netty.server.DirectMemoryMonitor;
import com.netflix.zuul.origins.BasicNettyOrigin;
import com.netflix.zuul.origins.BasicNettyOriginManager;
import com.netflix.zuul.origins.OriginManager;
import com.netflix.zuul.stats.BasicRequestMetricsPublisher;
import com.netflix.zuul.stats.RequestMetricsPublisher;

import org.apache.commons.configuration.AbstractConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.mantisrx.api.services.artifacts.ArtifactManager;
import io.mantisrx.api.services.artifacts.InMemoryArtifactManager;
import io.mantisrx.api.tunnel.MantisCrossRegionalClient;
import io.mantisrx.api.tunnel.NoOpCrossRegionalClient;
import io.mantisrx.client.MantisClient;
import io.mantisrx.server.master.client.MasterClientWrapper;
import io.mantisrx.server.worker.client.WorkerMetricsClient;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.DeserializationFeature;
import io.mantisrx.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import rx.Scheduler;
import rx.schedulers.Schedulers;

@Configuration
@EnableAutoConfiguration
public class MantisAPIConfiguration {

    @Value("${spring.application.name}")
    private String appName;

    @Value("${mantis.zookeeper.root}")
    private String mantisZookeeperRoot;

    @Value("${mantis.zookeeper.connectString}")
    private String mantisZookeeperConnectString;

    @Value("${mantis.zookeeper.leader.announcement.path}")
    private String mantisZookeeperLeaderAnnouncementPath;

    @Value("${default.nfzookeeper.session-timeout-ms}")
    private String zkSessionTimeout;


    /*

    @Bean
    String getInstanceId() {
        return "";
    }

    @Bean
    InstanceInfo getInstanceInfo() {
        return new InstanceInfo(
                "local-instance-id",
                "mantisapi",
                "appGroupName?",
                "ipAddress",
                new PortWrapper(true, 1234), // port
                new PortWrapper(true, 4567), // securePort
                "homePageUrl",
                "statusPageUrl",
                "/healthcheck", // healthCheckUrl
                "/healthcheck", // secureHealthCheckUrl
                "mantisapi:port", // vipAddress
                "mantisapi:4567", // secureVipAddress
                "US",
                new DataCenterInfo(),
                "hostname",
                new InstanceStatus(),
                null, // OverrriddenStatus
                null, // OverrriddenStatusAlt
                new LeaseInfo.Builder().setDurationInSecs(60)
                    .setRenewalTimestamp(System.currentTimeMillis())
                    .setEvictionTimestamp(0)
                    .setServiceUpTimestamp(System.currentTimeMillis())
                    .setRenewalIntervalInSecs(30)
                    .setRegistrationTimestamp(System.currentTimeMillis())
                    .build()
                false, // isCoordinatingDiscoveryServer
                new HashMap<String, String>(), // metadata
                System.currentTimeMillis(), // lastUpdatedTimestamp
                System.currentTimeMillis(), // lastDirtyTimestamp
                ActionType.ADDED,
                "mantisapi-blah" // asgName
                );
    }

    @Bean
    EurekaInstanceConfig getEurekaInstanceConfig() {
        return null;
    }

    @Bean
    ApplicationInfoManager getOtheApplicationInfoManager(EurekaInstanceConfig config, InstanceInfo instanceInfo) {
        return new ApplicationInfoManager(config, instanceInfo);
    }

    @Bean
    EurekaClient getEurekaClient(ApplicationInfoManager applicationInfoManager, EurekaClientConfig eurekaClientConfig) {
        return new DiscoveryClient(applicationInfoManager, eurekaClientConfig);
    }

    @Bean
    ApplicationInfoManager getApplicationInfoManager() {
        System.out.println("CODY: " + ApplicationInfoManager.getInstance().getInfo());
        return ApplicationInfoManager.getInstance();
    }

    */
    @Bean
    ServerStatusManager getServerStatusManager(ApplicationInfoManager aim) {
        return new ServerStatusManager(aim);
    }

    @Bean
    FilterRegistry getFilterRegistry() {
        return new MutableFilterRegistry();
    }

    @Bean
    FilterLoader getFilterLoader(FilterRegistry filterRegistry, DynamicCodeCompiler compiler, FilterFactory filterFactory) {
        return new DynamicFilterLoader(filterRegistry, compiler, filterFactory);
    }

    @Bean
    DynamicCodeCompiler getDynamicCodeCompiler() {
        return new GroovyCompiler();
    }

    @Bean
    FilterFactory getFilterFactory() {
        return new DefaultFilterFactory(); // TODO: This may have been bound to GuiceFilterFactory, may need a Spring Boot Analogue.
    }

    @Bean
    SessionContextDecorator getSessionContextDecorator(OriginManager<BasicNettyOrigin> originManager) {
        return new ZuulSessionContextDecorator(originManager);
    }

    @Bean
    OriginManager<BasicNettyOrigin> getOriginManager(Registry registry) {
        return new BasicNettyOriginManager(registry);
    }

    @Bean
    Registry getRegistry() {
        return new DefaultRegistry();
    }

    @Bean
    ObjectMapper getObjectMapper() {
        return new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Bean
    FilterUsageNotifier getFilterUsageNotifier(Registry registry) {
        return new FilterUsageNotifier() {
			@Override
			public void notify(ZuulFilter<?, ?> filter, ExecutionStatus status) {
                registry.counter(
                        "zuul.filter-" + filter.getClass().getSimpleName(),
                        "status", status.name(),
                        "filtertype", filter.filterType().toString()).increment();
			}
            
        };
    }

    @Bean
    RequestCompleteHandler getRequestCompleteHandler() {
        return new BasicRequestCompleteHandler();
    }

    @Bean
    DirectMemoryMonitor getDirectMemoryMonitor(Registry registry) {
        return new DirectMemoryMonitor(registry);
    }

    @Bean
    EventLoopGroupMetrics getEventLoopGroupMetrics(Registry registry) {
        return new EventLoopGroupMetrics(registry);
    }
    /*

    @Bean
    EurekaClientConfig getEurekaClientConfig() {
        return new DefaultEurekaClientConfig();
    }
    */

    private class MyArgs extends AbstractDiscoveryClientOptionalArgs {
    }

    @Bean
    com.netflix.discovery.AbstractDiscoveryClientOptionalArgs<?> getNetflixAbstractDiscoveryClientOptionalArgs() {
        return new MyArgs();
    }

    @Bean
    RequestMetricsPublisher getRequestMetricsPublisher() {
        return new BasicRequestMetricsPublisher();
    }

    @Bean
    ArtifactManager getArtifactManager() {
        return new InMemoryArtifactManager();
    }

    @Bean
    MantisCrossRegionalClient getMantisCrossregionalClient() {
        return new NoOpCrossRegionalClient();
    }

    @Bean
    AccessLogPublisher getAccessLogPublisher() {
        return new AccessLogPublisher("ACCESS",
                (channel, httpRequest) -> ClientRequestReceiver.getRequestFromChannel(channel).getContext().getUUID());
    }

    // TODO: Properly fetch mantis.* properties for the next two
    // methods
    
    /*
    @Bean
    AbstractConfiguration getAbstractConfiguration() {
        return new DynamicURLConfiguration();
    }
    */

    @Bean
    MasterClientWrapper provideMantisClientWrapper() {

        Properties props = new Properties();

        props.put("mantis.zookeeper.root", mantisZookeeperRoot);
        props.put("mantis.zookeeper.connectString", mantisZookeeperConnectString);
        props.put("mantis.zookeeper.leader.announcement.path", mantisZookeeperLeaderAnnouncementPath);
        props.put("default.nfzookeeper.session-timeout-ms", zkSessionTimeout);

        return new MasterClientWrapper(props);
    }

    @Bean
    MantisClient provideMantisClient(AbstractConfiguration configuration) {
        Properties props = new Properties();

        props.put("mantis.zookeeper.root", mantisZookeeperRoot);
        props.put("mantis.zookeeper.connectString", mantisZookeeperConnectString);
        props.put("mantis.zookeeper.leader.announcement.path", mantisZookeeperLeaderAnnouncementPath);
        props.put("default.nfzookeeper.session-timeout-ms", zkSessionTimeout);

        return new MantisClient(props);
    }

    @Bean("io-scheduler")
    Scheduler provideIoScheduler(Registry registry) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(16, 128, 60,
                TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        ThreadPoolMonitor.attach(registry, executor, "io-thread-pool");
        return Schedulers.from(executor);
    }

    @Bean
    WorkerMetricsClient provideWorkerMetricsClient(AbstractConfiguration configuration) {
        Properties props = new Properties();

        props.put("mantis.zookeeper.root", mantisZookeeperRoot);
        props.put("mantis.zookeeper.connectString", mantisZookeeperConnectString);
        props.put("mantis.zookeeper.leader.announcement.path", mantisZookeeperLeaderAnnouncementPath);
        props.put("default.nfzookeeper.session-timeout-ms", zkSessionTimeout);

        return new WorkerMetricsClient(props);
    }

    @Bean("push-prefixes")
    List<String> providePushPrefixes() {
        List<String> pushPrefixes = new ArrayList<>(20);
        pushPrefixes.add("/jobconnectbyid");
        pushPrefixes.add("/api/v1/jobconnectbyid");
        pushPrefixes.add("/jobconnectbyname");
        pushPrefixes.add("/api/v1/jobconnectbyname");
        pushPrefixes.add("/jobsubmitandconnect");
        pushPrefixes.add("/api/v1/jobsubmitandconnect");
        pushPrefixes.add("/jobClusters/discoveryInfoStream");
        pushPrefixes.add("/jobstatus");
        pushPrefixes.add("/api/v1/jobstatus");
        pushPrefixes.add("/api/v1/jobs/schedulingInfo/");
        pushPrefixes.add("/api/v1/metrics");

        return pushPrefixes;
    }
}

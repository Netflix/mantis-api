/**
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mantisrx.api.services;

import com.google.inject.Inject;
import com.netflix.config.DynamicStringProperty;
import com.netflix.mantis.discovery.proto.AppJobClustersMap;
import com.netflix.spectator.api.Counter;
import com.netflix.zuul.netty.SpectatorUtils;
import io.mantisrx.api.proto.AppDiscoveryMap;
import io.mantisrx.api.util.JacksonObjectMapper;
import io.mantisrx.client.MantisClient;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.vavr.control.Either;
import io.vavr.control.Option;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class AppStreamDiscoveryService {

    private static final String APP_JOB_CLUSTER_MAPPING_KEY = "mreAppJobClusterMap";

    private final AtomicReference<AppJobClustersMap> appJobClusterMappings = new AtomicReference<>();

    private final MantisClient mantisClient;
    private final Counter appJobClusterMappingNullCount;
    private final Counter appJobClusterMappingRequestCount;
    private final Counter appJobClusterMappingFailCount;

    private final DynamicStringProperty appJobClustersProp = new DynamicStringProperty("mreAppJobClusterMap", "");

    @Inject
    public AppStreamDiscoveryService(MantisClient mantisClient) {
        this.mantisClient = mantisClient;
        this.appJobClusterMappingNullCount = SpectatorUtils.newCounter("appJobClusterMappingNull", "");
        this.appJobClusterMappingRequestCount = SpectatorUtils.newCounter("appJobClusterMappingRequest", "", "app", "unknown");
        this.appJobClusterMappingFailCount = SpectatorUtils.newCounter("appJobClusterMappingFail", "");

        updateAppJobClustersMapping(appJobClustersProp.get());
        appJobClustersProp.addCallback(() -> updateAppJobClustersMapping(appJobClustersProp.get()));
    }

    private void updateAppJobClustersMapping(String appJobClusterStr) {
        if (appJobClusterStr != null) {
            try {
                AppJobClustersMap appJobClustersMap = JacksonObjectMapper.getInstance()
                        .readValue(appJobClusterStr, AppJobClustersMap.class);
                log.info("appJobClustersMap updated to {}", appJobClustersMap);
                appJobClusterMappings.set(appJobClustersMap);
            } catch (Exception ioe) {
                log.error("failed to update appJobClustersMap on Property update {}", appJobClusterStr, ioe);
                appJobClusterMappingFailCount.increment();
            }
        } else {
            log.error("appJobCluster mapping property is NULL");
            appJobClusterMappingNullCount.increment();
        }
    }


    public Either<String, AppDiscoveryMap> getAppDiscoveryMap(List<String> appNames) {
        try {
            AppJobClustersMap appJobClustersMap = appJobClusterMappings.get();
            if (appJobClustersMap == null) {
                log.error("appJobCluster Mapping is null");
                appJobClusterMappingNullCount.increment();
                return Either.left("appJobCluster Mapping is null.");
            }

            AppJobClustersMap appJobClusters = getAppJobClustersMap(appNames, appJobClustersMap);

            //
            // Lookup discovery info per stream and build mapping
            //

            AppDiscoveryMap adm = new AppDiscoveryMap(appJobClusters.getVersion(), appJobClusters.getTimestamp());

            for (String app : appJobClusters.getMappings().keySet()) {
                for (String stream : appJobClusters.getMappings().get(app).keySet()) {
                    String jobCluster = appJobClusters.getMappings().get(app).get(stream);
                    Option<JobSchedulingInfo> jobSchedulingInfo = getJobDiscoveryInfo(jobCluster);
                    jobSchedulingInfo.map(jsi -> {
                        adm.addMapping(app, stream, jsi);
                        return jsi;
                    });
                }
            }
            return Either.right(adm);
        } catch (Exception ex) {
            log.error(ex.getMessage());
            return Either.left(ex.getMessage());
        }
    }

    public AppJobClustersMap getAppJobClustersMap(List<String> appNames) {
        return this.getAppJobClustersMap(appNames, this.appJobClusterMappings.get());
    }

    public AppJobClustersMap getAppJobClustersMap(List<String> appNames, AppJobClustersMap appJobClustersMap) {
        AppJobClustersMap appJobClusters;

        if (appNames.size() > 0) {
            //appNames.forEach(app -> SpectatorUtils.buildAndRegisterCounter(registry, "appJobClusterMappingRequest", "app", app).increment());
            appJobClusters = appJobClustersMap.getFilteredAppJobClustersMap(appNames);
        } else {
            appJobClusterMappingRequestCount.increment();
            appJobClusters = appJobClustersMap;
        }
        return appJobClusters;
    }

    // TODO: Does this block?
    private Option<JobSchedulingInfo> getJobDiscoveryInfo(String jobCluster) {
        JobDiscoveryService jdim = JobDiscoveryService.getInstance(mantisClient);
        return jdim
                .jobDiscoveryInfoStream(jdim.key(JobDiscoveryService.LookupType.JOB_CLUSTER, jobCluster))
                .map(Option::of)
                .take(1)
                .timeout(2, TimeUnit.SECONDS, Observable.just(Option.none()))
                .doOnError((t) -> {
                    log.warn("Timed out looking up job discovery info for cluster: " + jobCluster + ".");
                })
                .toSingle()
                .toBlocking()
                .value();
    }
}

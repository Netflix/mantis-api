/*
 * Copyright 2019 Netflix, Inc.
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

package io.mantisrx.api;

/**
 * Contains the string names of all the Archaius properties used within the application.
 * This serves two purposes;
 * 1) Document the names of all dynamic properties effecting the behavior of the application.
 * 2) Allow users to follow property usage via IDE "show usages" feature.
 */
public class PropertyNames {

    public static final String mantisAPIWorkerThreads = "mantisapi.worker.threads";
    public static final String mantisAPIReadOnly = "mantisapi.readonly";

    public static final String inactiveTimeoutSecs = "mantisapi.connection.inactive.timeout.secs";

    public static final String mantisAPICacheExpirySeconds = "mantisapi.cache.expiry.secs";
    public static final String mantisAPICacheSize = "mantisapi.cache.size";
    public static final String mantisAPICacheEnabled = "mantisapi.cache.enabled";
    public static final String artifactRepositoryLocation = "mantisapi.artifact.disk.cache.location";
    public static final String artifactCacheEnabled = "mantisapi.artifact.disk.cache.enabled";
    public static final String mantisLatestJobDiscoveryInfoEndpointEnabled = "mantis.latest.job.discovery.info.get.enabled";
    public static final String mantisLatestJobDiscoveryCacheTtlMs = "mantis.latest.job.discovery.info.cache.ttl.ms";
    public static final String mantisLatestJobDiscoveryMasterTimeoutMs = "mantis.latest.job.discovery.info.master.timeout.ms";

    public static final String samplingInterval = "mantisapi.job.sink.connector.source.job.min.sample.interval.millis";

    public static final String mantisAPIRegions = "mantisapi.regions.list";

    public static final String healthCheckMinHeapBytes = "mantisapi.healthcheck.heapsize.min";

    public static final String sourceSamplingEnabled = "mantisapi.job.sink.connector.source.sampling.enabled";

    public static final String crossRegionCacheEnabled = "mantisapi.crossregion.cache.enabled";
    public static final String crossRegionCacheSize = "mantisapi.crossregion.cache.size";
    public static final String crossRegionCacheExpirySeconds = "mantisapi.crossregion.cache.expiry.secs";

    public static final String websocketTimeout = "mantisapi.websocket.idleTimeout";

    // DOS Filter
    public static final String ipWhiteList = "mantisapi.throttling.ipWhitelist";
    public static final String delayMs = "mantisapi.throttling.delayMs";
    public static final String maxRequestsPerSec = "mantisapi.throttling.maxRequestsPerSec";

    // IP Blacklist
    public static final String ipBlackList = "mantisapi.throttling.ipBlacklist";
}

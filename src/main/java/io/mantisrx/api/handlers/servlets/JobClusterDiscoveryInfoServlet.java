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

package io.mantisrx.api.handlers.servlets;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.spectator.api.Registry;
import io.mantisrx.api.PropertyNames;
import io.mantisrx.api.SessionContext;
import io.mantisrx.api.SessionContextBuilder;
import io.mantisrx.api.SpectatorUtils;
import io.mantisrx.api.WorkerThreadPool;
import io.mantisrx.api.handlers.utils.HttpUtils;
import io.mantisrx.api.handlers.utils.JobDiscoveryInfoManager;
import io.mantisrx.api.handlers.utils.JobDiscoveryLookupKey;
import io.mantisrx.api.handlers.utils.MantisClientUtil;
import io.mantisrx.api.handlers.utils.PathUtils;
import io.mantisrx.client.MantisClient;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.mantisrx.server.core.json.DefaultObjectMapper;
import io.mantisrx.server.master.client.MasterClientWrapper;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;



/**
 * curl "http://<host>:<port>/jobClusters/discoveryInfo/<jobCluster>"
 */
public class JobClusterDiscoveryInfoServlet extends HttpServlet {

    private static final long serialVersionUID = JobClusterDiscoveryInfoServlet.class.hashCode();

    private static Logger logger = LoggerFactory.getLogger(JobClusterDiscoveryInfoServlet.class);
    private transient final MasterClientWrapper masterClientWrapper;
    private transient final MantisClient mantisClient;
    public static final String endpointName = "jobClusters/discoveryInfo";
    public static final String helpMsg = endpointName + "/<JobCluster>";
    private transient final Registry registry;
    private transient final WorkerThreadPool workerThreadPool;
    private transient final PropertyRepository propertyRepository;
    /**
     * Fast property to toggle between the old SSE based job discovery info lookup which would establish two long lived connections
     * and the new request response endpoint to retrieve the discovery info with one API call
     */
    private transient final Property<Boolean> latestJobDiscoveryInfoEndpointEnabled;
    private transient final Property<Integer> cacheExpiryTtlMillis;
    private transient final Property<Integer> masterTimeoutMillis;
    private final LoadingCache<String, Tuple2<HttpResponseStatus, Optional<JobSchedulingInfo>>> discoveryInfoCache;

    private static final String JOB_CLUSTER_LATEST_JOB_DISCOVERY_ENDPOINT = "/api/v1/jobClusters/%s/latestJobDiscoveryInfo";
    private static final ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false).registerModule(new Jdk8Module());

    public JobClusterDiscoveryInfoServlet(MasterClientWrapper masterClientWrapper,
                                          MantisClient mantisClient,
                                          PropertyRepository propertyRepository,
                                          Registry registry,
                                          WorkerThreadPool workerThreadPool) {
        this.masterClientWrapper = masterClientWrapper;
        this.mantisClient = mantisClient;
        this.registry = registry;
        this.workerThreadPool = workerThreadPool;
        this.propertyRepository = propertyRepository;
        this.latestJobDiscoveryInfoEndpointEnabled = propertyRepository.get(PropertyNames.mantisLatestJobDiscoveryInfoEndpointEnabled, Boolean.class).orElse(true);
        this.cacheExpiryTtlMillis = propertyRepository.get(PropertyNames.mantisLatestJobDiscoveryCacheTtlMs, Integer.class).orElse(250);
        this.masterTimeoutMillis = propertyRepository.get(PropertyNames.mantisLatestJobDiscoveryMasterTimeoutMs, Integer.class).orElse(1000);

        Integer cacheTtlMillis = cacheExpiryTtlMillis.get();
        logger.info("cache TTL {}", cacheTtlMillis);
        discoveryInfoCache = Caffeine.newBuilder()
            .expireAfterWrite(cacheTtlMillis, TimeUnit.MILLISECONDS)
            .maximumSize(500)
            .build(this::jobSchedInfo);
    }

    private Tuple2<HttpResponseStatus, Optional<JobSchedulingInfo>> jobSchedInfo(String jobCluster) {
        String uri = String.format(JOB_CLUSTER_LATEST_JOB_DISCOVERY_ENDPOINT, jobCluster);

        try {
            Tuple2<HttpResponseStatus, Optional<JobSchedulingInfo>> resp = MantisClientUtil
                .callGetOnMaster(masterClientWrapper.getMasterMonitor().getMasterObservable(), uri, registry)
                .flatMap(mr -> {
                    HttpResponseStatus status = mr.getStatus();
                    return mr.getByteBuf()
                        .map(bb -> {
                            Optional<JobSchedulingInfo> jsi = Optional.empty();
                            try {
                                String responseContent = bb.toString(Charsets.UTF_8);
                                logger.debug("master response content {}", responseContent);
                                if (status.code() == 200 && !Strings.isNullOrEmpty(responseContent)) {
                                    JobSchedulingInfo jobSchedulingInfo = DefaultObjectMapper.getInstance().readValue(responseContent, JobSchedulingInfo.class);
                                    jsi = Optional.ofNullable(jobSchedulingInfo);
                                    SpectatorUtils.buildAndRegisterCounter(registry, "jobClusterDiscoveryInfoCacheRefreshSuccess", "endpoint", endpointName, "jobCluster", jobCluster).increment();
                                } else {
                                    logger.info("failed to get JobSchedulingInfo for {} MantisMaster response (status={}, content={})", jobCluster, status.code(), responseContent);
                                    SpectatorUtils.buildAndRegisterCounter(registry, "jobClusterDiscoveryCacheRefreshFailed", "endpoint", endpointName,
                                                                           "reason", "status_"+status.code(), "jobCluster", jobCluster).increment();
                                }
                            } catch (IOException e) {
                                logger.info("failed to parse JobSchedulingInfo for {} MantisMaster response (status={}, content={})", uri, status.code(), bb.toString(Charsets.UTF_8));
                                SpectatorUtils.buildAndRegisterCounter(registry, "jobClusterDiscoveryCacheRefreshFailed", "endpoint", endpointName,
                                                                       "reason", "exc_"+e.getClass().getSimpleName(), "jobCluster", jobCluster).increment();
                            }
                            return Tuple.of(status, jsi);
                        });
                })
                .timeout(masterTimeoutMillis.get(), TimeUnit.MILLISECONDS, Observable.error(new Exception("timed out waiting for JobSchedulingInfo from MantisMaster")))
                .doOnError((t) -> {
                    logger.warn("Timed out getting discovery info " + uri);
                    SpectatorUtils.buildAndRegisterCounter(registry, "jobClusterDiscoveryCacheRefreshFailed",
                                                           "reason", "TimedOut", "jobCluster", jobCluster).increment();
                    return;
                })
                .toBlocking()
                .first();
            return resp;
        } catch (Exception e) {
            logger.info("caught exception getting discovery info for {}", jobCluster, e);
            SpectatorUtils.buildAndRegisterCounter(registry, "jobClusterDiscoveryCacheRefreshFailed", "endpoint", endpointName,
                                                   "reason", "exc_" + e.getClass().getSimpleName(), "jobCluster", jobCluster).increment();
            return Tuple.of(HttpResponseStatus.INTERNAL_SERVER_ERROR, Optional.empty());
        }
    }


    @Override
    public void doOptions(HttpServletRequest request, HttpServletResponse response) {
        HttpUtils.addBaseHeaders(response, "GET", "OPTIONS");
        response.setStatus(HttpServletResponse.SC_NO_CONTENT);
    }

    private SessionContext getSessionContext(HttpServletRequest request) {
        final SessionContextBuilder contextBuilder = SessionContextBuilder.getInstance(propertyRepository, registry, workerThreadPool);
        return contextBuilder
            .createHttpSessionCtx(request.getRemoteAddr(),
                                  request.getRequestURI() + "?" + request.getQueryString(),
                                  request.getMethod());
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) {
        if (latestJobDiscoveryInfoEndpointEnabled.get()) {
            latestJobDiscoveryInfoEndpointGET(request, response);
        } else {
            sseNamedJobInfoAndAssignmentResultsGET(request, response);
        }
    }

    private void WithJobClusterAndSessionContext(HttpServletRequest request, HttpServletResponse response, BiConsumer<String, SessionContext> jobClusterFn) {
        final SessionContext httpSessionCtx = getSessionContext(request);

        String jobCluster = PathUtils.getTokenAfter(request.getPathInfo(), "");
        if (jobCluster == null || jobCluster.isEmpty()) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            try {
                response.getOutputStream().print("Job cluster not specified");
                response.flushBuffer();
            } catch (Exception e) {
                logger.warn("Couldn't write message (\"Job cluster not specified\") to client session {}",
                            httpSessionCtx.getId(), e);
            }
            SpectatorUtils.buildAndRegisterCounter(registry, "jobClusterDiscoveryInfoFailed", "endpoint", endpointName, "reason", "jobClusterNullOrEmpty").increment();
            httpSessionCtx.endSession();
            return;
        }
        SpectatorUtils.buildAndRegisterCounter(registry, "jobClusterDiscoveryInfoRequest", "endpoint", endpointName, "jobCluster", jobCluster).increment();

        jobClusterFn.accept(jobCluster, httpSessionCtx);
    }

    private void sseNamedJobInfoAndAssignmentResultsGET(HttpServletRequest request, HttpServletResponse response) {
        WithJobClusterAndSessionContext(request, response, (jobCluster, httpSessionCtx) -> {

            String jobDiscoveryInfo = JobDiscoveryInfoManager.getInstance(mantisClient, registry)
                .jobDiscoveryInfoStream(new JobDiscoveryLookupKey(JobDiscoveryLookupKey.LookupType.JOB_CLUSTER, jobCluster))
                .map((jSchedInfo) -> {
                    try {
                        return mapper.writeValueAsString(jSchedInfo);
                    } catch (Exception e) {
                        logger.info("failed to serialize job discovery info {}", jSchedInfo);
                        SpectatorUtils.buildAndRegisterCounter(registry, "jobClusterDiscoveryInfoFailed", "endpoint", endpointName,
                                                               "reason", "SerializationFailed", "jobCluster", jobCluster).increment();
                        return null;
                    }

                })
                .filter(Objects::nonNull)
                .take(1)
                .timeout(2, TimeUnit.SECONDS, Observable.error(new Exception("timed out waiting for JobSchedulingInfo")))
                .doOnError((t) -> {
                    logger.warn("Timed out relaying request for session id " + httpSessionCtx.getId() +
                                    " url=" + request.getRequestURI());
                    SpectatorUtils.buildAndRegisterCounter(registry, "jobClusterDiscoveryInfoFailed", "endpoint", endpointName,
                                                           "reason", "TimedOut", "jobCluster", jobCluster).increment();
                    response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                    try {
                        response.getOutputStream().print(t.getMessage());
                        response.flushBuffer();
                    } catch (IOException e) {
                        logger.error("Couldn't write message ({}) to client session {}", t.getMessage(), httpSessionCtx.getId(), e);
                        httpSessionCtx.endSession();
                    }
                    httpSessionCtx.endSession();
                    return;
                })
                .toSingle()
                .toBlocking()
                .value();

            response.setStatus(HttpServletResponse.SC_OK);
            try {
                response.getOutputStream().print(jobDiscoveryInfo);
                response.flushBuffer();
            } catch (IOException e) {
                logger.warn("caught exc sending job discovery info for {}", jobCluster, e);
            }

            httpSessionCtx.endSession();
        });
    }

    private void latestJobDiscoveryInfoEndpointGET(HttpServletRequest request, HttpServletResponse response) {
        WithJobClusterAndSessionContext(request, response, (jobCluster, httpSessionCtx) -> {
            try {
                String uri = request.getRequestURI();
                if (request.getQueryString() != null && !request.getQueryString().isEmpty())
                    uri = uri + "?" + request.getQueryString();
                logger.debug("JobCluster discovery info request " + uri);
                // lookup from loading cache
                Tuple2<HttpResponseStatus, Optional<JobSchedulingInfo>> result = discoveryInfoCache.get(jobCluster);
                if (result != null) {
                    HttpResponseStatus status = result._1;
                    Optional<JobSchedulingInfo> jsi = result._2;
                    if (jsi.isPresent()) {
                        HttpUtils.addBaseHeaders(response, "GET");
                        String serializedSchedInfo = mapper.writeValueAsString(jsi.get());
                        SpectatorUtils.buildAndRegisterCounter(registry, "jobClusterDiscoveryInfoSuccess", "endpoint", endpointName, "jobCluster", jobCluster).increment();

                        response.setStatus(HttpResponseStatus.OK.code());
                        response.getOutputStream().print(serializedSchedInfo);
                        response.flushBuffer();

                    } else {
                        SpectatorUtils.buildAndRegisterCounter(registry, "jobClusterDiscoveryInfoFailed", "endpoint", endpointName, "reason", "JobSchedulingInfoEmpty").increment();
                        response.setStatus(status.code());
                        response.getOutputStream().print("job discovery info lookup failed for "+jobCluster);
                        response.flushBuffer();
                    }
                } else {
                    SpectatorUtils.buildAndRegisterCounter(registry, "jobClusterDiscoveryInfoFailed", "endpoint", endpointName, "reason", "CacheLookupFailed").increment();
                    logger.info("failed to lookup job discovery info for {}", jobCluster);
                    response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                    response.getOutputStream().print("error getting job discovery info for "+jobCluster);
                    response.flushBuffer();
                }
            } catch (IOException e) {
                SpectatorUtils.buildAndRegisterCounter(registry, "jobClusterDiscoveryInfoFailed", "endpoint", endpointName, "reason", "exc_"+e.getClass().getSimpleName()).increment();
                logger.warn("caught exception handling request for job discovery info {}", jobCluster, e);
            } finally {
                httpSessionCtx.endSession();
            }
        });
    }
}

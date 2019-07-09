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
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import io.mantisrx.api.SessionContext;
import io.mantisrx.api.SessionContextBuilder;
import io.mantisrx.api.SpectatorUtils;
import io.mantisrx.api.WorkerThreadPool;
import io.mantisrx.api.handlers.utils.HttpUtils;
import io.mantisrx.api.handlers.utils.JobDiscoveryInfoManager;
import io.mantisrx.api.handlers.utils.JobDiscoveryLookupKey;
import io.mantisrx.api.handlers.utils.PathUtils;
import io.mantisrx.client.MantisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;


/**
 * curl "http://<host>:<port>/jobClusters/discoveryInfo/<jobCluster>"
 */
public class JobClusterDiscoveryInfoServlet extends HttpServlet {

    private static final long serialVersionUID = JobClusterDiscoveryInfoServlet.class.hashCode();

    @SuppressWarnings("unused")
    private static Logger logger = LoggerFactory.getLogger(JobClusterDiscoveryInfoServlet.class);
    private transient final MantisClient mantisClient;
    public static final String endpointName = "jobClusters/discoveryInfo";
    public static final String helpMsg = endpointName + "/<JobCluster>";
    private final ObjectMapper mapper = new ObjectMapper();
    private transient final Registry registry;
    private transient final WorkerThreadPool workerThreadPool;
    private transient final PropertyRepository propertyRepository;


    public JobClusterDiscoveryInfoServlet(MantisClient mantisClient, PropertyRepository propertyRepository, Registry registry, WorkerThreadPool workerThreadPool) {
        this.mantisClient = mantisClient;
        this.registry = registry;
        this.workerThreadPool = workerThreadPool;
        this.propertyRepository = propertyRepository;
    }


    @Override
    public void doOptions(HttpServletRequest request, HttpServletResponse response) {
        HttpUtils.addBaseHeaders(response, "GET", "OPTIONS");
        response.setStatus(HttpServletResponse.SC_NO_CONTENT);
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) {
        final SessionContextBuilder contextBuilder = SessionContextBuilder.getInstance(propertyRepository, registry, workerThreadPool);
        final SessionContext httpSessionCtx = contextBuilder.createHttpSessionCtx(request.getRemoteAddr(),
                request.getRequestURI() + "?" + request.getQueryString(), request.getMethod());

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
            e.printStackTrace();
        }

        httpSessionCtx.endSession();
    }
}

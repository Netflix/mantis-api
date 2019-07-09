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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.mantis.discovery.proto.AppJobClustersMap;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import io.mantisrx.api.SessionContext;
import io.mantisrx.api.SessionContextBuilder;
import io.mantisrx.api.SpectatorUtils;
import io.mantisrx.api.WorkerThreadPool;
import io.mantisrx.api.handlers.utils.HttpUtils;
import io.mantisrx.api.handlers.utils.JacksonObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MREAppStreamToJobClusterMappingServlet extends HttpServlet {

    private static Logger logger = LoggerFactory.getLogger(MREAppStreamToJobClusterMappingServlet.class);
    private static final long serialVersionUID = MREAppStreamToJobClusterMappingServlet.class.hashCode();

    private static final String APP_JOB_CLUSTER_MAPPING_KEY = "mreAppJobClusterMap";
    private static final String APPNAME_QUERY_PARAM = "app";

    public static final String PATH_SPEC = "/api/v1/mantis/publish/streamJobClusterMap";
    private transient final WorkerThreadPool workerThreadPool;
    private transient final Registry registry;
    private transient final PropertyRepository propertyRepository;

    private final Counter appJobClusterMappingNullCount;
    private final Counter appJobClusterMappingFailCount;
    private final Counter appJobClusterMappingRequestCount;
    private final AtomicReference<AppJobClustersMap> appJobClusterMappings = new AtomicReference<>();

    public MREAppStreamToJobClusterMappingServlet(PropertyRepository propertyRepository, Registry registry, WorkerThreadPool workerThreadPool) {
        this.workerThreadPool = workerThreadPool;
        this.registry = registry;
        this.propertyRepository = propertyRepository;
        this.appJobClusterMappingNullCount = registry.counter("appJobClusterMappingNull");
        this.appJobClusterMappingFailCount = registry.counter("appJobClusterMappingFail");
        this.appJobClusterMappingRequestCount = registry.counter("appJobClusterMappingRequest", "app", "unknown");
        Property<String> appJobClustersProp = propertyRepository.get(APP_JOB_CLUSTER_MAPPING_KEY, String.class);
        updateAppJobClustersMapping(appJobClustersProp.get());
        appJobClustersProp.subscribe(appJobClusterStr -> updateAppJobClustersMapping(appJobClusterStr));
    }


    private void updateAppJobClustersMapping(String appJobClusterStr) {
        if (appJobClusterStr != null) {
            try {
                AppJobClustersMap appJobClustersMap = JacksonObjectMapper.getInstance()
                        .readValue(appJobClusterStr, AppJobClustersMap.class);
                logger.info("appJobClustersMap updated to {}", appJobClustersMap);
                appJobClusterMappings.set(appJobClustersMap);
            } catch (Exception ioe) {
                logger.error("failed to update appJobClustersMap on Property update {}", appJobClusterStr, ioe);
                appJobClusterMappingFailCount.increment();
            }
        } else {
            logger.error("appJobCluster mapping property is NULL");
            appJobClusterMappingNullCount.increment();
        }
    }

    @Override
    public void doOptions(HttpServletRequest request, HttpServletResponse response) {
        HttpUtils.addBaseHeaders(response, "GET", "OPTIONS");
        response.setStatus(HttpServletResponse.SC_NO_CONTENT);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) {
        final SessionContextBuilder contextBuilder = SessionContextBuilder.getInstance(propertyRepository, registry, workerThreadPool);
        final SessionContext httpSessionCtx = contextBuilder.createHttpSessionCtx(request.getRemoteAddr(),
                request.getRequestURI() + "?" + request.getQueryString(), request.getMethod());
        Map<String, List<String>> qp = HttpUtils.getQryParams(request.getQueryString());
        try {
            HttpUtils.addBaseHeaders(response, "GET", "OPTIONS");
            try {
                AppJobClustersMap appJobClustersMap = appJobClusterMappings.get();
                if (appJobClustersMap == null) {
                    logger.error("appJobCluster Mapping is null");
                    appJobClusterMappingNullCount.increment();
                    response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                    return;
                }
                AppJobClustersMap appJobClusters;
                if (qp.containsKey(APPNAME_QUERY_PARAM) && qp.get(APPNAME_QUERY_PARAM) != null) {
                    List<String> appNames = qp.get(APPNAME_QUERY_PARAM);
                    appNames.forEach(app -> SpectatorUtils.buildAndRegisterCounter(registry, "appJobClusterMappingRequest", "app", app).increment());
                    appJobClusters = appJobClustersMap.getFilteredAppJobClustersMap(qp.get(APPNAME_QUERY_PARAM));
                } else {
                    appJobClusterMappingRequestCount.increment();
                    appJobClusters = appJobClustersMap;
                }
                response.getOutputStream().print(JacksonObjectMapper.getInstance().writeValueAsString(appJobClusters));
                response.setContentType("application/json");
                response.setStatus(HttpServletResponse.SC_OK);
            } catch (Exception e) {
                logger.error("caught exception", e);
                appJobClusterMappingFailCount.increment();
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            }
        } finally {
            httpSessionCtx.endSession();
        }
    }
}

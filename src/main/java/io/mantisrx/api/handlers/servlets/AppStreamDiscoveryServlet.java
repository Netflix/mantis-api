package io.mantisrx.api.handlers.servlets;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.mantis.discovery.proto.AppJobClustersMap;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import io.mantisrx.api.SessionContext;
import io.mantisrx.api.SessionContextBuilder;
import io.mantisrx.api.SpectatorUtils;
import io.mantisrx.api.WorkerThreadPool;
import io.mantisrx.api.handlers.domain.AppDiscoveryMap;
import io.mantisrx.api.handlers.utils.HttpUtils;
import io.mantisrx.api.handlers.utils.JacksonObjectMapper;
import io.mantisrx.api.handlers.utils.JobDiscoveryInfoManager;
import io.mantisrx.api.handlers.utils.JobDiscoveryLookupKey;
import io.mantisrx.client.MantisClient;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.vavr.control.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;


public class AppStreamDiscoveryServlet extends HttpServlet {

    private static final Logger logger = LoggerFactory.getLogger(AppStreamDiscoveryServlet.class);
    private static final long serialVersionUID = AppStreamDiscoveryServlet.class.hashCode();
    private static final ObjectMapper mapper = new ObjectMapper();

    private static final String APP_JOB_CLUSTER_MAPPING_KEY = "mreAppJobClusterMap";
    private static final String APPNAME_QUERY_PARAM = "app";

    public static final String PATH_SPEC = "/api/v1/mantis/publish/streamDiscovery";
    private transient final WorkerThreadPool workerThreadPool;
    private transient final Registry registry;
    private transient final PropertyRepository propertyRepository;
    private transient final MantisClient mantisClient;

    private final Counter appJobClusterMappingNullCount;
    private final Counter appJobClusterMappingFailCount;
    private final Counter appJobClusterMappingRequestCount;

    private final AtomicReference<AppJobClustersMap> appJobClusterMappings = new AtomicReference<>();

    public AppStreamDiscoveryServlet(PropertyRepository propertyRepository, Registry registry, WorkerThreadPool workerThreadPool, MantisClient mantisClient) {
        this.workerThreadPool = workerThreadPool;
        this.registry = registry;
        this.propertyRepository = propertyRepository;
        this.mantisClient = mantisClient;
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

                response.getOutputStream().print(JacksonObjectMapper.getInstance().writeValueAsString(adm));
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

    private Option<JobSchedulingInfo> getJobDiscoveryInfo(String jobCluster) {
        return JobDiscoveryInfoManager.getInstance(mantisClient, registry)
                .jobDiscoveryInfoStream(new JobDiscoveryLookupKey(JobDiscoveryLookupKey.LookupType.JOB_CLUSTER, jobCluster))
                .map(Option::of)
                .take(1)
                .timeout(2, TimeUnit.SECONDS, Observable.just(Option.none()))
                .doOnError((t) -> {
                    logger.warn("Timed out looking up job discovery info for cluster: " + jobCluster + ".");
                })
                .toSingle()
                .toBlocking()
                .value();
    }
}

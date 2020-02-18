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

import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;

import com.netflix.spectator.api.Registry;

import io.mantisrx.api.handlers.servlets.AppStreamDiscoveryServlet;
import io.mantisrx.client.MantisClient;

import io.mantisrx.server.master.client.MantisMasterClientApi;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.ForwardedRequestCustomizer;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.NCSARequestLog;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.InetAccessHandler;

import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import javax.servlet.DispatcherType;

import io.mantisrx.server.master.client.MasterClientWrapper;

import io.vavr.Tuple2;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import javax.net.ssl.SSLContext;

import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.eclipse.jetty.servlets.DoSFilter;

import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mantisrx.api.filters.CustomInetAccessHandler;
import io.mantisrx.api.filters.ReadOnlyFilter;
import io.mantisrx.api.handlers.connectors.RemoteSinkConnector;
import io.mantisrx.api.handlers.servlets.ArtifactUploadServlet;
import io.mantisrx.api.handlers.servlets.HealthCheckServlet;
import io.mantisrx.api.handlers.servlets.HelpHandlerServlet;
import io.mantisrx.api.handlers.servlets.JobClusterDiscoveryInfoServlet;
import io.mantisrx.api.handlers.servlets.JobClusterDiscoveryInfoWebSocketServlet;
import io.mantisrx.api.handlers.servlets.JobConnectByIdWebSocketServlet;
import io.mantisrx.api.handlers.servlets.JobConnectByNameWebSocketServlet;
import io.mantisrx.api.handlers.servlets.JobSchedulingInfoServlet;
import io.mantisrx.api.handlers.servlets.JobStatusWebSocketServlet;
import io.mantisrx.api.handlers.servlets.JobSubmitAndConnectServlet;
import io.mantisrx.api.handlers.servlets.MREAppStreamToJobClusterMappingServlet;
import io.mantisrx.api.handlers.servlets.MantisMasterEndpointRelayServlet;
import io.mantisrx.api.handlers.servlets.RegionsHandlerWebSocketServlet;
import io.mantisrx.api.handlers.servlets.StatsHandlerServlet;
import io.mantisrx.api.tunnel.StreamingClientFactory;


public class MantisAPIServer {

    private static Logger logger = LoggerFactory.getLogger(MantisAPIServer.class);
    private final int serverPort;
    private final Server server;
    private final int sslPort;
    private final MantisClient mantisClient;

    public MantisAPIServer(int port, int sslPort, MantisClient mantisClient, MasterClientWrapper masterClientWrapper, MantisMasterClientApi mantisMasterClientApi, SSLContext sslContext, RemoteSinkConnector remoteSinkConnector, StreamingClientFactory streamingClientFactory, PropertyRepository propertyRepository, Registry registry, WorkerThreadPool workerThreadPool, ArtifactManager artifatManager, List<Tuple2<String, ServletHolder>> servlets) {
        this.serverPort = port;
        this.sslPort = sslPort;
        this.mantisClient = mantisClient;

        final Property<Integer> queueCapacity = propertyRepository.get("mantisapi.jetty.queueCapacity", Integer.class).orElse(2000);
        final Property<Integer> jettyThreads = propertyRepository.get("mantisapi.jetty.threads", Integer.class).orElse(512);
        final Property<Integer> jettyIdleTimeout = propertyRepository.get("mantisapi.jetty.idleTimeout", Integer.class).orElse(1000 * 60 * 30);

        ServletContextHandler servletContextHandler = new ServletContextHandler();

        BlockingArrayQueue<Runnable> requestQueue = new BlockingArrayQueue<>(queueCapacity.get());
        QueuedThreadPool threadPool = new QueuedThreadPool(jettyThreads.get(), jettyThreads.get(), jettyIdleTimeout.get(), requestQueue);

        server = new Server(threadPool);

        ServerConnector http = new ServerConnector(server);
        http.setPort(port);
        http.setIdleTimeout(30000);

        // SSL Connector

        SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setSslContext(sslContext);
        sslContextFactory.setNeedClientAuth(true);
        ServerConnector https = new ServerConnector(server, sslContextFactory);
        https.setPort(sslPort);
        https.setIdleTimeout(30000);

        server.addConnector(https);
        logger.info("Added ssl connector on port " + sslPort);

        server.addConnector(http);
        logger.info("Added connector on port " + port);


        // Port 7001 because some clients + ELB use it.
        ServerConnector http_7001 = new ServerConnector(server);
        http_7001.setPort(7001);
        http_7001.setIdleTimeout(30000);
        server.addConnector(http_7001);

        // The UI uses 7102 to connect to websockets for whatever reason.
        ServerConnector http_7102 = new ServerConnector(server);
        http_7102.setPort(7102);
        http_7102.setIdleTimeout(30000);
        server.addConnector(http_7102);

        servletContextHandler.setResourceBase("/");
        servletContextHandler.setContextPath("/");

        SessionContextBuilder sessionContextBuilder = SessionContextBuilder.getInstance(propertyRepository, registry, workerThreadPool);

        servletContextHandler.addServlet(new ServletHolder(new HealthCheckServlet(propertyRepository, registry, workerThreadPool)), "/healthcheck");
        servletContextHandler.addServlet(new ServletHolder(new MREAppStreamToJobClusterMappingServlet(propertyRepository, registry, workerThreadPool)), MREAppStreamToJobClusterMappingServlet.PATH_SPEC + "/*");
        servletContextHandler.addServlet(new ServletHolder(new AppStreamDiscoveryServlet(propertyRepository, registry, workerThreadPool, mantisClient)), AppStreamDiscoveryServlet.PATH_SPEC);


        List<String> helpMsgs = new ArrayList<>();

        servletContextHandler.addServlet(new ServletHolder(new JobConnectByIdWebSocketServlet(this.mantisClient, remoteSinkConnector, propertyRepository, registry, workerThreadPool, mantisMasterClientApi)), "/" + JobConnectByIdWebSocketServlet.handlerName + "/*");
        servletContextHandler.addServlet(new ServletHolder(new JobConnectByIdWebSocketServlet(this.mantisClient, remoteSinkConnector, propertyRepository, registry, workerThreadPool, mantisMasterClientApi)), "/api/v1/" + JobConnectByIdWebSocketServlet.handlerName + "/*");
        helpMsgs.add(JobConnectByIdWebSocketServlet.helpMsg);

        servletContextHandler.addServlet(new ServletHolder(new JobStatusWebSocketServlet(this.mantisClient, remoteSinkConnector, propertyRepository, registry, workerThreadPool)), "/" + JobStatusWebSocketServlet.endpointName + "/*");
        servletContextHandler.addServlet(new ServletHolder(new JobStatusWebSocketServlet(this.mantisClient, remoteSinkConnector, propertyRepository, registry, workerThreadPool)), "/api/v1/" + JobStatusWebSocketServlet.endpointName + "/*");
        helpMsgs.add(JobStatusWebSocketServlet.helpMsg);

        helpMsgs.add(JobStatusWebSocketServlet.helpMsg);

        servletContextHandler.addServlet(new ServletHolder(new JobSchedulingInfoServlet(this.mantisClient, propertyRepository, registry, workerThreadPool)), "/api/v1/jobs/" + JobSchedulingInfoServlet.endpointName + "/*");
        helpMsgs.add(JobSchedulingInfoServlet.helpMsg);
        servletContextHandler.addServlet(new ServletHolder(new JobClusterDiscoveryInfoServlet(masterClientWrapper, this.mantisClient, propertyRepository, registry, workerThreadPool)), "/" + JobClusterDiscoveryInfoServlet.endpointName + "/*");
        helpMsgs.add(JobClusterDiscoveryInfoServlet.helpMsg);
        servletContextHandler.addServlet(new ServletHolder(new JobClusterDiscoveryInfoWebSocketServlet(this.mantisClient, propertyRepository, registry, workerThreadPool)), "/" + JobClusterDiscoveryInfoWebSocketServlet.endpointName + "/*");
        servletContextHandler.addServlet(new ServletHolder(new JobClusterDiscoveryInfoWebSocketServlet(this.mantisClient, propertyRepository, registry, workerThreadPool)), "/api/v1/" + JobClusterDiscoveryInfoWebSocketServlet.endpointName + "/*");
        helpMsgs.add(JobClusterDiscoveryInfoWebSocketServlet.helpMsg);
        servletContextHandler.addServlet(new ServletHolder(new JobConnectByNameWebSocketServlet(this.mantisClient, remoteSinkConnector, propertyRepository, registry, workerThreadPool)), "/" + JobConnectByNameWebSocketServlet.handlerName + "/*");
        servletContextHandler.addServlet(new ServletHolder(new JobConnectByNameWebSocketServlet(this.mantisClient, remoteSinkConnector, propertyRepository, registry, workerThreadPool)), "/api/v1/" + JobConnectByNameWebSocketServlet.handlerName + "/*");
        helpMsgs.add(JobConnectByNameWebSocketServlet.helpMsg);
        servletContextHandler.addServlet(new ServletHolder(new JobSubmitAndConnectServlet(this.mantisClient, masterClientWrapper, remoteSinkConnector, registry, propertyRepository, workerThreadPool)), "/" + JobSubmitAndConnectServlet.endpointName + "/*");
        servletContextHandler.addServlet(new ServletHolder(new JobSubmitAndConnectServlet(this.mantisClient, masterClientWrapper, remoteSinkConnector, registry, propertyRepository, workerThreadPool)), "/api/v1/" + JobSubmitAndConnectServlet.endpointName + "/*");
        helpMsgs.add(JobSubmitAndConnectServlet.helpMsg);
        servletContextHandler.addServlet(new ServletHolder(new RegionsHandlerWebSocketServlet(this.mantisClient, masterClientWrapper, mantisMasterClientApi, streamingClientFactory, propertyRepository, registry, workerThreadPool)), "/" + RegionsHandlerWebSocketServlet.endpointName + "/*");
        helpMsgs.add(RegionsHandlerWebSocketServlet.helpMsg);

        ServletHolder fileUploadServletHolder = new ServletHolder(new ArtifactUploadServlet(artifatManager));
        servletContextHandler.addServlet(fileUploadServletHolder, "/api/v1/" + ArtifactUploadServlet.endpointName + "/*");

        servletContextHandler.addServlet(new ServletHolder(new HelpHandlerServlet(helpMsgs, masterClientWrapper.getMasterMonitor(), registry, sessionContextBuilder)), "/help");
        servletContextHandler.addServlet(new ServletHolder(new StatsHandlerServlet(sessionContextBuilder)), "/" + StatsHandlerServlet.endpointName);
        servletContextHandler.addServlet(new ServletHolder(new MantisMasterEndpointRelayServlet(masterClientWrapper, streamingClientFactory, propertyRepository, registry, workerThreadPool)), "/");

        for (Tuple2<String, ServletHolder> tup : servlets) {
            servletContextHandler.addServlet(tup._2, tup._1);
        }

        //
        // Filters: DOS, CORS, INetAccess, ForwardedRequestCustomizer
        //

        EnumSet<DispatcherType> SCOPE = EnumSet.of(DispatcherType.REQUEST);
        servletContextHandler.addFilter(getDoSFilterHolder(propertyRepository), "/*", SCOPE);
        servletContextHandler.addFilter(getCORSFilterHolder(), "/*", SCOPE);
        servletContextHandler.addFilter(getReadOnlyFilter(propertyRepository), "/*", SCOPE);

        InetAccessHandler inetAccessHandler = new CustomInetAccessHandler();
        String[] blacklistPatterns = getBlackList(propertyRepository);
        if (blacklistPatterns.length > 0) {
            inetAccessHandler.exclude(blacklistPatterns);
        }
        inetAccessHandler.setHandler(servletContextHandler);

        server.setHandler(inetAccessHandler);
        addForwardedRequestCustomizer(server);

        //
        // Access Log
        //

        NCSARequestLog requestLog = new NCSARequestLog("/logs/mantisapi/jetty-yyyy_mm_dd.request.log");
        requestLog.setAppend(true);
        requestLog.setExtended(true);
        requestLog.setLogTimeZone("PDT");
        requestLog.setLogLatency(true);
        requestLog.setRetainDays(7);

        server.setRequestLog(requestLog);
    }

    public void start() {
        try {
            logger.info("Starting jetty webserver on port {} and ssl port {}", serverPort, sslPort);
            server.start();
            logger.info("Started jetty webserver on port {} and ssl port {}", serverPort, sslPort);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void stop() {
        try {
            server.stop();
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
        }
    }

    private FilterHolder getDoSFilterHolder(PropertyRepository propertyRepository) {
        Property<String> ipWhiteList = propertyRepository.get(PropertyNames.ipWhiteList, String.class).orElse("");
        Property<String> delayMs = propertyRepository.get(PropertyNames.delayMs, String.class).orElse("-1");
        Property<String> maxRequestsPerSec = propertyRepository.get(PropertyNames.maxRequestsPerSec, String.class).orElse("25");

        FilterHolder holder = new FilterHolder(DoSFilter.class);
        holder.setInitParameter("maxRequestsPerSec", maxRequestsPerSec.get());
        holder.setInitParameter("delayMs", delayMs.get());
        holder.setInitParameter("ipWhitelist", ipWhiteList.get());

        return holder;
    }

    private FilterHolder getCORSFilterHolder() {
        FilterHolder holder = new FilterHolder(CrossOriginFilter.class);
        holder.setInitParameter("allowedMethods", "GET");

        return holder;
    }

    private FilterHolder getReadOnlyFilter(PropertyRepository propertyRepository) {
        FilterHolder holder = new FilterHolder(new ReadOnlyFilter(propertyRepository));
        return holder;
    }

    private String[] getBlackList(PropertyRepository propertyRepository) {
        Property<String> ipBlacklist = propertyRepository.get(PropertyNames.ipBlackList, String.class).orElse("");
        return ipBlacklist.get().isEmpty() ? new String[] {} : ipBlacklist.get().split(",");
    }


    /**
     * Adds ForwardedRequestCustomizer to the server's connection factories. This is important to use in conjunction
     * with the DOSFilter as it will otherwise block the ELB.
     *
     * @param server The Server to which we should add ForwardedRequestCustomizer
     */
    public void addForwardedRequestCustomizer(Server server) {
        ForwardedRequestCustomizer customizer = new ForwardedRequestCustomizer();
        for (Connector connector : server.getConnectors()) {
            for (ConnectionFactory connectionFactory : connector
                    .getConnectionFactories()) {
                if (connectionFactory instanceof HttpConfiguration.ConnectionFactory) {
                    ((HttpConfiguration.ConnectionFactory) connectionFactory)
                            .getHttpConfiguration().addCustomizer(customizer);
                }
            }
        }
    }
}

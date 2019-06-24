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

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.spectator.api.Registry;
import io.mantisrx.api.PropertyNames;
import io.mantisrx.api.SessionContext;
import io.mantisrx.api.SessionContextBuilder;
import io.mantisrx.api.WorkerThreadPool;
import io.mantisrx.api.handlers.utils.HttpUtils;
import io.mantisrx.api.handlers.utils.MantisClientUtil;
import io.mantisrx.api.handlers.utils.Regions;
import io.mantisrx.api.handlers.utils.RetryUtils;
import io.mantisrx.api.tunnel.StreamingClientFactory;
import io.mantisrx.server.master.client.MasterClientWrapper;
import io.netty.handler.codec.http.HttpMethod;
import mantis.io.reactivex.netty.channel.StringTransformer;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.functions.Func1;


public class MantisMasterEndpointRelayServlet extends HttpServlet {

    private static final Logger logger = LoggerFactory.getLogger(MantisMasterEndpointRelayServlet.class);
    private static final Func1<Observable<? extends Throwable>, Observable<?>> retryLogic =
            RetryUtils.getRetryFunc(logger, 5);
    private static final long serialVersionUID = MantisMasterEndpointRelayServlet.class.hashCode();
    private static final Pattern clusterJobPattern = Pattern.compile("api/v1/jobClusters/.*/jobs");
    private static final Pattern scaleStagePattern = Pattern.compile("api/v1/jobs/.*/actions/scaleStage");

    private transient final MasterClientWrapper masterClientWrapper;
    private transient final StreamingClientFactory streamingClientFactory;
    private transient final PropertyRepository propertyRepository;
    private transient final Registry registry;
    private transient final WorkerThreadPool workerThreadPool;

    private transient final Property<Boolean> apiCacheEnabled;
    private transient final Property<Boolean> crossRegionCacheEnabled;
    private transient final Property<Integer> instanceLimit;

    public MantisMasterEndpointRelayServlet(MasterClientWrapper masterClientWrapper, StreamingClientFactory streamingClientFactory, PropertyRepository propertyRepository, Registry registry, WorkerThreadPool workerThreadPool) {
        this.masterClientWrapper = masterClientWrapper;
        this.streamingClientFactory = streamingClientFactory;
        this.propertyRepository = propertyRepository;
        this.registry = registry;
        this.workerThreadPool = workerThreadPool;
        this.apiCacheEnabled = propertyRepository.get(PropertyNames.mantisAPICacheEnabled, Boolean.class).orElse(false);
        this.crossRegionCacheEnabled = propertyRepository.get(PropertyNames.crossRegionCacheEnabled, Boolean.class).orElse(false);
        this.instanceLimit = propertyRepository.get("mantisapi.submit.instanceLimit", Integer.class).orElse(750);
    }

    @Override
    public void doOptions(HttpServletRequest request, HttpServletResponse response) {
        HttpUtils.addBaseHeaders(response, "GET", "OPTIONS", "POST", "DELETE", "PUT");
        response.setStatus(HttpServletResponse.SC_NO_CONTENT);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) {
        final SessionContext httpSessionCtx = getSessionContext(request);
        CountDownLatch latch = new CountDownLatch(1);
        Subscription subscription = null;
        try {
            String uri = request.getRequestURI();
            if (request.getQueryString() != null && !request.getQueryString().isEmpty())
                uri = uri + "?" + request.getQueryString();
            logger.info("Relaying " + uri);

            subscription = MantisClientUtil
                    .callGetOnMasterWithCache(masterClientWrapper.getMasterMonitor().getMasterObservable(), uri, apiCacheEnabled)
                    .retryWhen(retryLogic)
                    .doOnError(t -> {
                        logger.warn("Timed out relaying request for session id " + httpSessionCtx.getId() +
                                " url=" + request.getRequestURI());
                        response.setStatus(HttpServletResponse.SC_REQUEST_TIMEOUT);
                        try {
                            response.setStatus(500);
                            response.getOutputStream().print(t.getMessage());
                            response.flushBuffer();
                        } catch (IOException e) {
                            logger.info("Couldn't write message (" + t.getMessage() + ") to client session " +
                                    httpSessionCtx.getId() + ": " + e.getMessage());
                        }
                        latch.countDown();
                    })
                    .doOnNext(result -> {
                        HttpUtils.addBaseHeaders(response, "GET", "POST", "OPTIONS");
                        try {
                            response.setStatus(result._1().code());
                            response.getOutputStream().print(result._2());
                            response.flushBuffer();
                        } catch (IOException e) {
                            logger.info("Error writing to client session " + httpSessionCtx.getId() + ": " + e.getMessage());
                            latch.countDown();
                        }
                    })
                    .doOnTerminate(latch::countDown)
                    .doOnUnsubscribe(latch::countDown)
                    .subscribe();
            while (latch.getCount() > 0) {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    logger.info("Will retry after getting interrupted waiting for latch: " + e.getMessage());
                }
            }
        } finally {
            if (subscription != null && !subscription.isUnsubscribed())
                subscription.unsubscribe();
            httpSessionCtx.endSession();
        }
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws IOException {

        SessionContext httpSessionCtx = getSessionContext(request);
        String content = getRequestContent(request);

        try {
            // handle special case to limit request behaviors for submit and scale calls
            JsonParser parser = new JsonParser();
            if (isSubmit(request.getRequestURI(), HttpMethod.POST)) {
                JsonObject obj = parser.parse(content).getAsJsonObject();

                JsonObject stages = obj.getAsJsonObject("schedulingInfo")
                        .getAsJsonObject("stages");

                Integer totalInstances = stages.entrySet().stream()
                        .map(o -> o.getValue()
                                .getAsJsonObject()
                                .getAsJsonPrimitive("numberOfInstances")
                                .getAsInt())
                        .reduce(Integer::sum)
                        .orElse(0);

                if (totalInstances > instanceLimit.get()) {
                    failRequestTooManyInstances(response, httpSessionCtx, totalInstances);
                    return;
                }
            } else if (isScale(request.getRequestURI(), HttpMethod.POST)) {
                JsonObject payload = parser.parse(content).getAsJsonObject();
                Integer totalInstances = payload.get("NumWorkers").getAsInt();

                if (totalInstances > instanceLimit.get()) {
                    failRequestTooManyInstances(response, httpSessionCtx, totalInstances);
                    return;
                }
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }

        forwardRequestToMaster(httpSessionCtx, request, response, content);
    }

    @Override
    protected void doPut(HttpServletRequest request, HttpServletResponse response) throws IOException {
        final String content = getRequestContent(request);
        forwardRequestToMaster(request, response, content);
    }

    @Override
    protected void doDelete(HttpServletRequest request, HttpServletResponse response) {
        forwardRequestToMaster(request, response);
    }

    private void forwardRequestToMaster(HttpServletRequest request, HttpServletResponse response) {
        forwardRequestToMaster(getSessionContext(request), request, response, "");
    }

    private void forwardRequestToMaster(HttpServletRequest request, HttpServletResponse response, String content) {
        forwardRequestToMaster(getSessionContext(request), request, response, content);
    }

    private void forwardRequestToMaster(SessionContext httpSessionCtx, HttpServletRequest request, HttpServletResponse response, String content) {
        CountDownLatch latch = new CountDownLatch(1);
        Subscription subscription = null;

        try {
            String uri = request.getRequestURI();
            if (request.getQueryString() != null && !request.getQueryString().isEmpty())
                uri = uri + "?" + request.getQueryString();

            Observable<MantisClientUtil.MasterResponse> responseObservable;
            switch (request.getMethod().toLowerCase()) {
            case "post":
                responseObservable = MantisClientUtil
                        .callPostOnMaster(masterClientWrapper.getMasterMonitor().getMasterObservable(), uri, content);
                break;

            case "put":
                responseObservable = MantisClientUtil
                        .callPutOnMaster(masterClientWrapper.getMasterMonitor().getMasterObservable(), uri, content);
                break;

            case "get":
                responseObservable = MantisClientUtil
                        .callGetOnMaster(masterClientWrapper.getMasterMonitor().getMasterObservable(), uri, registry);
                break;

            case "delete":
                responseObservable = MantisClientUtil
                        .callDeleteOnMaster(masterClientWrapper.getMasterMonitor().getMasterObservable(), uri);
                break;
            default:
                response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
                HttpUtils.addBaseHeaders(response, "GET", "POST", "PUT", "DELETE", "OPTIONS");
                return;
            }

            subscription = responseObservable
                    .retryWhen(retryLogic)
                    .flatMap(reply -> {
                        response.setStatus(reply.getStatus().code());

                        return reply.getByteBuf().map(bb -> {
                            HttpUtils.addBaseHeaders(response, "GET", "POST", "PUT", "DELETE", "OPTIONS");
                            try {
                                if (response.getOutputStream() != null) {
                                    response.getOutputStream()
                                            .print(bb.toString(Charset.forName("UTF-8")));
                                }
                            } catch (IOException e) {
                                logger.info("Error writing to client session " + httpSessionCtx.getId() + ": " + e.getMessage());
                                latch.countDown();
                            }
                            return Observable.empty();
                        });
                    })
                    .doOnTerminate(latch::countDown)
                    .doOnUnsubscribe(latch::countDown)
                    .subscribe();

            while (latch.getCount() > 0) {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    logger.info("Will retry after getting interrupted waiting for latch: " + e.getMessage());
                }
            }
        } finally {
            if (subscription != null && !subscription.isUnsubscribed())
                subscription.unsubscribe();
            httpSessionCtx.endSession();
        }
    }

    private SessionContext getSessionContext(HttpServletRequest request) {
        final SessionContextBuilder contextBuilder = SessionContextBuilder.getInstance(propertyRepository, registry, workerThreadPool);
        return contextBuilder
                .createHttpSessionCtx(request.getRemoteAddr(),
                        request.getRequestURI() + "?" + request.getQueryString(),
                        request.getMethod());
    }

    private String getRequestContent(HttpServletRequest request) throws IOException {
        StringBuilder content = new StringBuilder();

        // get json content
        try (BufferedReader reader = request.getReader()) {
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line);
            }
        }

        return content.toString();
    }

    private boolean isSubmit(String path, HttpMethod verb) {

        return path != null && verb == HttpMethod.POST &&
                (path.contains("api/submit") || path.contains("api/submitandconnect") ||
                        path.contains("api/v1/jobs") || clusterJobPattern.matcher(path).matches());
    }

    private boolean isScale(String path, HttpMethod verb) {
        return path != null && verb == HttpMethod.POST &&
                (path.contains("api/jobs/scaleStage") || scaleStagePattern.matcher(path).matches());
    }

    private void failRequestTooManyInstances(HttpServletResponse response, SessionContext httpSessionCtx, Integer totalInstances) throws IOException {
        response.setStatus(400);
        response.getOutputStream().print(String.format("Unable to submit/scale job with more than %d total" +
                " instances across all stages. Yours had %d. Controlled by mantisapi.submit.instanceLimit" +
                " property.%n", instanceLimit.get(), totalInstances));
        response.getOutputStream().flush();
        httpSessionCtx.endSession();
    }

    public void handleGetWithRegions(HttpServletRequest request, HttpServletResponse response, String path, SessionContext sessionContext) {
        logger.info("Relaying GET URI " + path + " to regions: " + sessionContext.getRegions());

        final SessionContextBuilder contextBuilder = SessionContextBuilder.getInstance(propertyRepository, registry, workerThreadPool);
        final SessionContext httpSessionCtx = contextBuilder.createHttpSessionCtx(request.getRemoteAddr(),
                request.getRequestURI() + "?" + request.getQueryString(), request.getMethod());

        List<String> regions = sessionContext.getRegions();
        if (regions == null || regions.isEmpty())
            regions = Lists.newArrayList(Regions.getLocalRegion());
        final String queryString = request.getQueryString();
        final CountDownLatch latch = new CountDownLatch(1);

        Subscription subscription = null;
        try {
            subscription = Observable.from(regions)
                    .flatMap(region -> {
                        if (Regions.isLocalRegion(region)) {
                            String uri = path;
                            if (queryString != null && !queryString.isEmpty())
                                uri = uri + "?" + queryString;
                            return MantisClientUtil
                                    .callGetOnMasterWithCache(masterClientWrapper.getMasterMonitor().getMasterObservable(), "/" + uri, apiCacheEnabled)
                                    .retryWhen(retryLogic)
                                    .map(res -> new EndpointRelay.RegionData(region, true, res._2(), res._1().code()));
                        } else {
                            final String uri = EndpointRelay.getUri(path, queryString);

                            HttpClientRequest<String> rq = HttpClientRequest.create(HttpMethod.GET, uri);
                            return EndpointRelay.handleSubmitResponseForGetWithCache(streamingClientFactory, sessionContext.getId(), rq, region, crossRegionCacheEnabled);

                        }
                    })
                    .reduce(new ArrayList<EndpointRelay.RegionData>(), (regionDatas, regionData) -> {
                        regionDatas.add(regionData);
                        return regionDatas;
                    })
                    .flatMap(dataList -> {
                        writeResults(response, dataList, latch, sessionContext);
                        return Observable.empty();
                    })
                    .doOnTerminate(latch::countDown)
                    .doOnUnsubscribe(latch::countDown)
                    .subscribe();
            while (latch.getCount() > 0) {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    logger.info("Will retry after getting interrupted waiting for latch: " + e.getMessage());
                }
            }
        } finally {
            if (subscription != null && !subscription.isUnsubscribed())
                subscription.unsubscribe();
            httpSessionCtx.endSession();
        }
    }

    private void writeResults(HttpServletResponse response, ArrayList<EndpointRelay.RegionData> dataList,
                              CountDownLatch latch, SessionContext sessionContext) {
        HttpUtils.addBaseHeaders(response, "GET", "POST", "OPTIONS");
        response.setHeader("content-type", "application/json");
        response.setStatus(HttpServletResponse.SC_OK);
        StringBuilder sb = new StringBuilder("[");
        try {
            boolean first = true;
            for (EndpointRelay.RegionData data : dataList) {
                if (first)
                    first = false;
                else {
                    sb.append(",");
                }

                if (data.success) {
                    String outputData = Regions.getForceWrappedJson(data.data, data.region, data.responseCode, null);
                    sb.append(outputData);
                } else {
                    sb.append(Regions.getForceWrappedJson("", data.region, data.responseCode, data.data));
                }

            }
            sb.append("]");
            response.getWriter().print(sb.toString());
            response.flushBuffer();
        } catch (IOException e) {
            logger.info("Error writing data to client session " + sessionContext.getId() + ": " + e.getMessage());
            e.printStackTrace();
            latch.countDown();
        }
    }

    public void handlePostWithRegions(HttpServletRequest request, HttpServletResponse response, String path,
                                      String content, SessionContext sessionContext) {
        logger.info("Relaying POST URI " + path);
        List<String> regions = sessionContext.getRegions();
        if (regions == null || regions.isEmpty())
            regions = Lists.newArrayList(Regions.getLocalRegion());
        final String queryString = request.getQueryString();
        final CountDownLatch latch = new CountDownLatch(1);
        Subscription subscription = null;
        try {
            subscription = Observable.from(regions)
                    .flatMap(region -> {
                        if (Regions.isLocalRegion(region)) {
                            String uri = (queryString == null || queryString.isEmpty()) ?
                                    path : path + "?" + queryString;
                            return MantisClientUtil
                                    .callPostOnMaster(masterClientWrapper.getMasterMonitor().getMasterObservable(), "/" + uri, content)
                                    .retryWhen(retryLogic)
                                    .flatMap(masterResponse -> {
                                        int code = masterResponse.getStatus().code();
                                        return masterResponse.getByteBuf()
                                                .take(1)
                                                .map(byteBuf -> new EndpointRelay.RegionData(region, true, byteBuf.toString(Charset.forName("UTF-8")), code))
                                                .onErrorReturn(t -> {
                                                    logger.warn("Error on POST to local master: " + t.getMessage(), t);
                                                    return new EndpointRelay.RegionData(region, false, t.getMessage(), code);
                                                });
                                    })
                                    .onErrorReturn(t -> {
                                        logger.warn("Session id " + sessionContext.getId() + ": Error on POST to local master: " + t.getMessage(), t);
                                        return new EndpointRelay.RegionData(region, false, t.getMessage(), 0);
                                    })
                                    ;
                        } else {
                            final String uri = EndpointRelay.getUri(path, queryString);
                            logger.info("*********** PostWithRegions url=" + uri);
                            logger.info("Content: " + content);
                            HttpClientRequest<String> rq = HttpClientRequest.create(HttpMethod.POST, uri);
                            rq.withRawContent(content, StringTransformer.DEFAULT_INSTANCE);
                            return EndpointRelay.handleSubmitResponse(streamingClientFactory, sessionContext.getId(), rq, region);
                        }
                    })
                    .reduce(new ArrayList<EndpointRelay.RegionData>(), (regionDatas, regionData) -> {
                        regionDatas.add(regionData);
                        return regionDatas;
                    })
                    .flatMap(dataList -> {
                        writeResults(response, dataList, latch, sessionContext);
                        return Observable.empty();
                    })
                    .doOnTerminate(latch::countDown)
                    .doOnUnsubscribe(latch::countDown)
                    .subscribe();
            while (latch.getCount() > 0) {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    logger.warn("Will retry after getting interrupted waiting for latch: " + e.getMessage());
                }
            }
        } finally {
            if (subscription != null && !subscription.isUnsubscribed())
                subscription.unsubscribe();
            sessionContext.endSession();
        }
    }
}

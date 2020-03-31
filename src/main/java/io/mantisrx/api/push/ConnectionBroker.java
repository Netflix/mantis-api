package io.mantisrx.api.push;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.spectator.api.Counter;
import com.netflix.zuul.netty.SpectatorUtils;
import io.mantisrx.api.Constants;
import io.mantisrx.api.Util;
import io.mantisrx.api.services.JobDiscoveryService;
import io.mantisrx.api.tunnel.MantisCrossRegionalClient;
import io.mantisrx.client.MantisClient;
import io.mantisrx.client.SinkConnectionFunc;
import io.mantisrx.client.SseSinkConnectionFunction;
import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.runtime.parameter.SinkParameters;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.vavr.control.Try;
import lombok.extern.slf4j.Slf4j;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import mantis.io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.schedulers.Schedulers;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.mantisrx.api.Constants.TunnelPingMessage;

@Slf4j
@Singleton
public class ConnectionBroker {

    private final MantisClient mantisClient;
    private final MantisCrossRegionalClient mantisCrossRegionalClient;
    private final JobDiscoveryService jobDiscoveryService;
    private final Scheduler scheduler;
    private final ObjectMapper objectMapper;

    private final Map<PushConnectionDetails, Observable<String>> connectionCache = new ConcurrentHashMap<>();

    @Inject
    public ConnectionBroker(MantisClient mantisClient,
                            MantisCrossRegionalClient mantisCrossRegionalClient,
                            @Named("io-scheduler") Scheduler scheduler,
                            ObjectMapper objectMapper) {
        this.mantisClient = mantisClient;
        this.mantisCrossRegionalClient = mantisCrossRegionalClient;
        this.jobDiscoveryService = JobDiscoveryService.getInstance(mantisClient, scheduler);
        this.scheduler = scheduler;
        this.objectMapper = objectMapper;
    }

    public Observable<String> connect(PushConnectionDetails details) {

        if (!connectionCache.containsKey(details)) {
            switch (details.type) {
                case CONNECT_BY_NAME:
                    connectionCache.put(details,
                            getConnectByNameFor(details)
                                    .subscribeOn(scheduler)
                                    .doOnUnsubscribe(() -> {
                                        log.info("Purging {} from cache.", details);
                                        connectionCache.remove(details);
                                    })
                                    .doOnCompleted(() -> {
                                        log.info("Purging {} from cache.", details);
                                        connectionCache.remove(details);
                                    })
                                    .share());
                    break;
                case CONNECT_BY_ID:
                    connectionCache.put(details,
                            getConnectByIdFor(details)
                            .subscribeOn(scheduler)
                            .doOnUnsubscribe(() -> {
                                log.info("Purging {} from cache.", details);
                                connectionCache.remove(details);
                            })
                            .doOnCompleted(() -> {
                                log.info("Purging {} from cache.", details);
                                connectionCache.remove(details);
                            })
                            .share());
                    break;

                case JOB_STATUS:
                    connectionCache.put(details,
                            mantisClient
                                    .getJobStatusObservable(details.target)
                                    .subscribeOn(scheduler)
                                    .doOnCompleted(() -> {
                                        log.info("Purging {} from cache.", details);
                                        connectionCache.remove(details);
                                    })
                                    .doOnUnsubscribe(() -> {
                                        log.info("Purging {} from cache.", details);
                                        connectionCache.remove(details);
                                    })
                                    .replay(25)
                                    .autoConnect());
                    break;
                case JOB_SCHEDULING_INFO:
                    connectionCache.put(details,
                            mantisClient.getSchedulingChanges(details.target)
                                    .subscribeOn(scheduler)
                                    .map(changes -> Try.of(() -> objectMapper.writeValueAsString(changes)).getOrElse("Error"))
                                    .doOnCompleted(() -> {
                                        log.info("Purging {} from cache.", details);
                                        connectionCache.remove(details);
                                    })
                                    .doOnUnsubscribe(() -> {
                                        log.info("Purging {} from cache.", details);
                                        connectionCache.remove(details);
                                    })
                            .replay(1)
                            .autoConnect());
                    break;

                case JOB_CLUSTER_DISCOVERY:
                    connectionCache.put(details,
                            jobDiscoveryService.jobDiscoveryInfoStream(jobDiscoveryService.key(JobDiscoveryService.LookupType.JOB_CLUSTER, details.target))
                                    .subscribeOn(scheduler)
                                    .map(jdi ->Try.of(() -> objectMapper.writeValueAsString(jdi)).getOrElse("Error"))
                                    .doOnCompleted(() -> {
                                        log.info("Purging {} from cache.", details);
                                        connectionCache.remove(details);
                                    })
                                    .doOnUnsubscribe(() -> {
                                        log.info("Purging {} from cache.", details);
                                        connectionCache.remove(details);
                                    })
                                    .replay(1)
                                    .autoConnect());
                    break;
            }
            log.info("Caching connection for: {}", details);
        }
        return connectionCache.get(details);
    }

    //
    // Helpers
    //

    private Observable<String> getConnectByNameFor(PushConnectionDetails details) {
        return details.regions.isEmpty()
                ? getResults(false, this.mantisClient, details.target, details.getSinkparameters())
                .flatMap(m -> m)
                .map(MantisServerSentEvent::getEventAsString)
                : getRemoteDataObservable(details.getUri(), details.target, details.getRegions().asJava());
    }

    private Observable<String> getConnectByIdFor(PushConnectionDetails details) {
        return details.getRegions().isEmpty()
                ? getResults(true, this.mantisClient, details.target, details.getSinkparameters())
                .flatMap(m -> m)
                .map(MantisServerSentEvent::getEventAsString)
                : getRemoteDataObservable(details.getUri(), details.target, details.getRegions().asJava());
    }


    private static SinkConnectionFunc<MantisServerSentEvent> getSseConnFunc(final String target, SinkParameters sinkParameters) {
        return new SseSinkConnectionFunction(true,
                t -> log.warn("Reconnecting to sink of job " + target + " after error: " + t.getMessage()),
                sinkParameters);
    }

    private static Observable<Observable<MantisServerSentEvent>> getResults(boolean isJobId, MantisClient mantisClient,
                                                                            final String target, SinkParameters sinkParameters) {
        final AtomicBoolean hasError = new AtomicBoolean();
        return  isJobId ?
                mantisClient.getSinkClientByJobId(target, getSseConnFunc(target, sinkParameters), null).getResults() :
                mantisClient.getSinkClientByJobName(target, getSseConnFunc(target, sinkParameters), null)
                        .switchMap(serverSentEventSinkClient -> {
                            if (serverSentEventSinkClient.hasError()) {
                                hasError.set(true);
                                return Observable.error(new Exception(serverSentEventSinkClient.getError()));
                            }
                            return serverSentEventSinkClient.getResults();
                        })
                        .takeWhile(o -> !hasError.get());
    }

    //
    // Tunnel
    //

    private Observable<String> getRemoteDataObservable(String uri, String target, List<String> regions) {
        return Observable.from(regions)
                .flatMap(region -> {
                    log.info("Connecting to remote region {} at {}.", region, uri);
                    return mantisCrossRegionalClient.getSecureSseClient(region)
                            .submit(HttpClientRequest.createGet(uri))
                            .retryWhen(Util.getRetryFunc(log, uri + " in " + region))
                            .doOnError(throwable -> log.warn(
                                    "Error getting response from remote SSE server for uri {} in region {}: {}",
                                    uri, region, throwable.getMessage(), throwable)
                            ).flatMap(remoteResponse -> {
                                if (!remoteResponse.getStatus().reasonPhrase().equals("OK")) {
                                    log.warn("Unexpected response from remote sink for uri {} region {}: {}", uri, region, remoteResponse.getStatus().reasonPhrase());
                                    String err = remoteResponse.getHeaders().get(Constants.metaErrorMsgHeader);
                                    if (err == null || err.isEmpty())
                                        err = remoteResponse.getStatus().reasonPhrase();
                                    return Observable.<MantisServerSentEvent>error(new Exception(err))
                                            .map(datum -> datum.getEventAsString());
                                }
                                final String originReplacement = "\\{\"" + Constants.metaOriginName + "\": \"" + region + "\", ";
                                return streamContent(remoteResponse, target, region, uri)
                                        .map(datum -> datum.getEventAsString().replaceFirst("^\\{", originReplacement))
                                        .doOnError(t -> log.error(t.getMessage()));
                            })
                            .subscribeOn(scheduler)
                            .observeOn(scheduler)
                            .doOnError(t -> log.warn("Error streaming in remote data ({}). Will retry: {}", region, t.getMessage(), t))
                            .doOnCompleted(() -> log.info(String.format("remote sink connection complete for uri %s, region=%s", uri, region)));
                })
                .observeOn(scheduler)
                .subscribeOn(scheduler)
                .doOnError(t -> log.error("Error in flatMapped cross-regional observable for {}", uri, t));
    }

    private Observable<MantisServerSentEvent> streamContent(HttpClientResponse<ServerSentEvent> response, String target, String
            region, String uri) {

        Counter numRemoteBytes = SpectatorUtils.newCounter(Constants.numRemoteBytesCounterName, target, "region", region);
        Counter numRemoteMessages = SpectatorUtils.newCounter(Constants.numRemoteMessagesCounterName, target, "region", region);
        Counter numSseErrors = SpectatorUtils.newCounter(Constants.numSseErrorsCounterName, target, "region", region);

        return response.getContent()
                .doOnError(t -> log.warn(t.getMessage()))
                .timeout(3 * Constants.TunnelPingIntervalSecs, TimeUnit.SECONDS)
                .doOnError(t -> log.warn("Timeout getting data from remote {} connection for {}", region, uri))
                .filter(sse -> !(!sse.hasEventType() || !sse.getEventTypeAsString().startsWith("error:")) ||
                        !TunnelPingMessage.equals(sse.contentAsString()))
                .map(t1 -> {
                    String data = "";
                    if (t1.hasEventType() && t1.getEventTypeAsString().startsWith("error:")) {
                        log.error("SSE has error, type=" + t1.getEventTypeAsString() + ", content=" + t1.contentAsString());
                        numSseErrors.increment();
                        throw new RuntimeException("Got error SSE event: " + t1.contentAsString());
                    }
                    try {
                        data = t1.contentAsString();
                        if (data != null) {
                            numRemoteBytes.increment(data.length());
                            numRemoteMessages.increment();
                        }
                    } catch (Exception e) {
                        log.error("Could not extract data from SSE " + e.getMessage(), e);
                    }
                    return new MantisServerSentEvent(data);
                });
    }

    public static void main(String[] args) throws InterruptedException {

        Observable.interval(1, TimeUnit.SECONDS)
                .mergeWith(Observable
                        .interval(1, TimeUnit.SECONDS)
                        .filter(x -> x < 0)
                        .map(x -> x = x * 1000)) // Very big numbers, but never emits anything.
                .subscribeOn(Schedulers.newThread())
                .observeOn(Schedulers.newThread())
                .subscribe(System.out::println);
        Thread.sleep(5050);
    }
}

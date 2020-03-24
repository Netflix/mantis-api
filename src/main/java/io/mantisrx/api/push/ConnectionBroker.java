package io.mantisrx.api.push;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.mantisrx.api.services.JobDiscoveryService;
import io.mantisrx.client.MantisClient;
import io.mantisrx.client.SinkConnectionFunc;
import io.mantisrx.client.SseSinkConnectionFunction;
import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.runtime.parameter.SinkParameters;
import io.vavr.control.Try;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;
import rx.Scheduler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Singleton
public class ConnectionBroker {

    private final MantisClient mantisClient;
    private final JobDiscoveryService jobDiscoveryService;
    private final Scheduler scheduler;
    private final ObjectMapper objectMapper;

    private final Map<PushConnectionDetails, Observable<String>> connectionCache = new ConcurrentHashMap<>();

    @Inject
    public ConnectionBroker(MantisClient mantisClient,
                            @Named("io-scheduler") Scheduler scheduler,
                            ObjectMapper objectMapper) {
        this.mantisClient = mantisClient;
        this.jobDiscoveryService = JobDiscoveryService.getInstance(mantisClient, scheduler);
        this.scheduler = scheduler;
        this.objectMapper = objectMapper;
    }

    public Observable<String> connect(PushConnectionDetails details) {

        if (!connectionCache.containsKey(details)) {
            switch (details.type) {
                case CONNECT_BY_NAME:
                    connectionCache.put(details,
                            getResults(false, this.mantisClient, details.target, details.getSinkparameters())
                                    .flatMap(m -> m)
                                    .map(MantisServerSentEvent::getEventAsString)
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
                            getResults(true, this.mantisClient, details.target, details.getSinkparameters())
                                    .flatMap(m -> m)
                                    .map(MantisServerSentEvent::getEventAsString)
                                    .subscribeOn(scheduler)
                                    .doOnCompleted(() -> {
                                        log.info("Purging {} from cache.", details);
                                        connectionCache.remove(details);
                                    })
                                    .doOnUnsubscribe(() -> {
                                        log.info("Purging {} from cache.", details);
                                        connectionCache.remove(details);
                                    })
                                    .share());
                    break;

                    // TODO: The three connections below are not currently being cached because they are cold then hot. Just need to work out a pattern for it.
                case JOB_STATUS:
                    return mantisClient
                            .getJobStatusObservable(details.target)
                            .subscribeOn(scheduler);
                    /*
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
                                    .share());
                    break;
                     */
                case JOB_SCHEDULING_INFO:
                    return mantisClient.getSchedulingChanges(details.target)
                            .subscribeOn(scheduler)
                            .map(changes -> Try.of(() -> objectMapper.writeValueAsString(changes)).getOrElse("Error"));
                    /*
                    connectionCache.put(details,
                            mantisClient.getSchedulingChanges(details.target)
                                    .subscribeOn(scheduler)
                                    .map(changes -> Try.of(() -> JacksonObjectMapper.getInstance().writeValueAsString(changes)).getOrElse("Error"))
                                    .doOnCompleted(() -> {
                                        log.info("Purging {} from cache.", details);
                                        connectionCache.remove(details);
                                    })
                                    .doOnUnsubscribe(() -> {
                                        log.info("Purging {} from cache.", details);
                                        connectionCache.remove(details);
                                    })
                                    .replay(1)
                                    .share());
                    break;
                     */

                case JOB_CLUSTER_DISCOVERY:
                    return jobDiscoveryService.jobDiscoveryInfoStream(jobDiscoveryService.key(JobDiscoveryService.LookupType.JOB_CLUSTER, details.target))
                            .subscribeOn(scheduler)
                            .map(jdi ->Try.of(() -> objectMapper.writeValueAsString(jdi)).getOrElse("Error"));
                    /*
                    connectionCache.put(details,
                            jobDiscoveryService.jobDiscoveryInfoStream(jobDiscoveryService.key(JobDiscoveryService.LookupType.JOB_CLUSTER, details.target))
                                    .subscribeOn(scheduler)
                                    .map(jdi ->Try.of(() -> JacksonObjectMapper.getInstance().writeValueAsString(jdi)).getOrElse("Error"))
                                    .doOnCompleted(() -> {
                                        log.info("Purging {} from cache.", details);
                                        connectionCache.remove(details);
                                    })
                                    .doOnUnsubscribe(() -> {
                                        log.info("Purging {} from cache.", details);
                                        connectionCache.remove(details);
                                    })
                                    .share());
                    break;
                     */
            }
            log.info("Caching connection for: {}", details);
        }
        return connectionCache.get(details);
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
}

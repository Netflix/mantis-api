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

package io.mantisrx.api.handlers.utils;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.impl.AtomicDouble;
import io.mantisrx.api.SpectatorUtils;
import io.mantisrx.client.MantisClient;
import io.mantisrx.server.core.JobSchedulingInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.schedulers.Schedulers;
import rx.subjects.BehaviorSubject;
import rx.subjects.Subject;


/**
 * The Purpose of this class is to dedup multiple job discovery info streams for the same JobId.
 * The first subscriber will cause a BehaviorSubject to be setup with data obtained from mantisClient.jobDiscoveryInfoStream
 * Future subscribers will connect to the same Subject
 * When the no. of subscribers falls to zero the Observable is un-subscribed and a cleanup callback is invoked.
 */

public class JobDiscoveryInfoSubjectHolder implements AutoCloseable {

    private static Logger logger = LoggerFactory.getLogger(JobDiscoveryInfoSubjectHolder.class);
    private final Func1<Observable<? extends Throwable>, Observable<?>> retryLogic;
    private Subscription subscription;
    private final AtomicInteger subscriberCount = new AtomicInteger();
    private final JobDiscoveryLookupKey lookupKey;
    private final MantisClient mantisClient;
    private AtomicBoolean inited = new AtomicBoolean(false);
    private CountDownLatch initComplete = new CountDownLatch(1);
    private final Action1 doOnZeroConnections;
    private final Subject<JobSchedulingInfo, JobSchedulingInfo> discoveryInfoBehaviorSubject = BehaviorSubject.create();
    private final Registry registry;

    private final Counter cleanupCounter;
    private final AtomicDouble subscriberCountGauge;

    public JobDiscoveryInfoSubjectHolder(MantisClient mantisClient, JobDiscoveryLookupKey lookupKey, Action1 onZeroConnections, Registry registry) {
        this(mantisClient, lookupKey, onZeroConnections, 5, registry);
    }

    /**
     * Ctor only no subscriptions happen as part of the ctor
     *
     * @param mantisClient      - Used to get the schedulingInfo Observable
     * @param lookupKey         - JobId or JobCluster to get schedulingInfo
     * @param onZeroConnections - Call back when there are no more subscriptions for this observable
     * @param retryCount        - No. of retires in case of error connecting to schedulingInfo
     */
    JobDiscoveryInfoSubjectHolder(MantisClient mantisClient,
                                  JobDiscoveryLookupKey lookupKey,
                                  Action1 onZeroConnections,
                                  int retryCount,
                                  Registry registry) {
        Preconditions.checkNotNull(mantisClient, "Mantis Client cannot be null");
        Preconditions.checkNotNull(lookupKey, "lookup key cannot be null");
        Preconditions.checkArgument(lookupKey.getId() != null && !lookupKey.getId().isEmpty(), "lookup key cannot be empty or null");
        Preconditions.checkNotNull(onZeroConnections, "on Zero Connections callback cannot be null");
        Preconditions.checkArgument(retryCount >= 0, "Retry count cannot be less than 0");
        this.lookupKey = lookupKey;
        this.mantisClient = mantisClient;
        this.doOnZeroConnections = onZeroConnections;
        this.retryLogic = RetryUtils.getRetryFunc(logger, retryCount);
        this.registry = registry;


        cleanupCounter = SpectatorUtils.buildAndRegisterCounter(registry, "mantisapi.discoveryinfo.cleanupCount", "lookupKey", lookupKey.getId());
        subscriberCountGauge = SpectatorUtils.buildAndRegisterGauge(registry, "mantisapi.discoveryinfo.subscriberCount", "lookupKey", lookupKey.getId());
    }

    /**
     * If invoked the first time it will subscribe to the schedulingInfo Observable via mantisClient and onNext
     * the results to the schedulinginfoSubject
     * If 2 or more threads concurrently invoke this only 1 will do the initialization while others wait.
     */
    private void init() {
        if (!inited.getAndSet(true)) {
            Observable<JobSchedulingInfo> jobSchedulingInfoObs;
            switch (lookupKey.getLookupType()) {
            case JOB_ID:
                jobSchedulingInfoObs = mantisClient.getSchedulingChanges(lookupKey.getId());
                break;
            case JOB_CLUSTER:
                jobSchedulingInfoObs = mantisClient.jobClusterDiscoveryInfoStream(lookupKey.getId());
                break;
            default:
                throw new IllegalArgumentException("lookup key type is not supported " + lookupKey.getLookupType());
            }
            subscription = jobSchedulingInfoObs
                    .retryWhen(retryLogic)
                    .doOnError((t) -> {
                        logger.info("cleanup jobDiscoveryInfo onError for {}", lookupKey);
                        discoveryInfoBehaviorSubject.toSerialized().onError(t);
                        doOnZeroConnections.call(lookupKey);
                    })
                    .doOnCompleted(() -> {
                        logger.info("cleanup jobDiscoveryInfo onCompleted for {}", lookupKey);
                        discoveryInfoBehaviorSubject.toSerialized().onCompleted();
                        doOnZeroConnections.call(lookupKey);
                    })
                    .subscribeOn(Schedulers.io())
                    .subscribe((schedInfo) -> discoveryInfoBehaviorSubject.onNext(schedInfo));
            initComplete.countDown();

        } else {
            try {
                initComplete.await();
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
    }


    /**
     * For testing
     *
     * @return current subscription count
     */
    int getSubscriptionCount() {
        return subscriberCount.get();
    }

    /**
     * If a subject holding schedulingInfo for the job exists return it as an Observable
     * if not then invoke mantisClient to get an Observable of scheduling changes, write them to a Subject
     * and return it as an observable
     * Also keep track of subscription Count, When the subscription count falls to 0 unsubscribe from schedulingInfo Observable
     *
     * @return Observable of scheduling changes
     */
    public Observable<JobSchedulingInfo> jobDiscoveryInfoStream() {
        init();
        return discoveryInfoBehaviorSubject
                .doOnSubscribe(() -> {
                    if (logger.isDebugEnabled()) { logger.debug("Subscribed"); }
                    subscriberCount.incrementAndGet();
                    subscriberCountGauge.set(subscriberCount.get());
                    if (logger.isDebugEnabled()) { logger.debug("Subscriber count " + subscriberCount.get()); }
                })
                .doOnUnsubscribe(() -> {
                    if (logger.isDebugEnabled()) {logger.debug("UnSubscribed"); }
                    int subscriberCnt = subscriberCount.decrementAndGet();
                    subscriberCountGauge.set(subscriberCount.get());
                    if (logger.isDebugEnabled()) { logger.debug("Subscriber count " + subscriberCnt); }
                    if (0 == subscriberCount.get()) {
                        if (logger.isDebugEnabled()) { logger.debug("Shutting down"); }
                        close();

                    }
                })
                .doOnError((t) -> close())
                ;
    }

    /**
     * Invoked If schedulingInfo Observable Completes or throws an onError of if the subscription count falls to 0
     * Unsubscribes from the schedulingInfoObservable and invokes doOnZeroConnection callback
     */

    @Override
    public void close() {
        if (logger.isDebugEnabled()) { logger.debug("In Close un-subscribing...." + subscription.isUnsubscribed()); }
        if (inited.get() && subscription != null && !subscription.isUnsubscribed()) {
            if (logger.isDebugEnabled()) { logger.debug("Unsubscribing...."); }
            subscription.unsubscribe();
            inited.set(false);
            initComplete = new CountDownLatch(1);
        }
        cleanupCounter.increment();
        logger.info("jobDiscoveryInfo close for {}", lookupKey);
        this.doOnZeroConnections.call(this.lookupKey);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobDiscoveryInfoSubjectHolder that = (JobDiscoveryInfoSubjectHolder) o;
        return Objects.equals(lookupKey, that.lookupKey);
    }

    @Override
    public int hashCode() {

        return Objects.hash(lookupKey);
    }
}

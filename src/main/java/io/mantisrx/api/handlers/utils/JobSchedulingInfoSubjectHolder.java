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
 * The Purpose of this class is to dedup multiple schedulingChanges streams for the same JobId.
 * The first subscriber will cause a BehaviorSubject to be setup with data obtained from mantisClient.getSchedulingChanges
 * Future subscribers will simply connect to the same Subject
 * When the no. of subscribers falls to zero the Observable is unsubscribed and a cleanup callback is invoked.
 */

public class JobSchedulingInfoSubjectHolder implements AutoCloseable {

    private static Logger logger = LoggerFactory.getLogger(JobSchedulingInfoSubjectHolder.class);
    private final Func1<Observable<? extends Throwable>, Observable<?>> retryLogic;
    private Subscription subscription;
    private final AtomicInteger subscriberCount = new AtomicInteger();
    private final String jobId;
    private final MantisClient mantisClient;
    private AtomicBoolean inited = new AtomicBoolean(false);
    private CountDownLatch initComplete = new CountDownLatch(1);
    private final Action1 doOnZeroConnections;
    private final Subject<JobSchedulingInfo, JobSchedulingInfo> schedulingInfoBehaviorSubjectingSubject = BehaviorSubject.create();
    private final Registry registry;

    private final Counter cleanupCounter;
    private final AtomicDouble subscriberCountGauge;

    public JobSchedulingInfoSubjectHolder(MantisClient mantisClient, String jobId, Action1 onZeroConnections, Registry registry) {
        this(mantisClient, jobId, onZeroConnections, 5, registry);
    }

    /**
     * Ctor only no subscriptions happen as part of the ctor
     *
     * @param mantisClient      - Used to get the schedulingInfo Observable
     * @param jobId             - JobId of job to get schedulingInfo
     * @param onZeroConnections - Call back when there are no more subscriptions for this observable
     * @param retryCount        - No. of retires in case of error connecting to schedulingInfo
     */
    JobSchedulingInfoSubjectHolder(MantisClient mantisClient,
                                   String jobId,
                                   Action1 onZeroConnections,
                                   int retryCount,
                                   Registry registry) {
        Preconditions.checkNotNull(mantisClient, "Mantis Client cannot be null");
        Preconditions.checkNotNull(jobId, "JobId cannot be null");
        Preconditions.checkArgument(!jobId.isEmpty(), "JobId cannot be empty");
        Preconditions.checkNotNull(onZeroConnections, "on Zero Connections callback cannot be null");
        Preconditions.checkArgument(retryCount >= 0, "Retry count cannot be less than 0");
        this.jobId = jobId;
        this.mantisClient = mantisClient;
        this.doOnZeroConnections = onZeroConnections;
        this.retryLogic = RetryUtils.getRetryFunc(logger, retryCount);
        this.registry = registry;


        cleanupCounter = SpectatorUtils.buildAndRegisterCounter(registry, "mantisapi.schedulingChanges.cleanupCount", "jobId", jobId);
        subscriberCountGauge = SpectatorUtils.buildAndRegisterGauge(registry, "mantisapi.schedulingChanges.subscriberCount", "jobId", jobId);
    }

    /**
     * If invoked the first time it will subscribe to the schedulingInfo Observable via mantisClient and onNext
     * the results to the schedulinginfoSubject
     * If 2 or more threads concurrently invoke this only 1 will do the initialization while others wait.
     */
    private void init() {
        if (!inited.getAndSet(true)) {
            subscription = mantisClient.getSchedulingChanges(jobId)
                    .retryWhen(retryLogic)
                    .doOnError((t) -> {
                        schedulingInfoBehaviorSubjectingSubject.toSerialized().onError(t);
                        doOnZeroConnections.call(jobId);
                    })
                    .doOnCompleted(() -> {
                        schedulingInfoBehaviorSubjectingSubject.toSerialized().onCompleted();
                        doOnZeroConnections.call(jobId);
                    })
                    .subscribeOn(Schedulers.io())
                    .subscribe((schedInfo) -> schedulingInfoBehaviorSubjectingSubject.onNext(schedInfo));
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
    public Observable<JobSchedulingInfo> getSchedulingChanges() {
        init();
        return schedulingInfoBehaviorSubjectingSubject

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
        if (logger.isDebugEnabled()) { logger.debug("In Close Unsubscribing...." + subscription.isUnsubscribed()); }
        if (inited.get() && subscription != null && !subscription.isUnsubscribed()) {
            if (logger.isDebugEnabled()) { logger.debug("Unsubscribing...."); }
            subscription.unsubscribe();
            inited.set(false);
            initComplete = new CountDownLatch(1);
        }
        cleanupCounter.increment();
        this.doOnZeroConnections.call(this.jobId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobSchedulingInfoSubjectHolder that = (JobSchedulingInfoSubjectHolder) o;
        return Objects.equals(jobId, that.jobId);
    }

    @Override
    public int hashCode() {

        return Objects.hash(jobId);
    }
}

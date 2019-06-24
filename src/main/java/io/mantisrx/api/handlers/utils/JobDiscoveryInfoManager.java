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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.impl.AtomicDouble;
import io.mantisrx.api.SpectatorUtils;
import io.mantisrx.client.MantisClient;
import io.mantisrx.server.core.JobSchedulingInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Action1;


/**
 * A Cache for Subjects per Job cluster holding job discovery Changes.
 * This is a singleton
 */
public class JobDiscoveryInfoManager {

    private static Logger logger = LoggerFactory.getLogger(JobDiscoveryInfoManager.class);

    private final MantisClient mantisClient;
    private final AtomicDouble subjectMapSizeGauge;
    private final Registry registry;

    private int retryCount = 5;
    private static JobDiscoveryInfoManager INSTANCE = null;

    public static synchronized JobDiscoveryInfoManager getInstance(MantisClient mantisClient, Registry registry) {
        if (INSTANCE == null) {
            INSTANCE = new JobDiscoveryInfoManager(mantisClient, registry);
        }
        return INSTANCE;
    }

    private JobDiscoveryInfoManager(final MantisClient mClient, final Registry registry) {
        Preconditions.checkNotNull(mClient, "mantisClient cannot be null");
        this.mantisClient = mClient;
        this.subjectMapSizeGauge = SpectatorUtils.buildAndRegisterGauge(registry, "mantisapi.discoveryInfo.subjectMapSize");
        this.registry = registry;
    }

    /**
     * For testing purposes
     *
     * @param cnt No of retries
     */
    @VisibleForTesting
    void setRetryCount(int cnt) {
        this.retryCount = cnt;
    }

    private final ConcurrentMap<JobDiscoveryLookupKey, JobDiscoveryInfoSubjectHolder> subjectMap = new ConcurrentHashMap<>();

    /**
     * Invoked by the subjectHolders when the subscription count goes to 0 (or if there is an error)
     */
    private final Action1<JobDiscoveryLookupKey> removeSubjectAction = key -> {
        if (logger.isDebugEnabled()) { logger.info("Removing subject for key {}", key.toString()); }
        removeSchedulingInfoSubject(key);
    };

    /**
     * Atomically inserts a JobDiscoveryInfoSubjectHolder if absent and returns an Observable of JobSchedulingInfo to the caller
     *
     * @param lookupKey - Job cluster name or JobID
     *
     * @return
     */

    public Observable<JobSchedulingInfo> jobDiscoveryInfoStream(JobDiscoveryLookupKey lookupKey) {
        Preconditions.checkNotNull(lookupKey, "lookup key cannot be null for fetching job discovery info");
        Preconditions.checkArgument(lookupKey.getId() != null && !lookupKey.getId().isEmpty(), "Lookup ID cannot be null or empty" + lookupKey);
        subjectMapSizeGauge.set(subjectMap.size());
        return subjectMap.computeIfAbsent(lookupKey, (jc) -> new JobDiscoveryInfoSubjectHolder(mantisClient, jc, removeSubjectAction, this.retryCount, registry)).jobDiscoveryInfoStream();
    }

    /**
     * Intended to be called via a callback when subscriber count falls to 0
     *
     * @param lookupKey JobId whose entry needs to be removed
     */
    private void removeSchedulingInfoSubject(JobDiscoveryLookupKey lookupKey) {
        subjectMap.remove(lookupKey);
        subjectMapSizeGauge.set(subjectMap.size());
    }

    /**
     * For testing purposes
     *
     * @return No. of entries in the subject
     */
    int getSubjectMapSize() {
        return subjectMap.size();
    }

    /**
     * For testing purposes
     */
    void clearMap() {
        subjectMap.clear();
    }
}

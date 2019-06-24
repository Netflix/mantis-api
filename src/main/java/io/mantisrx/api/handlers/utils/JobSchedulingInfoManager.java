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
 * A Cache for Subjects per JobId holding schedulingInfo Changes.
 * This is a singleton
 */
public class JobSchedulingInfoManager {

    private static Logger logger = LoggerFactory.getLogger(JobSchedulingInfoManager.class);

    private final MantisClient mantisClient;
    private final AtomicDouble subjectMapSizeGauge;
    private final Registry registry;

    private int retryCount = 5;
    private static JobSchedulingInfoManager INSTANCE = null;

    public static synchronized JobSchedulingInfoManager getInstance(MantisClient mantisClient, Registry registry) {
        if (INSTANCE == null) {
            INSTANCE = new JobSchedulingInfoManager(mantisClient, registry);
        }
        return INSTANCE;
    }

    private JobSchedulingInfoManager(final MantisClient mClient, final Registry registry) {
        Preconditions.checkNotNull(mClient, "mantisClient cannot be null");
        this.mantisClient = mClient;
        this.subjectMapSizeGauge = SpectatorUtils.buildAndRegisterGauge(registry, "mantisapi.schedulingChanges.subjectMapSize");
        this.registry = registry;
    }

    /**
     * For testing purposes
     *
     * @param cnt No of retries
     */
    void setRetryCount(int cnt) {
        this.retryCount = cnt;
    }

    private final ConcurrentMap<String, JobSchedulingInfoSubjectHolder> subjectMap = new ConcurrentHashMap<>();

    /**
     * Invoked by the subjectHolders when the subscription count goes to 0 (or if there is an error)
     */
    private final Action1<String> removeSubjectAction = jobId -> {
        if (logger.isDebugEnabled()) { logger.info("Removing subject for jobid " + jobId); }
        removeSchedulingInfoSubject(jobId);
    };

    /**
     * Atomically inserts a JobSchedulingSubjectHolder if absent and returns an Observable of JobSchedulingInfo to the caller
     *
     * @param jobId - JobId of job whose schedulingChanges are requested
     *
     * @return
     */

    public Observable<JobSchedulingInfo> getSchedulingInfoObservable(String jobId) {
        Preconditions.checkNotNull(jobId, "jobId cannot be null");
        Preconditions.checkArgument(!jobId.isEmpty(), "JobId cannot be empty" + jobId);
        subjectMapSizeGauge.set(subjectMap.size());
        return subjectMap.computeIfAbsent(jobId, (jId) -> new JobSchedulingInfoSubjectHolder(mantisClient, jId, removeSubjectAction, this.retryCount, registry)).getSchedulingChanges();
    }

    /**
     * Intended to be called via a callback when subscriber count falls to 0
     *
     * @param jobId JobId whose entry needs to be removed
     */
    private void removeSchedulingInfoSubject(String jobId) {
        subjectMap.remove(jobId);
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

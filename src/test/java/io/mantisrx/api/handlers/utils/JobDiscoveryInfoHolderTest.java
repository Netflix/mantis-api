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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.NoopRegistry;
import com.netflix.spectator.api.Registry;
import io.mantisrx.client.MantisClient;
import io.mantisrx.server.core.JobSchedulingInfo;
import io.mantisrx.server.core.WorkerAssignments;
import org.junit.Ignore;
import org.junit.Test;
import rx.Observable;


public class JobDiscoveryInfoHolderTest {

    Registry registry = new NoopRegistry();

    @Test
    public void testSubCounting() {
        Map<Integer, WorkerAssignments> dummyMap = new HashMap<>();
        Observable<JobSchedulingInfo> schedObs = Observable.interval(200, TimeUnit.MILLISECONDS).map((d) -> new JobSchedulingInfo("j1", dummyMap));
        MantisClient mc = mock(MantisClient.class);
        when(mc.jobClusterDiscoveryInfoStream(any())).thenReturn(schedObs);
        int cLatchCnt1 = 5;
        int cLatchCnt2 = 3;
        int cLatchCnt4 = 2;
        CountDownLatch cLatch1 = new CountDownLatch(cLatchCnt1);
        CountDownLatch cLatch2 = new CountDownLatch(cLatchCnt2);
        CountDownLatch cLatch3 = new CountDownLatch(2);
        CountDownLatch cLatch4 = new CountDownLatch(cLatchCnt4);
        JobDiscoveryInfoSubjectHolder jsis = new JobDiscoveryInfoSubjectHolder(mc,
                new JobDiscoveryLookupKey(JobDiscoveryLookupKey.LookupType.JOB_CLUSTER, "j"), o -> {
            System.out.println("clean up called");
            cLatch3.countDown();
        }, registry);

        Thread thread1 = getThread1(cLatchCnt1, cLatch1, jsis);

        Thread thread2 = getThread1(cLatchCnt2, cLatch2, jsis);
        thread1.start();
        thread2.start();

        try {
            cLatch1.await(2, TimeUnit.SECONDS);
            cLatch2.await(2, TimeUnit.SECONDS);
            cLatch3.await(2, TimeUnit.SECONDS);

            Thread thread4 = getThread1(cLatchCnt4, cLatch4, jsis);
            thread4.start();

            cLatch4.await(4, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testJobSchedulingObsCompletes() {
        Map<Integer, WorkerAssignments> dummyMap = new HashMap<>();
        Observable<JobSchedulingInfo> schedObs = Observable.interval(200, TimeUnit.MILLISECONDS)
                .map((d) -> new JobSchedulingInfo("j1", dummyMap))
                .take(2);
        MantisClient mc = mock(MantisClient.class);
        when(mc.jobClusterDiscoveryInfoStream(any())).thenReturn(schedObs);
        int cLatchCnt1 = 2;
        int cLatchCnt2 = 2;

        CountDownLatch cLatch1 = new CountDownLatch(cLatchCnt1);
        CountDownLatch cLatch2 = new CountDownLatch(cLatchCnt2);
        CountDownLatch cLatch3 = new CountDownLatch(1);

        JobDiscoveryInfoSubjectHolder jsis = new JobDiscoveryInfoSubjectHolder(mc,
                new JobDiscoveryLookupKey(JobDiscoveryLookupKey.LookupType.JOB_CLUSTER, "j1"), o -> {
            System.out.println("clean up called");
            cLatch3.countDown();
        }, registry);

        Thread thread1 = getThread1(cLatchCnt1, cLatch1, jsis);

        Thread thread2 = getThread1(cLatchCnt2, cLatch2, jsis);
        thread1.start();
        thread2.start();

        try {
            cLatch1.await(2, TimeUnit.SECONDS);
            cLatch2.await(2, TimeUnit.SECONDS);
            cLatch3.await(2, TimeUnit.SECONDS);


        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testJobSchedulingObsErrors() {
        Map<Integer, WorkerAssignments> dummyMap = new HashMap<>();
        Observable<JobSchedulingInfo> schedObs = Observable.interval(200, TimeUnit.MILLISECONDS)
                .map((d) -> {
                    System.out.println("d --> " + d);
                    if (d == 1) {
                        System.out.println("Throwing exception!!");
                        throw new RuntimeException("Forced exception");
                    }
                    return d;
                })
                .map((d) -> new JobSchedulingInfo("j1", dummyMap));
        MantisClient mc = mock(MantisClient.class);
        when(mc.jobClusterDiscoveryInfoStream(any())).thenReturn(schedObs);


        CountDownLatch cLatch3 = new CountDownLatch(1);

        JobDiscoveryInfoSubjectHolder jsis = new JobDiscoveryInfoSubjectHolder(mc,
                new JobDiscoveryLookupKey(JobDiscoveryLookupKey.LookupType.JOB_CLUSTER, "j1"), o -> {
            System.out.println("clean up called1");
            cLatch3.countDown();
        }, 1, registry);

        Thread thread1 = new Thread(() -> jsis.jobDiscoveryInfoStream()

                .doOnCompleted(() -> System.out.println("doOnCompleted"))
                .doOnError((t) -> System.out.println("error --> " + t.getMessage()))
                .toBlocking()
                .subscribe());

        Thread thread2 = new Thread(() -> jsis.jobDiscoveryInfoStream()

                .doOnCompleted(() -> System.out.println("doOnCompleted"))
                .doOnError((t) -> System.out.println("error --> " + t.getMessage()))
                .toBlocking()
                .subscribe());

        thread1.start();
        thread2.start();

        try {
            cLatch3.await(4, TimeUnit.SECONDS);


        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static Thread getThread1(int cLatchCnt1, CountDownLatch cLatch1, JobDiscoveryInfoSubjectHolder jsis) {
        return new Thread(() -> jsis.jobDiscoveryInfoStream()
                .map((d) -> {
                    cLatch1.countDown();
                    return d;
                })
                .take(cLatchCnt1)
                .doOnCompleted(() -> System.out.println("doOnCompleted"))
                .doOnError((t) -> System.out.println("error --> " + t.getMessage()))
                .toBlocking()
                .subscribe());
    }

    @Test
    @Ignore
    public void testSubCountingWithManager() {
        Map<Integer, WorkerAssignments> dummyMap = new HashMap<>();
        Observable<JobSchedulingInfo> schedObs = Observable.interval(200, TimeUnit.MILLISECONDS).map((d) -> new JobSchedulingInfo("j1", dummyMap));
        MantisClient mc = mock(MantisClient.class);
        when(mc.jobClusterDiscoveryInfoStream(any())).thenReturn(schedObs);
        int cLatchCnt1 = 5;
        int cLatchCnt2 = 3;
        int cLatchCnt4 = 2;
        CountDownLatch cLatch1 = new CountDownLatch(cLatchCnt1);
        CountDownLatch cLatch2 = new CountDownLatch(cLatchCnt2);

        CountDownLatch cLatch4 = new CountDownLatch(cLatchCnt4);

        JobDiscoveryInfoManager jInfoManager = JobDiscoveryInfoManager.getInstance(mc, registry);

        Thread thread1 = getThread1(jInfoManager, cLatchCnt1, cLatch1, jInfoManager.jobDiscoveryInfoStream(new JobDiscoveryLookupKey(JobDiscoveryLookupKey.LookupType.JOB_CLUSTER, "jobCluster")));

        Thread thread2 = getThread1(jInfoManager, cLatchCnt2, cLatch2, jInfoManager.jobDiscoveryInfoStream(new JobDiscoveryLookupKey(JobDiscoveryLookupKey.LookupType.JOB_CLUSTER, "jobCluster")));
        thread1.start();
        thread2.start();

        try {
            cLatch1.await(2, TimeUnit.SECONDS);
            cLatch2.await(2, TimeUnit.SECONDS);
            //            assertEquals(0, jInfoManager.getSubjectMapSize());
            Thread thread4 = getThread1(jInfoManager, cLatchCnt4, cLatch4, jInfoManager.jobDiscoveryInfoStream(new JobDiscoveryLookupKey(JobDiscoveryLookupKey.LookupType.JOB_CLUSTER, "jobCluster")));
            thread4.start();

            cLatch4.await(10, TimeUnit.SECONDS);
            assertEquals(0, jInfoManager.getSubjectMapSize());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    @Ignore
    public void testJobSchedulingObsErrorsWithManager() {
        Map<Integer, WorkerAssignments> dummyMap = new HashMap<>();
        Observable<JobSchedulingInfo> schedObs = Observable.interval(200, TimeUnit.MILLISECONDS)
                .map((d) -> {
                    System.out.println("d --> " + d);
                    if (d == 1) {
                        System.out.println("Throwing exception!!");
                        throw new RuntimeException("Forced exception");
                    }
                    return d;
                })
                .map((d) -> new JobSchedulingInfo("j1", dummyMap));
        MantisClient mc = mock(MantisClient.class);
        when(mc.jobClusterDiscoveryInfoStream(any())).thenReturn(schedObs);


        CountDownLatch cLatch3 = new CountDownLatch(2);

        JobDiscoveryInfoManager jInfoManager = JobDiscoveryInfoManager.getInstance(mc, registry);
        jInfoManager.setRetryCount(0);
        Thread thread1 = new Thread(() -> jInfoManager.jobDiscoveryInfoStream(new JobDiscoveryLookupKey(JobDiscoveryLookupKey.LookupType.JOB_CLUSTER, "jobCluster"))
                .map((d) -> {
                    assertEquals(1, jInfoManager.getSubjectMapSize());
                    return d;
                })
                .doOnCompleted(() -> System.out.println("doOnCompleted"))
                .doOnError((t) -> {
                    cLatch3.countDown();
                    System.out.println("error --> " + t.getMessage());
                })
                .toBlocking()
                .subscribe());

        Thread thread2 = new Thread(() -> jInfoManager.jobDiscoveryInfoStream(new JobDiscoveryLookupKey(JobDiscoveryLookupKey.LookupType.JOB_CLUSTER, "jobCluster"))
                .map((d) -> {
                    assertEquals(1, jInfoManager.getSubjectMapSize());
                    return d;
                })
                .doOnCompleted(() -> System.out.println("doOnCompleted"))
                .doOnError((t) -> {
                    cLatch3.countDown();
                    System.out.println("error --> " + t.getMessage());
                })
                .toBlocking()
                .subscribe());

        thread1.start();
        thread2.start();

        try {

            cLatch3.await(10, TimeUnit.SECONDS);
            assertEquals(0, jInfoManager.getSubjectMapSize());

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    Thread getThread1(JobDiscoveryInfoManager jInfoManager, int cLatchCnt1, CountDownLatch cLatch1, Observable<JobSchedulingInfo> sObs) {
        return new Thread(() -> sObs
                .map((d) -> {
                    cLatch1.countDown();
                    assertEquals(1, jInfoManager.getSubjectMapSize());
                    return d;
                })
                .take(cLatchCnt1)
                .doOnCompleted(() -> System.out.println("doOnCompleted"))
                .doOnError((t) -> System.out.println("error --> " + t.getMessage()))
                .toBlocking()
                .subscribe());
    }

}

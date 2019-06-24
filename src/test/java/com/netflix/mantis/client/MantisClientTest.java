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

package com.netflix.mantis.client;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.netflix.archaius.api.Property;
import com.netflix.spectator.api.NoopRegistry;
import com.netflix.spectator.api.Registry;
import io.mantisrx.api.handlers.utils.JobDiscoveryInfoSubjectHolder;
import io.mantisrx.api.handlers.utils.JobDiscoveryLookupKey;
import io.mantisrx.api.handlers.utils.MantisClientUtil;
import io.mantisrx.client.MantisClient;
import io.mantisrx.client.SinkConnectionsStatus;
import io.mantisrx.server.master.client.MasterClientWrapper;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.schedulers.Schedulers;


public class MantisClientTest {

    static Properties zkProps = new Properties();
    private static final int sinkStageNumber = 3;

    private static Registry registry = new NoopRegistry();
    private static Property<Boolean> cacheEnabled = mock(Property.class);

    static {
        when(cacheEnabled.get()).thenReturn(false);
    }

    MasterClientWrapper clientWrapper;

    static {
        zkProps.put("mantis.zookeeper.connectString", "100.67.80.172:2181,100.67.71.221:2181,100.67.89.26:2181,100.67.71.34:2181,100.67.80.18:2181");
        zkProps.put("mantis.zookeeper.leader.announcement.path", "/leader");
        zkProps.put("mantis.zookeeper.root", "/mantis/master");
    }

    private static final Logger logger = LoggerFactory.getLogger(MantisClientTest.class);

    static class NoOpSinkConnectionsStatusObserver implements Observer<SinkConnectionsStatus> {

        @Override
        public void onCompleted() {
            logger.warn("Got Completed on SinkConnectionStatus ");

        }

        @Override
        public void onError(Throwable e) {
            logger.error("Got Error on SinkConnectionStatus ", e);

        }

        @Override
        public void onNext(SinkConnectionsStatus t) {
            logger.info("Got Sink Connection Status update " + t);

        }

    }

    @Before
    public void init() {
        clientWrapper = new MasterClientWrapper(zkProps);
    }

    //    @Test
    public void jobDiscoveryInfoTest() {
        CountDownLatch cLatch = new CountDownLatch(2);
        MantisClient mClient = new MantisClient(zkProps);
        JobDiscoveryInfoSubjectHolder sHolder = new JobDiscoveryInfoSubjectHolder(mClient, new JobDiscoveryLookupKey(JobDiscoveryLookupKey.LookupType.JOB_CLUSTER, "SineFnTest"), s -> {

        }, registry);
        sHolder.jobDiscoveryInfoStream().map((jSched) -> String.valueOf(jSched))
                .take(1)
                .subscribeOn(Schedulers.io()).subscribe((data) -> {
            cLatch.countDown();
            System.out.println(data);
        });

        sHolder.jobDiscoveryInfoStream().map((jSched) -> String.valueOf(jSched))
                .take(1)
                .subscribeOn(Schedulers.io()).subscribe((data) -> {
            cLatch.countDown();
            System.out.println("sub2 " + data);
        });

        try {
            cLatch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

    public void callGetOnMasterTest() {
        String uri = "api/jobs/list/all";
        CountDownLatch gotData = new CountDownLatch(1);

        MantisClientUtil.callGetOnMasterWithCache(clientWrapper.getMasterMonitor().getMasterObservable(), "/" + uri, cacheEnabled)
                .map((result) -> {
                    System.out.println("res1 " + result);
                    return result;
                })

                .take(1)
                .toBlocking()
                .subscribe((data) -> {
                    assertTrue(data != null && data._2().contains("jobMetadata"));
                    gotData.countDown();
                    System.out.println("got data-> " + data);

                });
        ;
        try {

            gotData.await(10, TimeUnit.SECONDS);

        } catch (InterruptedException e) {
            fail();
        }
    }

    //@Test
    public void callGetOnMasterWithCacheTest() {
        String uri = "api/jobs/list/all";
        CountDownLatch gotData = new CountDownLatch(1);
        Observable<String> ob1 =
                MantisClientUtil.callGetOnMasterWithCache(clientWrapper.getMasterMonitor().getMasterObservable(), "/" + uri, cacheEnabled)
                        .map((result) -> {
                            System.out.println("res1 " + result);
                            return result._2;
                        });

        Observable<String> ob2 = MantisClientUtil.callGetOnMasterWithCache(clientWrapper.getMasterMonitor().getMasterObservable(), "/" + uri, cacheEnabled)
                .map((result) -> {
                    System.out.println("res2 " + result);
                    return result._2;
                });
        ob1.zipWith(ob2, (s1, s2) -> {

            if (s1.equals(s2)) {
                return true;
            }
            return false;
        })
                .take(1)
                .toBlocking()
                .subscribe((data) -> {
                    assertTrue(data);
                    gotData.countDown();
                    System.out.println("got data-> " + data);

                });
        ;
        try {

            gotData.await(10, TimeUnit.SECONDS);

        } catch (InterruptedException e) {
            fail();
        }
    }

    // @Test
    public void callGetOnMasterErrors() {
        String uri = "blah/all";
        CountDownLatch dataReceived = new CountDownLatch(1);
        MantisClientUtil.callGetOnMasterWithCache(clientWrapper.getMasterMonitor().getMasterObservable(), "/" + uri, cacheEnabled)
                .map((result) -> {
                    System.out.println("Result -> " + result);

                    return result;
                }).flatMap((r) -> {
            return MantisClientUtil.callGetOnMasterWithCache(clientWrapper.getMasterMonitor().getMasterObservable(), "/" + uri, cacheEnabled)
                    .map((result) -> {
                        System.out.println("Result2 -> " + result);

                        return result;
                    });
        })
                .onErrorResumeNext((thr) -> Observable.empty())
                .toBlocking()
                .subscribe((res) -> {

                }, (th) -> {
                    th.printStackTrace();
                    dataReceived.countDown();
                });
        ;
        try {

            dataReceived.await(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fail();
        }
    }

}




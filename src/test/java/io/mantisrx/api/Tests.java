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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.mantis.discovery.proto.AppJobClustersMap;
import com.netflix.mantis.discovery.proto.StreamJobClusterMap;
import io.mantisrx.api.handlers.servlets.MREAppStreamToJobClusterMappingServlet;
import io.mantisrx.api.handlers.utils.JacksonObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Ignore
public class Tests {

    private static MantisAPIServer server;
    private static final Logger logger = LoggerFactory.getLogger(Tests.class);

    private static Properties getProperties(String propertiesFile) {
        Properties props = new Properties();
        if (propertiesFile != null) {
            // Load configuration from the provided file
            try (FileInputStream infile = new FileInputStream(propertiesFile)) {
                props.load(infile);
            } catch (IOException e) {
                throw new IllegalArgumentException("Cannot load configuration from file " + propertiesFile);
            }
        }
        return props;
    }

    public static MantisAPIServer getServer() {
        String propFile = "conf/nfmantisapi-local.properties";
        Injector injector = Guice.createInjector(new MantisAPITestingModule(propFile));
        MantisAPIServer server = injector.getInstance(MantisAPIServer.class);
        return server;
    }

    @BeforeClass
    public static void setup() throws Exception {
        server = getServer();
        server.start();
    }

    @AfterClass
    public static void teardown() {
        server.stop();
    }

    @Test
    @Ignore
    public void healthcheckShouldRespondOkay() throws Exception {

        // given
        String url = "http://localhost:7101/healthcheck";

        HttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(url);
        HttpResponse response = client.execute(request);

        // then
        assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
    }

    @Test
    @Ignore
    public void streamJobClustersMapShouldRespondOkay() throws Exception {

        /*
         * configured value in nfmantisapi-local.properties
         * {"version": "1","timestamp":12345,"mappings": {"__default__": {"requestEventStream": "SharedPushRequestEventSource","sentryEventStream": "SentryLogEventSource","__any__": "SharedPushEventSource"},"customApp": {"logEventStream": "CustomAppEventSource","sentryEventStream": "CustomAppSentryLogSource"}}}
         * */
        // given
        String appName = "customApp";
        String url = "http://localhost:7101" + MREAppStreamToJobClusterMappingServlet.PATH_SPEC + "?app=" + appName;

        HttpClient client = HttpClientBuilder.create().build();
        HttpGet request = new HttpGet(url);
        HttpResponse response = client.execute(request);

        // then
        assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);

        AppJobClustersMap appJobClustersMap = JacksonObjectMapper.getInstance().readValue(response.getEntity().getContent(), AppJobClustersMap.class);
        assertEquals("1", appJobClustersMap.getVersion());
        assertEquals(12345, appJobClustersMap.getTimestamp());
        StreamJobClusterMap streamJobClusterMap = appJobClustersMap.getStreamJobClusterMap(appName);
        assertEquals(appName, streamJobClusterMap.getAppName());
        assertEquals("CustomAppEventSource", streamJobClusterMap.getJobCluster("logEventStream"));
        assertEquals("CustomAppSentryLogSource", streamJobClusterMap.getJobCluster("sentryEventStream"));
        assertEquals("SharedPushEventSource", streamJobClusterMap.getJobCluster("__any__"));
        assertEquals("SharedPushRequestEventSource", streamJobClusterMap.getJobCluster("requestEventStream"));
        logger.info("{}", appJobClustersMap);
    }
}

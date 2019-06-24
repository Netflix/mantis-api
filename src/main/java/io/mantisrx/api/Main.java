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

import java.util.HashMap;
import java.util.Properties;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import io.mantisrx.api.metrics.MantisMetricsAdapter;
import io.mantisrx.server.core.metrics.MetricsPublisherService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main {

    private static Logger logger = LoggerFactory.getLogger(Main.class);
    private static boolean debug = false;

    @Argument(alias = "p", description = "Specify a configuration file", required = false)
    private static String propFile = "conf/local.properties";

    public static void main(String[] args) {
        try {
            Args.parse(Main.class, args);
        } catch (IllegalArgumentException e) {
            Args.usage(Main.class);
            System.exit(1);
        }

        try {
            Injector injector = Guice.createInjector(new MantisAPIModule(propFile));
            MantisAPIServer server = injector.getInstance(MantisAPIServer.class);
            server.start();

            Property<Integer> publishFrequency = injector.getInstance(PropertyRepository.class)
                    .get("mantis.metricsPublisher.publishFrequencyInSeconds", Integer.class).orElse(15);
            Properties properties = injector.getInstance(Properties.class);
            publishMantisMetrics(publishFrequency.get(), properties);
        } catch (Exception e) {
            logger.error("Can't initialize: " + e.getMessage(), e);
        }
    }

    private static void publishMantisMetrics(Integer frequency, Properties properties) {
        new MetricsPublisherService(new MantisMetricsAdapter(properties), frequency, new HashMap<>())
                .start();
    }
}

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

package io.mantisrx.api.metrics;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// TODO: Does this class currently do anything?
public class MantisMetricsAdapter extends MetricsPublisher {

    private static final Logger logger = LoggerFactory.getLogger(MantisMetricsAdapter.class);
    private final Cache<String, Long> cache;

    public MantisMetricsAdapter(Properties properties) {
        super(properties);
        cache = CacheBuilder
                .newBuilder()
                .expireAfterAccess(30, TimeUnit.MINUTES)
                .build();
    }

    @Override
    public void publishMetrics(long timestamp, Collection<Metrics> currentMetricsRegistered) {
        //        if(currentMetricsRegistered != null && !currentMetricsRegistered.isEmpty()) {
        //            for(Metrics metrics: currentMetricsRegistered) {
        //
        //                final String prefix = metrics.getMetricGroupId().name();
        //                final Map<MetricId, Counter> counters = metrics.counters();
        //                for(Map.Entry<MetricId, Counter> entry: counters.entrySet()) {
        //                    long currVal = entry.getValue().value();
        //                    String name = prefix + "_" + entry.getKey();
        //                    Long prev = null;
        //                    try {
        //                        prev = cache.get(name, () -> 0L);
        //                        DynamicCounter.increment(name, null, currVal - prev);
        //                        cache.put(name, currVal);
        //                    } catch (ExecutionException e) {
        //                        logger.warn(e.getMessage(), e);
        //                    }
        //                }
        //                final Map<MetricId, Gauge> gauges = metrics.gauges();
        //                for(Map.Entry<MetricId, Gauge> entry: gauges.entrySet()) {
        //                    String name = prefix + "_" + entry.getKey();
        //                    DynamicGauge.set(name, null, entry.getValue().value());
        //                }
        //            }
        //        }
    }
}

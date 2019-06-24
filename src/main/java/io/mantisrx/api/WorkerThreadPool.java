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

import java.util.concurrent.ScheduledThreadPoolExecutor;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.ThreadPoolMonitor;


/**
 * Provides a global threadpool in which components of the API can schedule work.
 * This is useful for constraining the global number of threads in a single location, and typically used for long
 * running connections to job sinks.
 */
@Singleton
public class WorkerThreadPool extends ScheduledThreadPoolExecutor {

    @Inject
    public WorkerThreadPool(Registry registry, @Named("threads") Integer threads) {
        super(threads);
        ThreadPoolMonitor.attach(registry, this, "mantisapi-worker-pool");
    }
}

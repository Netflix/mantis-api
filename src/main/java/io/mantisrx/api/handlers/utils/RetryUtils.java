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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import rx.Observable;
import rx.functions.Func1;


public class RetryUtils {

    private static final int defaultNumRetries = 10;

    public static Func1<Observable<? extends Throwable>, Observable<?>> getRetryFunc(final Logger logger) {
        return getRetryFunc(logger, defaultNumRetries);
    }

    public static Func1<Observable<? extends Throwable>, Observable<?>> getRetryFunc(final Logger logger, final int retries) {
        final int limit = retries == Integer.MAX_VALUE ? retries : retries + 1;
        return attempts -> attempts
                .zipWith(Observable.range(1, limit), (t1, integer) -> {
                    logger.warn("caught exception", t1);
                    return new ImmutablePair<Throwable, Integer>(t1, integer);
                })
                .flatMap(pair -> {
                    Throwable t = pair.left;
                    int retryIter = pair.right;
                    long delay = 2 * (retryIter > 10 ? 10 : retryIter);
                    if (retryIter > retries) {
                        logger.error("exceeded max retries {} last exception {}", retries, t.getMessage(), t);
                        return Observable.error(new Exception("Timeout after " + retries + " retries"));
                    }
                    logger.info(": retrying conx after sleeping for {} secs", delay, t);
                    return Observable.timer(delay, TimeUnit.SECONDS);
                });
    }

    public static Func1<Observable<? extends Throwable>, Observable<?>> getRetryFuncConditional(
            final Logger logger, final int retries, final AtomicBoolean doRetry
    ) {
        return attempts -> attempts
                .zipWith(Observable.range(1, Integer.MAX_VALUE), (t1, integer) -> integer)
                .flatMap(integer -> {
                    if (doRetry.get()) {
                        long delay = 2 * (integer > 5 ? 5 : integer);
                        logger.info(": retrying conx after sleeping for " + delay + " secs");
                        return Observable.timer(delay, TimeUnit.SECONDS);
                    }
                    return Observable.error(new Exception("Not retriable error"));
                });
    }
}

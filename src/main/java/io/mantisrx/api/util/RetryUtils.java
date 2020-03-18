/**
 * Copyright 2018 Netflix, Inc.
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
package io.mantisrx.api.util;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import rx.Observable;
import rx.functions.Func1;

import java.util.concurrent.TimeUnit;

public class RetryUtils {

    private static final int defaultNumRetries = 5;

    public static Func1<Observable<? extends Throwable>, Observable<?>> getRetryFunc(final Logger logger, String name) {
        return getRetryFunc(logger, name, defaultNumRetries);
    }

    public static Func1<Observable<? extends Throwable>, Observable<?>> getRetryFunc(final Logger logger, String name, final int retries) {
        final int limit = retries == Integer.MAX_VALUE ? retries : retries + 1;
        return attempts -> attempts
                .zipWith(Observable.range(1, limit), (t1, integer) -> {
                    logger.warn("Caught exception connecting for {}.", name, t1);
                    return new ImmutablePair<Throwable, Integer>(t1, integer);
                })
                .flatMap(pair -> {
                    Throwable t = pair.left;
                    int retryIter = pair.right;
                    long delay = Math.round(Math.pow(2, retryIter));

                    if (retryIter > retries) {
                        logger.error("Exceeded maximum retries ({}) for {} with exception: {}", retries, name, t.getMessage(), t);
                        return Observable.error(new Exception("Timeout after " + retries + " retries"));
                    }
                    logger.info("Retrying connection to {} after sleeping for {} seconds.", name, delay, t);
                    return Observable.timer(delay, TimeUnit.SECONDS);
                });
    }
}
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

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StatsImpl implements Stats {

    private static final long delaySecs = 23;
    private static final Logger logger = LoggerFactory.getLogger(StatsImpl.class);
    private volatile ScheduledFuture<?> bpsEvaltrFuture = null;
    private final AtomicLong totalNumMessages = new AtomicLong();
    private final AtomicLong totalDroppedNumMessages = new AtomicLong();
    private final AtomicLong totalNumBytes = new AtomicLong();
    private final AtomicLong totalDroppedNumBytes = new AtomicLong();
    private volatile double messagesPerSec = 0.0;
    private volatile double messageBitsPerSec = 0.0;
    @JsonIgnore
    private long prevNumMsgs = 0L;
    @JsonIgnore
    private long prevNumBytes = 0L;
    @JsonIgnore
    private final long id;
    @JsonIgnore
    private long lastDataSentAt = System.currentTimeMillis();

    public StatsImpl(long id) {
        this.id = id;
    }

    @JsonCreator
    public StatsImpl() {
        id = -1;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {
        logger.info("Stopping bps evaluator id=" + id);
        if (bpsEvaltrFuture != null)
            bpsEvaltrFuture.cancel(true);
    }

    @Override
    public long getLastDataSentAt() {
        return lastDataSentAt;
    }

    @Override
    public void evalStats() {
        logger.info("evaluating bps, id=" + id);
        // crudely simple window based rate, with an appropriately long window
        long l = totalNumBytes.get();
        messageBitsPerSec = get2decimalDouble((double) (l - prevNumBytes) * 8.0 / (double) delaySecs);
        prevNumBytes = l;
        l = totalNumMessages.get();
        if (l > prevNumMsgs)
            lastDataSentAt = System.currentTimeMillis();
        messagesPerSec = get2decimalDouble((double) (l - prevNumMsgs) / (double) delaySecs);
        prevNumMsgs = l;
    }

    private double get2decimalDouble(double value) {
        return (double) ((long) (value * 100.0)) / 100.0;
    }

    @Override
    public void incrementNumMessages(long n) {
        totalNumMessages.addAndGet(n);
    }

    @Override
    public void incrementDroppedMessages(long n) {
        totalDroppedNumMessages.addAndGet(n);
    }

    @Override
    public void incrementNumBytes(long n) {
        totalNumBytes.addAndGet(n);
    }

    @Override
    public void incrementDroppedBytes(long n) {
        totalDroppedNumBytes.addAndGet(n);
    }

    public AtomicLong getTotalNumMessages() {
        return totalNumMessages;
    }

    public AtomicLong getTotalDroppedNumMessages() {
        return totalDroppedNumMessages;
    }

    public AtomicLong getTotalNumBytes() {
        return totalNumBytes;
    }

    public AtomicLong getTotalDroppedNumBytes() {
        return totalDroppedNumBytes;
    }

    public double getMessagesPerSec() {
        return messagesPerSec;
    }

    public double getMessageBitsPerSec() {
        return messageBitsPerSec;
    }
}

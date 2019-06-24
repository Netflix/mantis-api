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

import java.text.DateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.impl.AtomicDouble;
import io.mantisrx.api.handlers.servlets.MantisAPIRequestHandler;
import io.mantisrx.api.handlers.utils.HttpUtils;
import io.mantisrx.api.metrics.Stats;
import io.mantisrx.api.metrics.StatsImpl;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import mantis.io.reactivex.netty.protocol.http.server.HttpServerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Action0;
import rx.functions.Action1;


public class SessionContextBuilder {

    public class StatsHandler implements MantisAPIRequestHandler {

        public static final String endpointName = "sessionstats";

        @Override
        public String getName() {
            return endpointName; // same as the endpoint
        }

        @Override
        public String getHelp() {
            return StatsHandler.endpointName;
        }

        @Override
        public Observable<Void> handleOptions(HttpServerResponse<ByteBuf> response, Request request) {
            HttpUtils.setBaseHeaders(response.getHeaders(), HttpMethod.GET, HttpMethod.OPTIONS);
            response.setStatus(HttpResponseStatus.NO_CONTENT);
            return response.close();
        }

        @Override
        public Observable<Void> handleGet(HttpServerResponse<ByteBuf> response, Request request) {
            HttpUtils.setBaseHeaders(response.getHeaders(), HttpMethod.GET, HttpMethod.OPTIONS);
            try {
                return response.writeStringAndFlush(objectMapper.writeValueAsString(activeSessions));
            } catch (JsonProcessingException e) {
                logger.warn("Error writing session stats json string " + e.getMessage(), e);
                return response.writeStringAndFlush("Error getting session stats: " + e.getMessage());
            }
        }

        @Override
        public Observable<Void> handlePost(HttpServerResponse<ByteBuf> response, Request request) {
            return Observable.error(new UnsupportedOperationException("POST not supported"));
        }
    }

    public static class SessionContextImpl implements SessionContext {

        private final long id;
        private final long createTime;
        private final String remoteAddress;
        private final String openedAt;
        private final String uri;
        private final String method;
        private final Stats stats;
        private final ConxType type;
        private List<String> regions = ImmutableList.of();
        @JsonIgnore
        private final ConxRefs conxRefs;
        @JsonIgnore
        private Action1<Long> endSessionTrigger = null;
        @JsonIgnore
        private Action0 endSessionListener = null;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public SessionContextImpl(
                @JsonProperty("id") long id,
                @JsonProperty("remoteAddress") String remoteAddress,
                @JsonProperty("uri") String uri,
                @JsonProperty("method") String method,
                @JsonProperty("stats") StatsImpl stats,
                @JsonProperty("type") ConxType type) {
            this.id = id;
            Date d = new Date();
            createTime = d.getTime();
            final DateFormat dateFormatter = DateFormat.getDateTimeInstance();
            this.remoteAddress = remoteAddress;
            openedAt = dateFormatter.format(d);
            this.uri = uri;
            this.method = method;
            this.stats = stats;
            if (stats != null)
                stats.start();
            this.type = type;
            this.conxRefs = new ConxRefs(type);
        }

        @Override
        public long getId() {
            return id;
        }

        @Override
        public long createTime() {
            return createTime;
        }

        @Override
        public String getRemoteAddress() {
            return remoteAddress;
        }

        public String getOpenedAt() {
            return openedAt;
        }

        @Override
        public String getUri() {
            return uri;
        }

        @Override
        public String getMethod() {
            return method;
        }

        @Override
        public Stats getStats() {
            return stats;
        }

        public ConxType getType() {
            return type;
        }

        @Override
        public List<String> getRegions() {
            return regions;
        }

        @Override
        public void setRegions(List<String> regions) {
            this.regions = regions;
        }

        @Override
        public void endSession() {
            if (endSessionTrigger != null)
                endSessionTrigger.call(id);
            if (stats != null)
                stats.stop();
        }

        @Override
        @JsonIgnore
        public ConxRefs getConxRefs() {
            return conxRefs;
        }

        @JsonIgnore
        public void setEndSessionTrigger(Action1<Long> trigger) {
            this.endSessionTrigger = trigger;
        }

        @JsonIgnore
        public void setEndSessionListener(Action0 action) {
            this.endSessionListener = action;
        }

        @JsonIgnore
        public Action0 getEndSessionListener() {
            return endSessionListener;
        }
    }

    private static Logger logger = LoggerFactory.getLogger(SessionContextBuilder.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static SessionContextBuilder INSTANCE = null;

    private static final long delaySecs = 23;
    private final AtomicLong id = new AtomicLong();
    private ConcurrentMap<Long, SessionContext> activeSessions = new ConcurrentHashMap<>();
    private static final String sessionsCreateCounterName = "numSessionsCreated";
    private static final String sessionsClosedCounterName = "numSessionsClosed";
    private static final String activeSessionsGaugeName = "activeSessions";
    private static final String sessionLengthCounterName = "sessionLength";
    private final AtomicLong activeSessionsCount = new AtomicLong();
    private final StatsHandler statsHandler;

    private PropertyRepository propertyRepository;

    private Registry registry;
    private AtomicDouble activeSessionsGauge;
    private Counter sessionsClosedCounter;

    private SessionContextBuilder(PropertyRepository propertyRepository, Registry registry, WorkerThreadPool threadPool) {
        this.propertyRepository = propertyRepository;
        this.registry = registry;
        this.statsHandler = new StatsHandler();

        sessionsClosedCounter = SpectatorUtils.buildAndRegisterCounter(registry, sessionsClosedCounterName);
        activeSessionsGauge = SpectatorUtils.buildAndRegisterGauge(registry, activeSessionsGaugeName);

        Property<Integer> inactiveTimeoutSecs = propertyRepository.get(PropertyNames.inactiveTimeoutSecs, Integer.class).orElse(300);

        threadPool.scheduleWithFixedDelay(() -> {
            for (Map.Entry<Long, SessionContext> entry : activeSessions.entrySet()) {
                final Stats stats = entry.getValue().getStats();
                stats.evalStats();
                if (stats.getLastDataSentAt() < (System.currentTimeMillis() - 1000L * inactiveTimeoutSecs.get())) {
                    // this conx has no data flowing, close it
                    logger.info("Closing session id=" + entry.getKey() + " due to inactivity for " +
                            inactiveTimeoutSecs.get() + " secs");
                    entry.getValue().endSession();
                }
            }
        }, delaySecs, delaySecs, TimeUnit.SECONDS);
    }

    public static synchronized SessionContextBuilder getInstance(PropertyRepository propertyRepository, Registry registry, WorkerThreadPool threadPool) {
        if (INSTANCE == null) {
            INSTANCE = new SessionContextBuilder(propertyRepository, registry, threadPool);
        }
        return INSTANCE;
    }

    public Map<Long, SessionContext> getActiveSessions() {
        return Collections.unmodifiableMap(activeSessions);
    }

    public SessionContext createHttpSessionCtx(final String remoteAddr, String uri, String method) {
        return createSessionCtx(remoteAddr, uri, method, SessionContext.ConxType.Http);
    }

    public SessionContext createWebSocketSessionCtx(final String remoteAddr, String uri) {
        return createSessionCtx(remoteAddr, uri, "websocket", SessionContext.ConxType.WebSocket);
    }

    private SessionContext createSessionCtx(final String remoteAddr, String uri, String method,
                                            SessionContext.ConxType type) {
        final long newId = id.getAndIncrement();
        logger.info("Creating new session id " + newId + " for client " + remoteAddr + ", method=" + method +
                ", URI=" + uri);

        SpectatorUtils.buildAndRegisterCounter(registry, sessionsCreateCounterName, "urlPath", HttpUtils.getUrlMinusQueryStr(uri), "clientAddr", remoteAddr).increment();
        activeSessionsGauge.set(activeSessionsCount.incrementAndGet());

        final SessionContext context = new SessionContextImpl(newId, remoteAddr, uri, method, new StatsImpl(newId), type);
        ((SessionContextImpl) context).setEndSessionTrigger(this::endSessionContext);
        activeSessions.put(newId, context);
        return context;
    }

    private void endSessionContext(long sessionId) {
        final SessionContext removed = activeSessions.remove(sessionId);
        long length = 0L;
        if (removed != null)
            length = Math.max(System.currentTimeMillis() - removed.createTime(), 0L);
        logger.info("Ending session id {}{} ({} ms)",
                sessionId,
                Optional.ofNullable(removed).map(r -> " for client " + r.getRemoteAddress() + " " + r.getUri()).orElse(""),
                length);
        if (removed != null) {
            sessionsClosedCounter.increment();
            activeSessionsGauge.set(activeSessionsCount.decrementAndGet());
            SpectatorUtils.buildAndRegisterCounter(registry, sessionLengthCounterName, "urlPath", HttpUtils.getUrlMinusQueryStr(removed.getUri())).increment();

            if (removed.getEndSessionListener() != null)
                removed.getEndSessionListener().call();
        }
    }

    MantisAPIRequestHandler getStatsHandler() {
        return statsHandler;
    }
}

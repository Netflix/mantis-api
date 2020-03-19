package io.mantisrx.api.filters;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicIntProperty;
import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Spectator;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.zuul.filters.http.HttpOutboundSyncFilter;
import com.netflix.zuul.message.http.HttpResponseMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@Slf4j
public class MasterCacheLoader extends HttpOutboundSyncFilter {


    @Override
    public boolean needsBodyBuffered(HttpResponseMessage message) {
        return true;
    }

    private static DynamicBooleanProperty cacheEnabled = new DynamicBooleanProperty("mantisapi.cache.enabled", false);
    private static DynamicIntProperty cacheSize = new DynamicIntProperty("mantisapi.cache.size", 1000);
    private static DynamicIntProperty cacheDurationSeconds = new DynamicIntProperty("mantisapi.cache.seconds", 1);

    public static final Cache<String, String> masterCache = CacheBuilder.newBuilder()
            .maximumSize(cacheSize.get())
            .expireAfterWrite(cacheDurationSeconds.get(), TimeUnit.SECONDS)
            .build();

    static {
        PolledMeter.using(Spectator.globalRegistry())
                .withName("mantis.api.cache.size")
                .withTag(new BasicTag("id", "api"))
                .monitorMonotonicCounter(masterCache, Cache::size);
    }

    @Override
    public HttpResponseMessage apply(HttpResponseMessage input) {
        String key = input.getInboundRequest().getPathAndQuery();
        String responseBody = input.getBodyAsText();

        if (null != responseBody && cacheEnabled.get()) {
            masterCache.put(key, responseBody);
        }

        return input;
    }

    @Override
    public int filterOrder() {
        return 999;
    }

    @Override
    public boolean shouldFilter(HttpResponseMessage msg) {
        return msg.getOutboundRequest().getContext().getRouteVIP() != null
                && msg.getOutboundRequest().getContext().getRouteVIP().equalsIgnoreCase("api")
                && msg.getInboundRequest().getMethod().equalsIgnoreCase("get")
                && msg.getHeaders().get("x-nflx-mantisapi-cached").size() == 0; // Set by the MasterCacheHitChecker, ensures we aren't re-caching.
    }
}

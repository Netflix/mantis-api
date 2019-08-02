package io.mantisrx.api.handlers.domain;

import java.util.HashMap;
import java.util.Map;

import io.mantisrx.server.core.JobSchedulingInfo;


public class AppDiscoveryMap {

    public final String version;
    public final Long timestamp;
    public final Map<String, Map<String, JobSchedulingInfo>> mappings = new HashMap<>();

    public AppDiscoveryMap(String version, Long timestamp) {
        this.version = version;
        this.timestamp = timestamp;
    }

    public void addMapping(String app, String stream, JobSchedulingInfo schedulingInfo) {
        if(!mappings.containsKey(app)) {
            mappings.put(app, new HashMap<String, JobSchedulingInfo>());
        }
        mappings.get(app).put(stream, schedulingInfo);
    }
}

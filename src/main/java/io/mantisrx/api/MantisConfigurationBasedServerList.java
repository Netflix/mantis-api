package io.mantisrx.api;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.loadbalancer.ConfigurationBasedServerList;
import com.netflix.loadbalancer.Server;
import com.netflix.niws.loadbalancer.DiscoveryEnabledServer;

import java.util.List;

public class MantisConfigurationBasedServerList extends ConfigurationBasedServerList {
    @Override
    protected List<Server> derive(String value) {
        List<Server> list = Lists.newArrayList();
        if (!Strings.isNullOrEmpty(value)) {
            for (String s : value.split(",")) {
                Server server = new Server(s.trim());
                InstanceInfo instanceInfo =
                        InstanceInfo.Builder.newBuilder()
                                .setAppName("mantismasterv2")
                                .setIPAddr(server.getHost())
                                .setPort(server.getPort())
                                .build();
                list.add(new DiscoveryEnabledServer(instanceInfo, false, true));
            }
        }
        return list;
    }
}

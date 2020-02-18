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

import java.io.FileInputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Properties;

import javax.net.ssl.SSLContext;

import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.archaius.config.MapConfig;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.spectator.api.NoopRegistry;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Spectator;
import io.mantisrx.api.handlers.connectors.RemoteSinkConnector;
import io.mantisrx.api.handlers.domain.Artifact;
import io.mantisrx.api.tunnel.DummyStreamingClientFactory;
import io.mantisrx.api.tunnel.StreamingClientFactory;
import io.mantisrx.client.MantisClient;
import io.mantisrx.server.master.client.MantisMasterClientApi;
import io.mantisrx.server.master.client.MasterClientWrapper;
import io.vavr.Tuple2;
import org.eclipse.jetty.servlet.ServletHolder;


public class MantisAPITestingModule extends AbstractModule {

    protected final String propertiesFile;
    protected static final String CONFIG = "mantisapi";

    public MantisAPITestingModule(String propertiesFile) {
        this.propertiesFile = propertiesFile;
    }

    @Override
    protected void configure() {
        Properties props = new Properties();
        if (propertiesFile != null) {
            // Load configuration from the provided file
            try (FileInputStream infile = new FileInputStream(propertiesFile)) {
                props.load(infile);
            } catch (IOException e) {
                throw new IllegalArgumentException("Cannot load configuration from file " + propertiesFile);
            }
        }

        install(Modules.override(new ArchaiusModule() {
            @Override
            protected void configureArchaius() {
                bindConfigurationName().toInstance(CONFIG);
                bindApplicationConfigurationOverride().toInstance(MapConfig.from(props));
                bind(Properties.class).toInstance(props);
            }
        }).with(new AbstractModule() {
            @Override
            protected void configure() {

            }
        }));

        //
        // Spectator
        //

        Registry registry = new NoopRegistry();
        Spectator.globalRegistry().add(registry);
        bind(Registry.class).toInstance(registry);

        //
        // MantisAPI specific implementations
        //


        bind(Integer.class).annotatedWith(Names.named("threads")).toInstance(256);
        bind(new TypeLiteral<List<Tuple2<String, ServletHolder>>>() {}).annotatedWith(Names.named("servlets")).toInstance(Lists.newArrayList());
        bind(WorkerThreadPool.class).toInstance(new WorkerThreadPool(registry, 4));
        bind(StreamingClientFactory.class).to(DummyStreamingClientFactory.class);

        bind(ArtifactManager.class).to(InMemoryArtifactManager.class);
    }

    @Provides @Singleton
    MasterClientWrapper getMasterClientWrapper(Properties properties) {
        return new MasterClientWrapper(properties);
    }

    @Provides @Singleton
    MantisClient getMantisClient(Properties properties) {
        return new MantisClient(properties);
    }

    @Provides @Singleton
    MantisMasterClientApi getMantisMasterClientApi(MasterClientWrapper masterClientWrapper) {
        return new MantisMasterClientApi(masterClientWrapper.getMasterMonitor());
    }

    @Provides
    MantisAPIServer getServer(MasterClientWrapper masterClientWrapper,
                              MantisClient mantisClient,
                              MantisMasterClientApi mantisMasterClientApi,
                              StreamingClientFactory streamingClientFactory,
                              RemoteSinkConnector remoteSinkConnector,
                              PropertyRepository propertyRepository,
                              Registry registry,
                              ArtifactManager artifactManager,
                              WorkerThreadPool workerThreadPool,
                              @Named("servlets") List<Tuple2<String, ServletHolder>> additionalServlets) throws NoSuchAlgorithmException {

        Property<Integer> port = propertyRepository.get("mantistunnel.server.port", Integer.class).orElse(7101);
        Property<Integer> sslPort = propertyRepository.get("mantistunnel.server.sslPort", Integer.class).orElse(7004);
        return new MantisAPIServer(port.get(), sslPort.get(), mantisClient, masterClientWrapper,
                mantisMasterClientApi, SSLContext.getDefault(),
                remoteSinkConnector, streamingClientFactory, propertyRepository, registry, workerThreadPool,
                artifactManager, additionalServlets);
    }

    @Provides
    RemoteSinkConnector getRemoteSinkConnector(StreamingClientFactory streamingClientFactory, Registry registry) {
        return new RemoteSinkConnector(streamingClientFactory, registry);
    }

}

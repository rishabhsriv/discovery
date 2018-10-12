/*
 * Copyright 2010 Proofpoint, Inc.
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
package com.proofpoint.discovery.store;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.proofpoint.discovery.DiscoveryConfig;
import com.proofpoint.discovery.InitializationTracker;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.http.client.balancing.HttpServiceBalancerStats;
import com.proofpoint.node.NodeInfo;
import com.proofpoint.reporting.ReportCollectionFactory;
import com.proofpoint.reporting.ReportExporter;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.inject.Provider;
import java.lang.annotation.Annotation;
import java.time.Instant;
import java.util.function.Supplier;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.name.Names.named;
import static com.proofpoint.concurrent.Threads.daemonThreadsNamed;
import static com.proofpoint.configuration.ConfigBinder.bindConfig;
import static com.proofpoint.http.client.HttpClientBinder.httpClientBinder;
import static com.proofpoint.jaxrs.JaxrsBinder.jaxrsBinder;
import static com.proofpoint.reporting.ReportBinder.reportBinder;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

/**
 * Expects a LocalStore to be bound elsewhere.
 * Provides a DistributedStore with the specified annotation.
 */
public class ReplicatedStoreModule
    implements Module
{
    private final String name;
    private final Class<? extends Annotation> annotation;
    private final Class<? extends InMemoryStore> localStoreClass;

    public ReplicatedStoreModule(String name, Class<? extends Annotation> annotation, Class<? extends InMemoryStore> localStoreClass)
    {
        this.name = name;
        this.annotation = annotation;
        this.localStoreClass = localStoreClass;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.requireExplicitBindings();
        binder.disableCircularProxies();

        // global
        jaxrsBinder(binder).bind(StoreResource.class).withApplicationPrefix();
        binder.bind(new TypeLiteral<Supplier<Instant>>() {}).to(RealTimeSupplier.class).in(Scopes.SINGLETON);

        // per store
        Key<HttpClient> httpClientKey = Key.get(HttpClient.class, annotation);
        Key<InMemoryStore> localStoreKey = Key.get(InMemoryStore.class, annotation);
        Key<StoreConfig> storeConfigKey = Key.get(StoreConfig.class, annotation);
        Key<RemoteStore> remoteStoreKey = Key.get(RemoteStore.class, annotation);
        Key<UpdateListener> updateListenerKey = null;

        if (localStoreClass == InMemoryStore.class) {
            updateListenerKey = Key.get(UpdateListener.class, annotation);
            binder.bind(updateListenerKey).to(DynamicUpdateListener.class).in(Scopes.SINGLETON);
            reportBinder(binder).bindReportCollection(DynamicRenewals.class).withApplicationPrefix();
        }

        bindConfig(binder).bind(StoreConfig.class).annotatedWith(annotation).prefixedWith(name);
        httpClientBinder(binder).bindHttpClient(name, annotation);
        binder.bind(DistributedStore.class).annotatedWith(annotation).toProvider(new DistributedStoreProvider(name, localStoreKey, storeConfigKey, remoteStoreKey, updateListenerKey)).in(Scopes.SINGLETON);
        binder.bind(Replicator.class).annotatedWith(annotation).toProvider(new ReplicatorProvider(name, localStoreKey, httpClientKey, storeConfigKey)).in(Scopes.SINGLETON);
        binder.bind(HttpRemoteStore.class).annotatedWith(annotation).toProvider(new RemoteHttpStoreProvider(name, httpClientKey, storeConfigKey)).in(Scopes.SINGLETON);
        binder.bind(InMemoryStore.class).annotatedWith(annotation).to(localStoreClass).in(Scopes.SINGLETON);

        binder.bind(RemoteStore.class).annotatedWith(annotation).to(Key.get(HttpRemoteStore.class, annotation));

        reportBinder(binder).export(DistributedStore.class)
                .annotatedWith(annotation)
                .withApplicationPrefix()
                .withNamePrefix("DistributedStore." + name);
        newExporter(binder).export(HttpRemoteStore.class).annotatedWith(annotation).as(generatedNameOf(HttpRemoteStore.class, named(name)));
        newExporter(binder).export(Replicator.class).annotatedWith(annotation).as(generatedNameOf(Replicator.class, named(name)));

        newMapBinder(binder, String.class, InMemoryStore.class)
            .addBinding(name)
            .to(localStoreKey);

        newMapBinder(binder, String.class, StoreConfig.class)
                .addBinding(name)
                .to(storeConfigKey);
    }

    @ThreadSafe
    private static class ReplicatorProvider
        implements Provider<Replicator>
    {
        private final String name;
        private final Key<? extends InMemoryStore> localStoreKey;
        private final Key<? extends HttpClient> httpClientKey;
        private final Key<StoreConfig> storeConfigKey;

        @GuardedBy("this")
        private Injector injector;

        @GuardedBy("this")
        private NodeInfo nodeInfo;

        @GuardedBy("this")
        private ServiceSelector serviceSelector;

        @GuardedBy("this")
        private Replicator replicator;

        private ReplicatorProvider(String name, Key<? extends InMemoryStore> localStoreKey, Key<? extends HttpClient> httpClientKey, Key<StoreConfig> storeConfigKey)
        {
            this.name = name;
            this.localStoreKey = localStoreKey;
            this.httpClientKey = httpClientKey;
            this.storeConfigKey = storeConfigKey;
        }

        @Override
        public synchronized Replicator get()
        {
            if (replicator == null) {
                InMemoryStore localStore = injector.getInstance(localStoreKey);
                HttpClient httpClient = injector.getInstance(httpClientKey);
                StoreConfig storeConfig = injector.getInstance(storeConfigKey);
                InitializationTracker initializationTracker = injector.getInstance(InitializationTracker.class);

                ReportCollectionFactory reportCollectionFactory = injector.getInstance(ReportCollectionFactory.class);
                HttpServiceBalancerStats httpServiceBalancerStats = reportCollectionFactory.createReportCollection(
                        HttpServiceBalancerStats.class,
                        false,
                        "ServiceClient",
                        ImmutableMap.of("serviceType", "replicator-" + name)
                );

                replicator = new Replicator(name, nodeInfo, serviceSelector, httpClient, httpServiceBalancerStats, localStore, storeConfig, initializationTracker,
                        newSingleThreadScheduledExecutor(daemonThreadsNamed("replicator-" + name)));
                replicator.start();
            }

            return replicator;
        }

        @PreDestroy
        public synchronized void shutdown()
        {
            if (replicator != null) {
                replicator.shutdown();
            }
        }

        @Inject
        public synchronized void setInjector(Injector injector)
        {
            this.injector = injector;
        }

        @Inject
        public synchronized void setNodeInfo(NodeInfo nodeInfo)
        {
            this.nodeInfo = nodeInfo;
        }

        @Inject
        public synchronized void setServiceSelector(ServiceSelector serviceSelector)
        {
            this.serviceSelector = serviceSelector;
        }
    }

    @ThreadSafe
    private static class RemoteHttpStoreProvider
            implements Provider<HttpRemoteStore>
    {
        @GuardedBy("this")
        private HttpRemoteStore remoteStore;

        @GuardedBy("this")
        private Injector injector;

        @GuardedBy("this")
        private NodeInfo nodeInfo;

        @GuardedBy("this")
        private ServiceSelector serviceSelector;

        @GuardedBy("this")
        private ReportExporter reportExporter;

        private final String name;
        private final Key<? extends HttpClient> httpClientKey;
        private final Key<StoreConfig> storeConfigKey;


        @Inject
        private RemoteHttpStoreProvider(String name, Key<? extends HttpClient> httpClientKey, Key<StoreConfig> storeConfigKey)
        {
            this.name = name;
            this.httpClientKey = httpClientKey;
            this.storeConfigKey = storeConfigKey;
        }

        @Override
        public synchronized HttpRemoteStore get()
        {
            if (remoteStore == null) {
                HttpClient httpClient = injector.getInstance(httpClientKey);
                StoreConfig storeConfig = injector.getInstance(storeConfigKey);

                remoteStore = new HttpRemoteStore(name, nodeInfo, serviceSelector, storeConfig, httpClient, reportExporter,
                        newSingleThreadScheduledExecutor(daemonThreadsNamed("http-remote-store-" + name)));
                remoteStore.start();
            }

            return remoteStore;
        }

        @PreDestroy
        public synchronized void shutdown()
        {
            if (remoteStore != null) {
                remoteStore.shutdown();
            }
        }

        @Inject
        public synchronized void setInjector(Injector injector)
        {
            this.injector = injector;
        }

        @Inject
        public synchronized void setNodeInfo(NodeInfo nodeInfo)
        {
            this.nodeInfo = nodeInfo;
        }

        @Inject
        public synchronized void setServiceSelector(ServiceSelector serviceSelector)
        {
            this.serviceSelector = serviceSelector;
        }

        @Inject
        private synchronized void setReportExporter(ReportExporter reportExporter)
        {
            this.reportExporter = reportExporter;
        }
    }

    private static class DistributedStoreProvider
            implements Provider<DistributedStore>
    {
        private final String name;
        private final Key<? extends InMemoryStore> localStoreKey;
        private final Key<StoreConfig> storeConfigKey;
        private final Key<? extends RemoteStore> remoteStoreKey;
        private final Key<UpdateListener> updateListenerKey;

        private Injector injector;
        private Supplier<Instant> timeSupplier;
        private DistributedStore store;

        DistributedStoreProvider(String name,
                Key<? extends InMemoryStore> localStoreKey,
                Key<StoreConfig> storeConfigKey,
                Key<? extends RemoteStore> remoteStoreKey,
                Key<UpdateListener> updateListenerKey)
        {
            this.name = name;
            this.localStoreKey = localStoreKey;
            this.storeConfigKey = storeConfigKey;
            this.remoteStoreKey = remoteStoreKey;
            this.updateListenerKey = updateListenerKey;
        }

        @Override
        public synchronized DistributedStore get()
        {
            if (store == null) {
                InMemoryStore localStore = injector.getInstance(localStoreKey);
                StoreConfig storeConfig = injector.getInstance(storeConfigKey);
                RemoteStore remoteStore = injector.getInstance(remoteStoreKey);
                DiscoveryConfig discoveryConfig = injector.getInstance(DiscoveryConfig.class);

                if (updateListenerKey != null) {
                    UpdateListener updateListener = injector.getInstance(updateListenerKey);
                    localStore.setUpdateListener(updateListener);
                }

                store = new DistributedStore(name, localStore, remoteStore, storeConfig, discoveryConfig, timeSupplier);
                store.start();
            }

            return store;
        }

        @PreDestroy
        public synchronized void shutdown()
        {
            if (store != null) {
                store.shutdown();
            }
        }

        @Inject
        public synchronized void setInjector(Injector injector)
        {
            this.injector = injector;
        }

        @Inject
        public synchronized void setTimeSupplier(Supplier<Instant> timeSupplier)
        {
            this.timeSupplier = timeSupplier;
        }
    }
}

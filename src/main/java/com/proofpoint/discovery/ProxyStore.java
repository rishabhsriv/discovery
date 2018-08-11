/*
 * Copyright 2013 Proofpoint, Inc.
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
package com.proofpoint.discovery;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.proofpoint.discovery.client.DiscoveryException;
import com.proofpoint.discovery.client.DiscoveryLookupClient;
import com.proofpoint.discovery.client.HttpDiscoveryLookupClient;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceDescriptors;
import com.proofpoint.discovery.client.ServiceDescriptorsRepresentation;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.log.Logger;
import com.proofpoint.node.NodeInfo;
import com.proofpoint.units.Duration;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static com.google.common.base.Throwables.propagate;
import static com.proofpoint.concurrent.Threads.daemonThreadsNamed;
import static com.proofpoint.discovery.client.announce.DiscoveryAnnouncementClient.DEFAULT_DELAY;
import static com.proofpoint.json.JsonCodec.jsonCodec;

public class ProxyStore
{
    private final Set<String> proxyTypes;
    private final Map<String, Collection<Service>> map;

    private static final Logger log = Logger.get(ProxyStore.class);

    @Inject
    public ProxyStore(final DiscoveryConfig discoveryConfig, Injector injector)
    {
        this.proxyTypes = discoveryConfig.getProxyProxiedTypes();

        if (!proxyTypes.isEmpty()) {
            map = new ConcurrentHashMap<>();
            HttpClient httpClient = injector.getInstance(
                    Key.get(HttpClient.class, ForProxyStore.class));
            DiscoveryLookupClient lookupClient = new HttpDiscoveryLookupClient(
                    new NodeInfo(discoveryConfig.getProxyEnvironment()),
                    jsonCodec(ServiceDescriptorsRepresentation.class),
                    httpClient,
                    null);
            ScheduledThreadPoolExecutor poolExecutor = new ScheduledThreadPoolExecutor(5, daemonThreadsNamed("Proxy-Discovery-%s"));

            for (String type : proxyTypes) {
                new ServiceUpdater(type, lookupClient, poolExecutor).start();
            }
        }
        else {
            map = null;
        }
    }

    public Iterable<Service> filterAndGetAll(Iterable<Service> services)
    {
        if (proxyTypes.isEmpty()) {
            return services;
        }

        Builder<Service> builder = ImmutableList.builder();
        for (Service service : services) {
            if (!proxyTypes.contains(service.getType())) {
                builder.add(service);
            }
        }
        map.values().forEach(builder::addAll);
        return builder.build();
    }

    @Nullable
    public Stream<Service> get(String type)
    {
        if (!proxyTypes.contains(type)) {
            return null;
        }
        return map.get(type).stream();
    }

    @Nullable
    public Stream<Service> get(String type, final String pool)
    {
        if (!proxyTypes.contains(type)) {
            return null;
        }
        return map.get(type).stream()
                .filter(service -> pool.equals(service.getPool()));
    }

    private class ServiceUpdater
    {
        private final String type;
        private final DiscoveryLookupClient lookupClient;
        private final ScheduledThreadPoolExecutor poolExecutor;
        private final AtomicBoolean serverUp = new AtomicBoolean(true);

        public ServiceUpdater(String type, DiscoveryLookupClient lookupClient, ScheduledThreadPoolExecutor poolExecutor)
        {
            this.type = type;
            this.lookupClient = lookupClient;
            this.poolExecutor = poolExecutor;
        }

        public void start()
        {
            try {
                refresh().get(30, TimeUnit.SECONDS);
            }
            catch (TimeoutException ignored) {
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            catch (ExecutionException e) {
                log.error(e, "Exception upon initial refresh of proxied service %s", type);
            }
        }

        private ListenableFuture<ServiceDescriptors> refresh()
        {
            final ListenableFuture<ServiceDescriptors> future = lookupClient.getServices(type);

            future.addListener(() -> {
                Duration delay = DEFAULT_DELAY;
                try {
                    ServiceDescriptors descriptors = future.get();
                    delay = descriptors.getMaxAge();
                    Builder<Service> builder = ImmutableList.builder();
                    for (ServiceDescriptor descriptor : descriptors.getServiceDescriptors()) {
                        String nodeId = descriptor.getNodeId();
                        builder.add(new Service(
                                Id.valueOf(descriptor.getId()),
                                nodeId == null ? null : Id.valueOf(nodeId),
                                descriptor.getType(),
                                descriptor.getPool(),
                                descriptor.getLocation(),
                                descriptor.getProperties()));
                    }
                    map.put(type, builder.build());
                    if (serverUp.compareAndSet(false, true)) {
                        log.info("Proxied discovery server connect succeeded for refresh (%s)", type);
                    }
                }
                catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                catch (ExecutionException e) {
                    if (!(e.getCause() instanceof DiscoveryException)) {
                        throw propagate(e);
                    }
                    if (serverUp.compareAndSet(true, false)) {
                        log.error("Cannot connect to proxy discovery server for refresh (%s): %s", type, e.getCause().getMessage());
                    }
                    log.debug(e.getCause(), "Cannot connect to proxy discovery server for refresh (%s)", type);
                }
                finally {
                    if (!poolExecutor.isShutdown()) {
                        poolExecutor.schedule((Runnable) this::refresh, delay.toMillis(), TimeUnit.MILLISECONDS);
                    }
                }
            }, poolExecutor);

            return future;
        }
    }
}

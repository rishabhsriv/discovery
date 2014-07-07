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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Throwables.propagate;
import static com.proofpoint.discovery.client.announce.DiscoveryAnnouncementClient.DEFAULT_DELAY;
import static com.proofpoint.json.JsonCodec.jsonCodec;

public class ProxyStore
{
    private final Set<String> proxyTypes;
    private final Map<String, Set<Service>> map;

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
            ScheduledThreadPoolExecutor poolExecutor = new ScheduledThreadPoolExecutor(5, new ThreadFactoryBuilder()
                    .setNameFormat("Proxy-Discovery-%s")
                    .setDaemon(true)
                    .build());

            for (String type : proxyTypes) {
                new ServiceUpdater(type, lookupClient, poolExecutor).start();
            }
        }
        else {
            map = null;
        }
    }

    public Set<Service> filterAndGetAll(Set<Service> services)
    {
        if (proxyTypes.isEmpty()) {
            return services;
        }

        Builder<Service> builder = ImmutableSet.builder();
        for (Service service : services) {
            if (!proxyTypes.contains(service.getType())) {
                builder.add(service);
            }
        }
        map.values().forEach(builder::addAll);
        return builder.build();
    }

    @Nullable
    public Set<Service> get(String type)
    {
        if (!proxyTypes.contains(type)) {
            return null;
        }
        return map.get(type);
    }

    @Nullable
    public Set<Service> get(String type, final String pool)
    {
        if (!proxyTypes.contains(type)) {
            return null;
        }
        Builder<Service> builder = ImmutableSet.builder();
        for (Service service : map.get(type)) {
            if (pool.equals(service.getPool())) {
                builder.add(service);
            }
        }
        return builder.build();
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
                if (e.getCause() instanceof DiscoveryException) {
                    throw (DiscoveryException) e.getCause();
                }
                throw propagate(e);
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
                    Builder<Service> builder = ImmutableSet.builder();
                    for (ServiceDescriptor descriptor : descriptors.getServiceDescriptors()) {
                        builder.add(new Service(
                                Id.<Service>valueOf(descriptor.getId()),
                                Id.<Node>valueOf(descriptor.getNodeId()),
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
                        poolExecutor.schedule((Runnable) this::refresh, (long) delay.toMillis(), TimeUnit.MILLISECONDS);
                    }
                }
            }, poolExecutor);

            return future;
        }
    }
}

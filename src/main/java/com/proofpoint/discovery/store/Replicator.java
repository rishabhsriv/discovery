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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.proofpoint.discovery.InitializationTracker;
import com.proofpoint.discovery.InitializationTracker.CompletionNotifier;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.http.client.Request;
import com.proofpoint.http.client.Response;
import com.proofpoint.http.client.ResponseHandler;
import com.proofpoint.http.client.balancing.HttpServiceBalancerStats;
import com.proofpoint.http.client.balancing.HttpServiceBalancerStats.Status;
import com.proofpoint.log.Logger;
import com.proofpoint.node.NodeInfo;
import com.proofpoint.units.Duration;
import org.weakref.jmx.Managed;

import java.io.EOFException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Replicator
{
    private static final Logger log = Logger.get(Replicator.class);

    private final String name;
    private final NodeInfo node;
    private final ServiceSelector selector;
    private final HttpClient httpClient;
    private final HttpServiceBalancerStats httpServiceBalancerStats;
    private final InMemoryStore localStore;
    private final Duration replicationInterval;
    private final CompletionNotifier completionNotifier;
    private final ScheduledExecutorService executor;

    private ScheduledFuture<?> future;

    private final ObjectMapper mapper = new ObjectMapper(new SmileFactory()).disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    private final AtomicLong lastReplicationTimestamp = new AtomicLong();

    public Replicator(String name,
            NodeInfo node,
            ServiceSelector selector,
            HttpClient httpClient,
            HttpServiceBalancerStats httpServiceBalancerStats,
            InMemoryStore localStore,
            StoreConfig config,
            InitializationTracker initializationTracker,
            ScheduledExecutorService executor)
    {
        this.name = name;
        this.node = node;
        this.selector = selector;
        this.httpClient = httpClient;
        this.httpServiceBalancerStats = httpServiceBalancerStats;
        this.localStore = localStore;
        this.replicationInterval = config.getReplicationInterval();
        completionNotifier = initializationTracker.createTask();
        this.executor = executor;
    }

    public synchronized void start()
    {
        if (future == null) {
            future = executor.scheduleAtFixedRate(() -> {
                try {
                    synchronize();
                }
                catch (Throwable t) {
                    log.warn(t, "Error replicating state");
                }
            }, 0, replicationInterval.toMillis(), TimeUnit.MILLISECONDS);
        }

        // TODO: need fail-safe recurrent scheduler with variable delay
    }

    public synchronized void shutdown()
    {
        if (future != null) {
            future.cancel(true);
            executor.shutdownNow();
        }
    }

    @Managed
    public long getLastReplicationTimestamp()
    {
        return lastReplicationTimestamp.get();
    }

    private void synchronize()
    {
        for (ServiceDescriptor descriptor : selector.selectAllServices()) {
            if (node.getNodeId().equals(descriptor.getNodeId())) {
                // don't write to ourselves
                continue;
            }

            final String uri = descriptor.getProperties().get("http");
            if (uri == null) {
                log.error("service descriptor for node %s is missing http uri", descriptor.getNodeId());
                continue;
            }

            // TODO: build URI from resource class
            Request request = Request.Builder.prepareGet()
                    .setUri(URI.create(uri + "/v1/store/" + name))
                    .addHeader("Accept", "application/x-jackson-smile")
                    .build();

            try {
                final long startTime = System.nanoTime();
                httpClient.execute(request, new ResponseHandler<Void, Exception>()
                {
                    @Override
                    public Void handleException(Request request, Exception exception)
                            throws Exception
                    {
                        URI uri1 = URI.create(uri);
                        httpServiceBalancerStats.requestTime(uri1, Status.FAILURE).add(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
                        httpServiceBalancerStats.failure(uri1, exception.getClass().getSimpleName()).add(1);
                        throw exception;
                    }

                    @Override
                    public Void handle(Request request, Response response)
                            throws Exception
                    {
                        // TODO: read server date (to use to calibrate entry dates)

                        URI uri1 = URI.create(uri);
                        if (response.getStatusCode() == 200) {
                            httpServiceBalancerStats.requestTime(uri1, Status.SUCCESS).add(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
                            try {
                                List<Entry> entries = mapper.readValue(response.getInputStream(), new TypeReference<List<Entry>>() {});
                                entries.forEach(localStore::put);
                            }
                            catch (EOFException | NullPointerException ignored) {
                            }
                        }
                        else {
                            httpServiceBalancerStats.requestTime(uri1, Status.FAILURE).add(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
                            httpServiceBalancerStats.failure(uri1, response.getStatusCode() + " status code").add(1);
                        }

                        return null;
                    }
                });
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            catch (Exception ignored) {
            }
        }

        completionNotifier.complete();
        lastReplicationTimestamp.set(System.currentTimeMillis());
    }
}

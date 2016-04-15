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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.http.client.Request;
import com.proofpoint.http.client.Response;
import com.proofpoint.http.client.ResponseHandler;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.log.Logger;
import com.proofpoint.node.NodeInfo;
import com.proofpoint.reporting.ReportExporter;
import com.proofpoint.units.Duration;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.net.URI;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.and;
import static com.google.common.base.Predicates.compose;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static com.proofpoint.concurrent.Threads.daemonThreadsNamed;
import static com.proofpoint.http.client.SmileBodyGenerator.smileBodyGenerator;
import static com.proofpoint.json.JsonCodec.jsonCodec;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class HttpRemoteStore
        implements RemoteStore
{
    private static final Logger log = Logger.get(HttpRemoteStore.class);
    private static final Pattern HTTP_PATTERN = Pattern.compile("^http(?:s)?://");

    private final int maxBatchSize;
    private final int queueSize;
    private final Duration updateInterval;

    private final ConcurrentMap<String, BatchProcessor<Entry>> processors = new ConcurrentHashMap<>();
    private final String name;
    private final ServiceSelector selector;
    private final HttpClient httpClient;

    private Future<?> future;
    private ScheduledExecutorService executor;

    private final AtomicLong lastRemoteServerRefreshTimestamp = new AtomicLong();
    private final ReportExporter reportExporter;
    private final Predicate<ServiceDescriptor> ourNodeIdPredicate;


    @Inject
    public HttpRemoteStore(String name,
            final NodeInfo node,
            ServiceSelector selector,
            StoreConfig config,
            HttpClient httpClient,
            ReportExporter reportExporter)
    {
        checkNotNull(name, "name is null");
        checkNotNull(node, "node is null");
        checkNotNull(selector, "selector is null");
        checkNotNull(httpClient, "httpClient is null");
        checkNotNull(config, "config is null");
        checkNotNull(reportExporter, "reportExporter is null");

        this.name = name;
        this.selector = selector;
        this.httpClient = httpClient;
        this.reportExporter = reportExporter;

        maxBatchSize = config.getMaxBatchSize();
        queueSize = config.getQueueSize();
        updateInterval = config.getRemoteUpdateInterval();
        ourNodeIdPredicate = input -> node.getNodeId().equals(input.getNodeId());
    }

    @PostConstruct
    public synchronized void start()
    {
        if (future == null) {
            // note: this *must* be single threaded for the shutdown logic to work correctly
            executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("http-remote-store-" + name));

            future = executor.scheduleWithFixedDelay(() -> {
                try {
                    updateProcessors(selector.selectAllServices());
                }
                catch (Throwable e) {
                    log.warn(e, "Error refreshing batch processors");
                }
            }, 0, updateInterval.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @PreDestroy
    public synchronized void shutdown()
    {
        if (future != null) {
            future.cancel(true);

            try {
                // schedule a task to shut down all processors and wait for it to complete. We rely on the executor
                // having a *single* thread to guarantee the execution happens after any currently running task
                // (in case the cancel call above didn't do its magic and the scheduled task is still running)
                executor.submit(() -> updateProcessors(ImmutableList.of())).get();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            catch (ExecutionException e) {
                throw propagate(e);
            }

            executor.shutdownNow();

            future = null;
        }
    }

    private void updateProcessors(List<ServiceDescriptor> descriptors)
    {
        Set<String> hostPorts = ImmutableSet.copyOf(transform(descriptors, getHostPortFunction()));

        // remove old ones
        Iterator<Map.Entry<String, BatchProcessor<Entry>>> iterator = processors.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, BatchProcessor<Entry>> entry = iterator.next();

            if (!hostPorts.contains(entry.getKey())) {
                iterator.remove();
                entry.getValue().stop();
                reportExporter.unexportObject(entry.getValue());
            }
        }


        Predicate<ServiceDescriptor> predicate = and(not(ourNodeIdPredicate), compose(not(in(processors.keySet())), getHostPortFunction()));
        Iterable<ServiceDescriptor> newDescriptors = filter(descriptors, predicate);

        for (ServiceDescriptor descriptor : newDescriptors) {
            String hostPort = getHostPort(descriptor);
            BatchProcessor<Entry> processor = new BatchProcessor<>(hostPort,
                    new MyBatchHandler(name, descriptor, httpClient),
                    maxBatchSize,
                    queueSize);

            processor.start();
            processors.put(hostPort, processor);
            reportExporter.export(processor, true, "BatchProcessor." + name, ImmutableMap.of("target", hostPort));
        }

        lastRemoteServerRefreshTimestamp.set(System.currentTimeMillis());
    }

    @Managed
    public long getLastRemoteServerRefreshTimestamp()
    {
        return lastRemoteServerRefreshTimestamp.get();
    }

    private static Function<ServiceDescriptor, String> getHostPortFunction()
    {
        return HttpRemoteStore::getHostPort;
    }

    private static String getHostPort(ServiceDescriptor descriptor)
    {
        return HTTP_PATTERN.matcher(descriptor.getProperties().get("http")).replaceFirst("");
    }

    @Override
    public void put(Entry entry)
    {
        for (BatchProcessor<Entry> processor : processors.values()) {
            processor.put(entry);
        }
    }

    private static class MyBatchHandler
            implements BatchProcessor.BatchHandler<Entry>
    {
        private static final JsonCodec<Collection<Entry>> ENTRIES_CODEC = jsonCodec(new TypeToken<Collection<Entry>>()
        {
        });

        private final URI uri;
        private final HttpClient httpClient;

        public MyBatchHandler(String name, ServiceDescriptor descriptor, HttpClient httpClient)
        {
            this.httpClient = httpClient;

            // TODO: build URI from resource class
            uri = URI.create(descriptor.getProperties().get("http") + "/v1/store/" + name);
        }

        @Override
        public void processBatch(final Collection<Entry> entries)
                throws Exception
        {
            final Request request = Request.Builder.preparePost()
                    .setUri(uri)
                    .setHeader("Content-Type", "application/x-jackson-smile")
                    .setBodySource(smileBodyGenerator(ENTRIES_CODEC, entries))
                    .build();

            try {
                httpClient.execute(request, new ResponseHandler<Void, Exception>()
                {
                    @Override
                    public Void handleException(Request request, Exception exception)
                            throws Exception
                    {
                        throw exception;
                    }

                    @Override
                    public Void handle(Request request, Response response)
                            throws Exception
                    {
                        if (response.getStatusCode() >= 300) {
                            throw new Exception("Remote server returned " + response.getStatusCode() + " status code");
                        }
                        return null;
                    }
                });
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}

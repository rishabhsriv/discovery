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

import com.google.common.base.Preconditions;
import com.proofpoint.log.Logger;
import com.proofpoint.reporting.Gauge;
import com.proofpoint.stats.CounterStat;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.proofpoint.concurrent.Threads.threadsNamed;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class BatchProcessor<T>
{
    private static final Logger log = Logger.get(BatchProcessor.class);

    private final BatchHandler<T> handler;
    private final int maxBatchSize;
    private final BlockingQueue<T> queue;
    private final String name;

    private ExecutorService executor;
    private volatile Future<?> future;

    private final CounterStat processedEntries = new CounterStat();
    private final CounterStat droppedEntries = new CounterStat();
    private final CounterStat errors = new CounterStat();

    public BatchProcessor(String name, BatchHandler<T> handler, int maxBatchSize, int queueSize)
    {
        Preconditions.checkNotNull(name, "name is null");
        Preconditions.checkNotNull(handler, "handler is null");
        Preconditions.checkArgument(queueSize > 0, "queue size needs to be a positive integer");
        Preconditions.checkArgument(maxBatchSize > 0, "max batch size needs to be a positive integer");

        this.name = name;
        this.handler = handler;
        this.maxBatchSize = maxBatchSize;
        this.queue = new ArrayBlockingQueue<>(queueSize);
    }

    @PostConstruct
    public synchronized void start()
    {
        if (future == null) {
            executor = newSingleThreadExecutor(threadsNamed("batch-processor-" + name));

            future = executor.submit(() -> {
                while (!Thread.interrupted()) {
                    final List<T> entries = new ArrayList<>(maxBatchSize);

                    try {
                        T first = queue.take();
                        entries.add(first);
                        queue.drainTo(entries, maxBatchSize - 1);

                        handler.processBatch(Collections.unmodifiableList(entries));

                        processedEntries.add(entries.size());
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    catch (Throwable t) {
                        errors.add(1);
                        log.warn(t, "Error handling batch");
                    }

                    // TODO: expose timestamp of last execution via jmx
                }
            });
            log.info("Adding discovery peer %s", name);
        }
    }

    @Nested
    public CounterStat getProcessedEntries()
    {
        return processedEntries;
    }

    @Nested
    public CounterStat getDroppedEntries()
    {
        return droppedEntries;
    }

    @Nested
    public CounterStat getErrors()
    {
        return errors;
    }

    @Gauge
    public long getQueueSize()
    {
        return queue.size();
    }

    @PreDestroy
    public synchronized void stop()
    {
        if (future != null) {
            future.cancel(true);
            executor.shutdownNow();
            log.info("Removing discovery peer %s", name);

            future = null;
        }
    }

    public void put(T entry)
    {
        Preconditions.checkState(!future.isCancelled(), "Processor is not running");
        Preconditions.checkNotNull(entry, "entry is null");

        while (!queue.offer(entry)) {
            // throw away oldest and try again
            if (queue.poll() != null) {
                droppedEntries.add(1);
            }
        }
    }

    public interface BatchHandler<T>
    {
        void processBatch(Collection<T> entries)
                throws Exception;
    }
}

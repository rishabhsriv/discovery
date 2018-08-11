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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.proofpoint.reporting.Gauge;
import com.proofpoint.units.Duration;
import org.joda.time.DateTime;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.Predicates.and;
import static com.google.common.base.Predicates.not;
import static com.proofpoint.concurrent.Threads.daemonThreadsNamed;
import static com.proofpoint.discovery.store.Entry.entry;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

/**
 * A simple, eventually consistent, fully replicated, distributed key-value store.
 */
public class DistributedStore
{
    private final String name;
    private final InMemoryStore localStore;
    private final RemoteStore remoteStore;
    private final Supplier<DateTime> timeSupplier;
    private final Duration tombstoneMaxAge;
    private final Duration garbageCollectionInterval;

    private final ScheduledExecutorService garbageCollector;
    private final AtomicLong lastGcTimestamp = new AtomicLong();

    @Inject
    public DistributedStore(
            String name,
            InMemoryStore localStore,
            RemoteStore remoteStore,
            StoreConfig config,
            Supplier<DateTime> timeSupplier)
    {
        this.name = requireNonNull(name, "name is null");
        this.localStore = requireNonNull(localStore, "localStore is null");
        this.remoteStore = requireNonNull(remoteStore, "remoteStore is null");
        this.timeSupplier = requireNonNull(timeSupplier, "timeSupplier is null");

        requireNonNull(config, "config is null");
        tombstoneMaxAge = config.getTombstoneMaxAge();
        garbageCollectionInterval = config.getGarbageCollectionInterval();
        
        garbageCollector = newSingleThreadScheduledExecutor(daemonThreadsNamed("distributed-store-gc-" + name));
    }

    @PostConstruct
    public void start()
    {
        garbageCollector.scheduleAtFixedRate(this::removeExpiredEntries, 0, garbageCollectionInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Managed
    public String getName()
    {
        return name;
    }

    @Managed
    public long getLastGcTimestamp()
    {
        return lastGcTimestamp.get();
    }

    @Managed
    public void removeExpiredEntries()
    {
        for (Entry entry : localStore.getAll()) {
            if (isExpired(entry)) {
                localStore.delete(entry.getKey(), entry.getTimestamp());
            }
        }

        lastGcTimestamp.set(System.currentTimeMillis());
    }

    @Gauge
    public long getActiveEntryCount()
    {
        long count = 0;
        for (Entry entry : localStore.getAll()) {
            if (!isExpired(entry) && entry.getValue() != null) {
                ++count;
            }
        }
        return count;
    }

    private boolean isExpired(Entry entry)
    {
        long ageInMs = timeSupplier.get().getMillis() - entry.getTimestamp();

        return entry.getValue() == null && ageInMs > tombstoneMaxAge.toMillis() ||  // TODO: this is repeated in StoreResource
                entry.getMaxAgeInMs() != null && ageInMs > entry.getMaxAgeInMs();
    }

    @PreDestroy
    public void shutdown()
    {
        garbageCollector.shutdownNow();
    }

    public void put(byte[] key, byte[] value)
    {
        requireNonNull(key, "key is null");
        requireNonNull(value, "value is null");

        long now = timeSupplier.get().getMillis();

        Entry entry = entry(key, value, now, null);

        localStore.put(entry);
        remoteStore.put(entry);
    }
    
    public void put(byte[] key, byte[] value, Duration maxAge)
    {
        requireNonNull(key, "key is null");
        requireNonNull(value, "value is null");
        requireNonNull(maxAge, "maxAge is null");

        long now = timeSupplier.get().getMillis();

        Entry entry = entry(key, value, now, maxAge.toMillis());

        localStore.put(entry);
        remoteStore.put(entry);
    }

    public byte[] get(byte[] key)
    {
        requireNonNull(key, "key is null");

        Entry entry = localStore.get(key);
        
        byte[] result = null;
        if (entry != null && entry.getValue() != null && !isExpired(entry)) {
            result = Arrays.copyOf(entry.getValue(), entry.getValue().length);
        }

        return result;
    }

    public void delete(byte[] key)
    {
        requireNonNull(key, "key is null");

        long now = timeSupplier.get().getMillis();

        Entry entry = entry(key, null, now, null);

        localStore.put(entry);
        remoteStore.put(entry);
    }

    public Iterable<Entry> getAll()
    {
        return Iterables.filter(localStore.getAll(), and(not(expired()), not(tombstone())));
    }

    private Predicate<Entry> expired()
    {
        return this::isExpired;
    }

    private static Predicate<Entry> tombstone()
    {
        return entry -> entry.getValue() == null;
    }
}

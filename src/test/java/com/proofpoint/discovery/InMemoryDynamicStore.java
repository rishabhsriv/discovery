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
package com.proofpoint.discovery;

import com.google.common.collect.ImmutableSet;
import com.proofpoint.discovery.store.Entry;
import com.proofpoint.units.Duration;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.proofpoint.discovery.DynamicServiceAnnouncement.toServiceWith;
import static com.proofpoint.discovery.Service.matchesPool;
import static com.proofpoint.discovery.Service.matchesType;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class InMemoryDynamicStore
        implements DynamicStore
{
    private final Map<Id<Node>, LocalEntry> descriptors = new HashMap<>();
    private final Duration maxAge;
    private final Supplier<Instant> currentTime;

    @Inject
    public InMemoryDynamicStore(DiscoveryConfig config, Supplier<Instant> timeSource)
    {
        this.currentTime = timeSource;
        this.maxAge = config.getMaxAge();
    }

    @Override
    public synchronized void put(Id<Node> nodeId, DynamicAnnouncement announcement)
    {
        requireNonNull(nodeId, "nodeId is null");
        requireNonNull(announcement, "announcement is null");

        Set<Service> services = announcement.getServiceAnnouncements().stream()
                .map(toServiceWith(nodeId, announcement.getLocation(), announcement.getPool()))
                .collect(toImmutableSet());

        Instant expiration = currentTime.get().plusMillis((int) maxAge.toMillis());
        descriptors.put(nodeId, new LocalEntry(expiration, services, announcement.getAnnouncer()));
    }

    @Override
    public synchronized void delete(Id<Node> nodeId)
    {
        requireNonNull(nodeId, "nodeId is null");

        descriptors.remove(nodeId);
    }

    @Override
    public synchronized Stream<Service> getAll()
    {
        removeExpired();

        return descriptors.values().stream()
                .flatMap(entry -> entry.getServices().stream());
    }

    @Override
    public synchronized Stream<Service> get(String type)
    {
        requireNonNull(type, "type is null");

        return getAll()
                .filter(matchesType(type));
    }

    @Override
    public synchronized Stream<Service> get(String type, String pool)
    {
        requireNonNull(type, "type is null");
        requireNonNull(pool, "pool is null");

        return getAll()
                .filter(matchesType(type).and(matchesPool(pool)));
    }

    @Override
    public Entry get(Id<Node> nodeId)
    {
        removeExpired();

        LocalEntry localEntry = descriptors.get(nodeId);
        if (localEntry == null) {
            return null;
        }
        long timestamp = localEntry.getExpiration().minusMillis((int) maxAge.toMillis()).toEpochMilli();
        return Entry.entry(nodeId.getBytes(), new ArrayList<>(localEntry.getServices()), timestamp, maxAge.toMillis(), localEntry.getAnnouncer());
    }

    private synchronized void removeExpired()
    {
        Iterator<LocalEntry> iterator = descriptors.values().iterator();

        Instant now = currentTime.get();
        while (iterator.hasNext()) {
            LocalEntry entry = iterator.next();

            if (now.isAfter(entry.getExpiration())) {
                iterator.remove();
            }
        }
    }

    private static class LocalEntry
    {
        private final Set<Service> services;
        private final Instant expiration;
        private final String announcer;

        LocalEntry(Instant expiration, Set<Service> services, String announcer)
        {
            this.expiration = expiration;
            this.services = ImmutableSet.copyOf(services);
            this.announcer = announcer;
        }

        Instant getExpiration()
        {
            return expiration;
        }

        Set<Service> getServices()
        {
            return services;
        }

        String getAnnouncer()
        {
            return announcer;
        }
    }
}

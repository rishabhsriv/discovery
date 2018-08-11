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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.proofpoint.units.Duration;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.collect.Collections2.transform;
import static com.proofpoint.discovery.DynamicServiceAnnouncement.toServiceWith;
import static com.proofpoint.discovery.Service.matchesPool;
import static com.proofpoint.discovery.Service.matchesType;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class InMemoryDynamicStore
        implements DynamicStore
{
    private final Map<Id<Node>, Entry> descriptors = new HashMap<>();
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

        Set<Service> services = ImmutableSet.copyOf(transform(announcement.getServiceAnnouncements(), toServiceWith(nodeId, announcement.getLocation(), announcement.getPool())));

        Instant expiration = currentTime.get().plusMillis((int) maxAge.toMillis());
        descriptors.put(nodeId, new Entry(expiration, services));
    }

    @Override
    public synchronized void delete(Id<Node> nodeId)
    {
        requireNonNull(nodeId, "nodeId is null");

        descriptors.remove(nodeId);
    }

    @Override
    public synchronized Collection<Service> getAll()
    {
        removeExpired();

        ImmutableList.Builder<Service> builder = ImmutableList.builder();
        for (Entry entry : descriptors.values()) {
            builder.addAll(entry.getServices());
        }
        return builder.build();
    }

    @Override
    public synchronized Collection<Service> get(String type)
    {
        requireNonNull(type, "type is null");

        return getAll().stream()
                .filter(matchesType(type))
                .collect(Collectors.toList());
    }

    @Override
    public synchronized Collection<Service> get(String type, String pool)
    {
        requireNonNull(type, "type is null");
        requireNonNull(pool, "pool is null");

        return getAll().stream()
                .filter(matchesType(type).and(matchesPool(pool)))
                .collect(Collectors.toList());
    }

    private synchronized void removeExpired()
    {
        Iterator<Entry> iterator = descriptors.values().iterator();

        Instant now = currentTime.get();
        while (iterator.hasNext()) {
            Entry entry = iterator.next();

            if (now.isAfter(entry.getExpiration())) {
                iterator.remove();
            }
        }
    }

    private static class Entry
    {
        private final Set<Service> services;
        private final Instant expiration;

        Entry(Instant expiration, Set<Service> services)
        {
            this.expiration = expiration;
            this.services = ImmutableSet.copyOf(services);
        }

        public Instant getExpiration()
        {
            return expiration;
        }

        public Set<Service> getServices()
        {
            return services;
        }
    }
}

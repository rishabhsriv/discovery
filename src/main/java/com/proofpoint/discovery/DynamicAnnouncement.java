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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@Immutable
public class DynamicAnnouncement
{
    private final String environment;
    private final String location;
    private final String pool;
    private final Set<DynamicServiceAnnouncement> services;
    private final String announcerAddr;

    @JsonCreator
    public DynamicAnnouncement(
            @JsonProperty("environment") String environment,
            @JsonProperty("pool") String pool,
            @JsonProperty("location") String location,
            @JsonProperty("services") Set<DynamicServiceAnnouncement> services)
    {
        this(environment, pool, location, services, null);
    }

    private DynamicAnnouncement(
            String environment,
            String pool,
            String location,
            Set<DynamicServiceAnnouncement> services,
            String announcerAddr)
    {
        this.environment = environment;
        this.location = location;
        this.pool = pool;

        if (services != null) {
            this.services = ImmutableSet.copyOf(services);
        }
        else {
            this.services = null;
        }
        this.announcerAddr = announcerAddr;
    }

    @NotNull
    public String getEnvironment()
    {
        return environment;
    }

    public String getLocation()
    {
        return location;
    }

    @NotNull
    public String getPool()
    {
        return pool;
    }

    @NotNull
    @Valid
    public Set<DynamicServiceAnnouncement> getServiceAnnouncements()
    {
        return services;
    }

    @Nullable
    public String getAnnouncerAddr()
    {
        return announcerAddr;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DynamicAnnouncement that = (DynamicAnnouncement) o;
        return Objects.equals(environment, that.environment) &&
                Objects.equals(location, that.location) &&
                Objects.equals(pool, that.pool) &&
                Objects.equals(services, that.services) &&
                Objects.equals(announcerAddr, that.announcerAddr);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(environment, location, pool, services, announcerAddr);
    }

    @Override
    public String toString()
    {
        return "DynamicAnnouncement{" +
                "environment='" + environment + '\'' +
                ", location='" + location + '\'' +
                ", pool='" + pool + '\'' +
                ", services=" + services + '\'' +
                ", announcer=" + Optional.ofNullable(announcerAddr).orElse("null") +
                '}';
    }

    public static Builder copyOf(DynamicAnnouncement announcement)
    {
        return new Builder().copyOf(announcement);
    }

    public static class Builder
    {
        private String environment;
        private String location;
        private String pool;
        private Set<DynamicServiceAnnouncement> services;
        private String announcer;

        public Builder copyOf(DynamicAnnouncement announcement)
        {
            environment = announcement.getEnvironment();
            location = announcement.getLocation();
            services = announcement.getServiceAnnouncements();
            pool = announcement.getPool();
            announcer = announcement.getAnnouncerAddr();

            return this;
        }

        public Builder setLocation(String location)
        {
            this.location = location;
            return this;
        }

        public Builder setAnnouncer(String announcer)
        {
            this.announcer = announcer;
            return this;
        }

        public DynamicAnnouncement build()
        {
            return new DynamicAnnouncement(environment, pool, location, services, announcer);
        }
    }
}

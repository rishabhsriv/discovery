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
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.Immutable;
import java.util.Map;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

@Immutable
public class Service
{
    private final Id<Service> id;
    private final Id<Node> nodeId;
    private final String type;
    private final String pool;
    private final String location;
    private final Map<String, String> properties;

    @JsonCreator
    public Service(
            @JsonProperty("id") Id<Service> id,
            @JsonProperty("nodeId") Id<Node> nodeId,
            @JsonProperty("type") String type,
            @JsonProperty("pool") String pool,
            @JsonProperty("location") String location,
            @JsonProperty("properties") Map<String, String> properties)
    {
        requireNonNull(id, "id is null");
        requireNonNull(type, "type is null");
        requireNonNull(pool, "pool is null");
        requireNonNull(location, "location is null");
        requireNonNull(properties, "properties is null");

        this.id = id;
        this.nodeId = nodeId;
        this.type = type;
        this.pool = pool;
        this.location = location;
        this.properties = ImmutableMap.copyOf(properties);
    }

    @JsonProperty
    public Id<Service> getId()
    {
        return id;
    }

    @JsonProperty
    public Id<Node> getNodeId()
    {
        return nodeId;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @JsonProperty
    public String getPool()
    {
        return pool;
    }

    @JsonProperty
    public String getLocation()
    {
        return location;
    }

    @JsonProperty
    public Map<String, String> getProperties()
    {
        return properties;
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

        Service that = (Service) o;

        if (!id.equals(that.id)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return id.hashCode();
    }


    public static Predicate<Service> matchesType(final String type)
    {
        return descriptor -> descriptor.getType().equals(type);
    }

    public static Predicate<Service> matchesPool(final String pool)
    {
        return descriptor -> descriptor.getPool().equals(pool);
    }

    @Override
    public String toString()
    {
        return "Service{" +
                "id=" + id +
                ", nodeId=" + nodeId +
                ", type='" + type + '\'' +
                ", pool='" + pool + '\'' +
                ", location='" + location + '\'' +
                ", properties=" + properties +
                '}';
    }
}

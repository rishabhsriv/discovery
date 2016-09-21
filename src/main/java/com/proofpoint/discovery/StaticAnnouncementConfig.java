/*
 * Copyright 2016 Proofpoint, Inc.
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

import com.proofpoint.configuration.Config;

import javax.validation.constraints.AssertFalse;
import javax.validation.constraints.NotNull;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

public class StaticAnnouncementConfig
{
    private String type;
    private String pool = "general";
    private Map<String, String> properties;

    @NotNull
    public String getType()
    {
        return type;
    }

    @Config("type")
    public StaticAnnouncementConfig setType(String type)
    {
        this.type = type;
        return this;
    }

    public String getPool()
    {
        return pool;
    }

    @Config("pool")
    public StaticAnnouncementConfig setPool(String pool)
    {
        this.pool = pool;
        return this;
    }

    @NotNull
    public Map<String, String> getProperties()
    {
        return properties;
    }

    @Config("property")
    public StaticAnnouncementConfig setProperties(Map<String, String> properties)
    {
        this.properties = properties;
        return this;
    }

    @AssertFalse
    public boolean isUriInvalid()
    {
        if (properties == null) {
            return false;
        }

        String http = properties.get("http");
        if (http != null && !checkUri(http, "http")) {
            return true;
        }

        String https = properties.get("https");
        if (https != null && !checkUri(https, "https")) {
            return true;
        }

        return false;
    }

    private static boolean checkUri(String uriString, String scheme)
    {
        URI uri;

        try {
            uri = URI.create(uriString);
        }
        catch (Exception e) {
            return false;
        }

        if (!scheme.equals(uri.getScheme())) {
            return false;
        }

        if (uri.getHost() == null) {
            return false;
        }

        return true;
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
        StaticAnnouncementConfig that = (StaticAnnouncementConfig) o;
        return Objects.equals(type, that.type) &&
                Objects.equals(pool, that.pool) &&
                Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, pool, properties);
    }
}

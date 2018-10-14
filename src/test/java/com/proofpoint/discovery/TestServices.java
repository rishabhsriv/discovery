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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import com.proofpoint.json.JsonCodec;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static com.proofpoint.discovery.Services.services;
import static com.proofpoint.json.JsonCodec.jsonCodec;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestServices
{
    @Test
    public void testCreatesDefensiveCopyOfServices()
    {
        Set<Service> set = new HashSet<>();
        set.add(new Service(Id.random(), Id.random(), "blue", "pool", "/location", ImmutableMap.of("key", "value")));

        Services services = services("testing", set);
        assertThat(services.getServices()).containsExactlyElementsOf(set);

        Service newService = new Service(Id.random(), Id.random(), "red", "pool", "/location", ImmutableMap.of("key", "value"));
        set.add(newService);
        assertThat(services.getServices()).doesNotContain(newService);
    }

    @Test
    public void testServicesSetIsImmutable()
    {
        Service blue = new Service(Id.random(), Id.random(), "blue", "pool", "/location", ImmutableMap.of("key", "value"));

        Services services = services("testing", Sets.newHashSet(blue));
        try {
            services.getServices().add(new Service(Id.random(), Id.random(), "red", "pool", "/location", ImmutableMap.of("key", "value")));

            // a copy of the internal map is acceptable
            assertThat(services.getServices()).containsExactly(blue);
        }
        catch (UnsupportedOperationException e) {
            // an exception is ok, too
        }
    }

    @Test
    public void testToJson()
            throws IOException
    {
        Service blue = new Service(Id.valueOf("c0c5be5f-b298-4cfa-922a-3e5954208444"), Id.valueOf("3ff52f57-04e0-46c3-b606-7497b09dd5c7"), "blue", "poolA", "/locationA", ImmutableMap.of("key", "valueA"));
        Service red = new Service(Id.valueOf("e3780aba-98fe-4de1-b682-5cd7a3264367"), Id.valueOf("989c4a90-ef68-4a05-8ad9-73a02aea406c"), "red", "poolB", "/locationB", ImmutableMap.of("key", "valueB"));
        Services services = services("testing", ImmutableSet.of(blue, red));

        String json = jsonCodec(Services.class).toJson(services);

        JsonCodec<Object> codec = jsonCodec(Object.class);
        Object parsed = codec.fromJson(json);
        Object expected = codec.fromJson(Resources.toString(Resources.getResource("services.json"), UTF_8));

        assertThat(parsed).isEqualTo(expected);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = ".*environment.*")
    public void testValidatesEnvironmentNotNull()
    {
        services(null, Collections.emptySet());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "services.*")
    public void testValidatesServicesNotNull()
    {
        services("testing", (Iterable<Service>) null);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "services.*")
    public void testValidatesServicesStreamNotNull()
    {
        services("testing", (Stream<Service>) null);
    }
}

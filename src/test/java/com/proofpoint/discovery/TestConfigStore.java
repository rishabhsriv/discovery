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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.stream.Collectors;

import static com.proofpoint.testing.Assertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertEquals;

public class TestConfigStore
{
    private static final Service EXPECTED_SERVICE_1 = staticService("c4ca4238-a0b9-3382-8dcc-509a6f75849b", "type1", "general", "http://a1.invalid");
    private static final Service EXPECTED_SERVICE_2 = staticService("c81e728d-9d4c-3f63-af06-7f89cc14862c", "type1", "general", "http://a2.invalid");
    private static final Service EXPECTED_SERVICE_3 = staticService("eccbc87e-4b5c-32fe-a830-8fd9f2a7baf3", "type1", "alternate", "http://a3.invalid");
    private static final Service EXPECTED_SERVICE_4 = staticService("a87ff679-a2f3-371d-9181-a67b7542122c", "type2", "alternate", "http://a4.invalid");

    private final ConfigStore store = new ConfigStore(new ConfigStoreConfig().setAnnouncements(ImmutableMap.of(
            "1", staticAnnouncementConfig("type1", "general", "http://a1.invalid"),
            "2", staticAnnouncementConfig("type1", "general", "http://a2.invalid"),
            "3", staticAnnouncementConfig("type1", "alternate", "http://a3.invalid"),
            "4", staticAnnouncementConfig("type2", "alternate", "http://a4.invalid")
    )));

    @Test
    public void testGetAll()
    {
        assertEqualsIgnoreOrder(store.getAll(), ImmutableSet.of(EXPECTED_SERVICE_1, EXPECTED_SERVICE_2, EXPECTED_SERVICE_3, EXPECTED_SERVICE_4));
        assertEquals(new ConfigStore(new ConfigStoreConfig()).getAll(), ImmutableSet.of());
    }

    @Test
    public void testGetType()
    {
        assertEquals(store.get("type1"), ImmutableSet.of(EXPECTED_SERVICE_1, EXPECTED_SERVICE_2, EXPECTED_SERVICE_3));
        assertEquals(store.get("type2"), ImmutableSet.of(EXPECTED_SERVICE_4));
        assertEquals(store.get("unknown"), ImmutableSet.of());
    }

    @Test
    public void testGetTypeAndPool()
    {
        assertEquals(store.get("type1", "general").collect(Collectors.toList()), ImmutableSet.of(EXPECTED_SERVICE_1, EXPECTED_SERVICE_2));
        assertEquals(store.get("type1", "alternate").collect(Collectors.toList()), ImmutableSet.of(EXPECTED_SERVICE_3));
        assertEquals(store.get("type1", "unknown").collect(Collectors.toList()), ImmutableSet.of());
    }

    private static StaticAnnouncementConfig staticAnnouncementConfig(String type, String pool, String uri)
    {
        return new StaticAnnouncementConfig()
                .setType(type)
                .setPool(pool)
                .setProperties(ImmutableMap.of(schemeOf(uri), uri));
    }

    private static Service staticService(String uuid, String type, String pool, String uri)
    {
        return new Service(Id.valueOf(uuid), null, type, pool, "/location/" + uuid, ImmutableMap.of(schemeOf(uri), uri));
    }

    private static String schemeOf(String uri)
    {
        return uri.substring(0, uri.indexOf(':'));
    }
}

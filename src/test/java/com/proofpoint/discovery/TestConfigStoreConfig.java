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
import org.testng.annotations.Test;

import javax.validation.constraints.NotNull;
import java.util.Map;

import static com.proofpoint.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.proofpoint.configuration.testing.ConfigAssertions.assertLegacyEquivalence;
import static com.proofpoint.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.proofpoint.configuration.testing.ConfigAssertions.recordDefaults;
import static com.proofpoint.testing.ValidationAssertions.assertFailsValidation;

public class TestConfigStoreConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ConfigStoreConfig.class)
                .setAnnouncements(ImmutableMap.of()));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("store.static-announcement.foo.type", "sometype")
                .put("store.static-announcement.foo.property.http", "http://127.0.0.3")
                .build();

        ConfigStoreConfig expected = new ConfigStoreConfig()
                .setAnnouncements(ImmutableMap.of("foo", new StaticAnnouncementConfig()
                        .setType("sometype")
                        .setProperties(ImmutableMap.of("http", "http://127.0.0.3"))));

        assertFullMapping(properties, expected);
    }

    @Test
    public void testLegacyProperties()
    {
        assertLegacyEquivalence(ConfigStoreConfig.class,
                ImmutableMap.of());
    }

    @Test
    public void testRequiredConfigAbsent()
    {
        assertFailsValidation(new StaticAnnouncementConfig(), "type", "may not be null", NotNull.class);
        assertFailsValidation(new StaticAnnouncementConfig(), "properties", "may not be null", NotNull.class);
    }
}

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

import javax.validation.constraints.AssertFalse;
import javax.validation.constraints.NotNull;
import java.util.Map;

import static com.proofpoint.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.proofpoint.configuration.testing.ConfigAssertions.assertLegacyEquivalence;
import static com.proofpoint.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.proofpoint.configuration.testing.ConfigAssertions.recordDefaults;
import static com.proofpoint.testing.ValidationAssertions.assertFailsValidation;
import static com.proofpoint.testing.ValidationAssertions.assertValidates;

public class TestStaticAnnouncementConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(StaticAnnouncementConfig.class)
                .setType(null)
                .setPool("general")
                .setProperties(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("type", "sometype")
                .put("pool", "somepool")
                .put("property.http", "http://127.0.0.3")
                .build();

        StaticAnnouncementConfig expected = new StaticAnnouncementConfig()
                .setType("sometype")
                .setPool("somepool")
                .setProperties(ImmutableMap.of("http", "http://127.0.0.3"));

        assertFullMapping(properties, expected);
    }

    @Test
    public void testLegacyProperties()
    {
        assertLegacyEquivalence(StaticAnnouncementConfig.class,
                ImmutableMap.of("type", "sometype", "property.http", "http://127.0.0.3"));
    }

    @Test
    public void testRequiredConfigAbsent()
    {
        assertFailsValidation(new StaticAnnouncementConfig(), "type", "may not be null", NotNull.class);
        assertFailsValidation(new StaticAnnouncementConfig(), "properties", "may not be null", NotNull.class);
    }

    @Test
    public void testUriValidator()
    {
        StaticAnnouncementConfig config = new StaticAnnouncementConfig().setType("type");

        assertValidates(config.setProperties(ImmutableMap.of("foo", "bar")));
        assertValidates(config.setProperties(ImmutableMap.of("http", "http://10.20.3.4:5134")));
        assertValidates(config.setProperties(ImmutableMap.of("https", "https://foo.invalid:534")));
        assertValidates(config.setProperties(ImmutableMap.of("http", "http://10.20.3.4:5134", "https", "https://foo.invalid:534")));

        assertFailsValidation(config.setProperties(ImmutableMap.of("http", "http://10.20.3.4.5:5134")), "uriInvalid", "must be false", AssertFalse.class);
        assertFailsValidation(config.setProperties(ImmutableMap.of("https", "https://10.20.3.4.5:5134")), "uriInvalid", "must be false", AssertFalse.class);
        assertFailsValidation(config.setProperties(ImmutableMap.of("http", "https://10.20.3.4:5134")), "uriInvalid", "must be false", AssertFalse.class);
        assertFailsValidation(config.setProperties(ImmutableMap.of("https", "http://10.20.3.4:5134")), "uriInvalid", "must be false", AssertFalse.class);
        assertFailsValidation(config.setProperties(ImmutableMap.of("http", "http://")), "uriInvalid", "must be false", AssertFalse.class);
        assertFailsValidation(config.setProperties(ImmutableMap.of("https", "https://")), "uriInvalid", "must be false", AssertFalse.class);
    }
}

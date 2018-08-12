/*
 * Copyright 2013 Proofpoint, Inc.
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

import com.google.common.collect.ImmutableMap;
import com.proofpoint.json.JsonCodec;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static com.proofpoint.discovery.store.Entry.entry;
import static com.proofpoint.json.JsonCodec.jsonCodec;
import static com.proofpoint.json.testing.JsonTester.assertJsonEncode;
import static com.proofpoint.json.testing.JsonTester.decodeJson;
import static com.proofpoint.testing.ValidationAssertions.assertValidates;
import static org.testng.Assert.assertEquals;

public class TestEntry
{
    private final JsonCodec<Entry> codec = jsonCodec(Entry.class);
    private final Entry entry = entry(
            new byte[] { 0, 1, 2},
            new byte[] { 3, 4, 5},
            6789L,
            12345L
    );
    private Map<String,Object> jsonStructure;

    @BeforeMethod
    public void setup()
    {
        jsonStructure = new HashMap<>(ImmutableMap.<String, Object>of(
                "key", "AAEC",
                "value", "AwQF",
                "timestamp", 6789,
                "maxAgeInMs", 12345
        ));
    }

    @Test
    public void testJsonDecode()
    {
        assertEquals(assertValidates(decodeJson(codec, jsonStructure)), entry);
    }

    @Test
    public void testJsonEncode()
    {
        assertJsonEncode(entry, jsonStructure);
    }
}

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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.proofpoint.discovery.Id;
import com.proofpoint.discovery.Node;
import com.proofpoint.discovery.Service;
import com.proofpoint.json.JsonCodec;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.proofpoint.discovery.store.Entry.entry;
import static com.proofpoint.json.JsonCodec.jsonCodec;
import static com.proofpoint.json.testing.JsonTester.assertJsonEncode;
import static com.proofpoint.json.testing.JsonTester.decodeJson;
import static com.proofpoint.testing.ValidationAssertions.assertValidates;
import static org.assertj.core.api.Assertions.assertThat;

public class TestEntry
{
    private static final JsonCodec<Entry> ENTRY_CODEC = jsonCodec(Entry.class);
    private static final JsonCodec<List<Service>> SERVICE_LIST_CODEC = JsonCodec.listJsonCodec(Service.class);
    private static final Id<Node> NODE_ID = Id.valueOf("e8e71280-2325-4498-87a7-7f7d7d48defd");
    private static final Id<Service> SERVICE_ID_1 = Id.valueOf("efab997e-14b8-4f5a-b534-b3e70bfb8bd4");
    private static final Id<Service> SERVICE_ID_2 = Id.valueOf("d884bf44-7387-4e11-aabf-a32608776f8e");
    private static final ImmutableList<Service> SERVICES_LIST = ImmutableList.of(
            new Service(SERVICE_ID_1, NODE_ID, "testType", "testPool", "testLocation", ImmutableMap.of(
                    "http", "http://invalid.invalid",
                    "https", "https://invalid.invalid"
            )),
            new Service(SERVICE_ID_2, NODE_ID, "testType2", "testPool2", "testLocation2", ImmutableMap.of(
                    "http", "http://invalid2.invalid",
                    "https", "https://invalid2.invalid"
            ))
    );
    private static final Entry ENTRY = entry(
            NODE_ID.getBytes(),
            SERVICE_LIST_CODEC.toJsonBytes(SERVICES_LIST),
            6789L,
            12345L,
            "127.0.0.1");
    private static final Entry ENTRY_2 = entry(
            NODE_ID.getBytes(),
            SERVICES_LIST,
            6789L,
            12345L,
            "127.0.0.1");
    private static final Entry ENTRY_3 = entry(
            NODE_ID.getBytes(),
            SERVICES_LIST,
            6789L,
            12345L,
            null);
    private static final Entry TOMBSTONE_ENTRY = entry(
            NODE_ID.getBytes(),
            (byte[]) null,
            6789L,
            null,
            null);
    private static final Entry TOMBSTONE_ENTRY_2 = entry(
            NODE_ID.getBytes(),
            (List<Service>) null,
            6789L,
            null,
            null);

    private Map<String, Object> jsonStructure;

    @BeforeMethod
    public void setup()
    {
        jsonStructure = new HashMap<>(ImmutableMap.<String, Object>of(
                "key", "ZThlNzEyODAtMjMyNS00NDk4LTg3YTctN2Y3ZDdkNDhkZWZk",
                "value", "WyB7CiAgImlkIiA6ICJlZmFiOTk3ZS0xNGI4LTRmNWEtYjUzNC1iM2U3MGJmYjhiZDQiLAogICJub2RlSWQiIDogImU4ZTcxMjgwLTIzMjUtNDQ5OC04N2E3LTdmN2Q3ZDQ4ZGVmZCIsCiAgInR5cGUiIDogInRlc3RUeXBlIiwKICAicG9vbCIgOiAidGVzdFBvb2wiLAogICJsb2NhdGlvbiIgOiAidGVzdExvY2F0aW9uIiwKICAicHJvcGVydGllcyIgOiB7CiAgICAiaHR0cCIgOiAiaHR0cDovL2ludmFsaWQuaW52YWxpZCIsCiAgICAiaHR0cHMiIDogImh0dHBzOi8vaW52YWxpZC5pbnZhbGlkIgogIH0KfSwgewogICJpZCIgOiAiZDg4NGJmNDQtNzM4Ny00ZTExLWFhYmYtYTMyNjA4Nzc2ZjhlIiwKICAibm9kZUlkIiA6ICJlOGU3MTI4MC0yMzI1LTQ0OTgtODdhNy03ZjdkN2Q0OGRlZmQiLAogICJ0eXBlIiA6ICJ0ZXN0VHlwZTIiLAogICJwb29sIiA6ICJ0ZXN0UG9vbDIiLAogICJsb2NhdGlvbiIgOiAidGVzdExvY2F0aW9uMiIsCiAgInByb3BlcnRpZXMiIDogewogICAgImh0dHAiIDogImh0dHA6Ly9pbnZhbGlkMi5pbnZhbGlkIiwKICAgICJodHRwcyIgOiAiaHR0cHM6Ly9pbnZhbGlkMi5pbnZhbGlkIgogIH0KfSBd",
                "timestamp", 6789,
                "maxAgeInMs", 12345,
                "announcer", "127.0.0.1"
        ));
    }

    @Test
    public void testJsonDecode()
    {
        assertThat(assertValidates(decodeJson(ENTRY_CODEC, jsonStructure))).isEqualTo(ENTRY);
        assertThat(assertValidates(decodeJson(ENTRY_CODEC, jsonStructure))).isEqualTo(ENTRY_2);
        jsonStructure.remove("announcer");
        assertThat(assertValidates(decodeJson(ENTRY_CODEC, jsonStructure))).isEqualTo(ENTRY_3);
    }

    @Test
    public void testJsonDecodeTombstone()
    {
        jsonStructure.remove("value");
        jsonStructure.remove("maxAgeInMs");
        jsonStructure.remove("announcer");
        assertThat(assertValidates(decodeJson(ENTRY_CODEC, jsonStructure))).isEqualTo(TOMBSTONE_ENTRY);
        assertThat(assertValidates(decodeJson(ENTRY_CODEC, jsonStructure))).isEqualTo(TOMBSTONE_ENTRY_2);
    }

    @Test
    public void testJsonEncode()
    {
        assertJsonEncode(ENTRY, jsonStructure);
        assertJsonEncode(ENTRY_2, jsonStructure);
        jsonStructure.remove("announcer");
        assertJsonEncode(ENTRY_3, jsonStructure);
    }

    @Test
    public void testJsonEncodeTombstone()
    {
        jsonStructure.remove("value");
        jsonStructure.remove("maxAgeInMs");
        jsonStructure.remove("announcer");
        assertJsonEncode(TOMBSTONE_ENTRY, jsonStructure);
        assertJsonEncode(TOMBSTONE_ENTRY_2, jsonStructure);
    }
}

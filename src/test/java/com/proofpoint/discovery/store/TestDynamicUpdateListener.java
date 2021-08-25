/*
 * Copyright 2015 Proofpoint, Inc.
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
import com.proofpoint.discovery.Id;
import com.proofpoint.discovery.Node;
import com.proofpoint.discovery.Service;
import com.proofpoint.reporting.testing.TestingReportCollectionFactory;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

import static com.proofpoint.discovery.store.Entry.entry;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class TestDynamicUpdateListener
{
    @Mock
    private Supplier<Instant> instantSupplier;
    private DynamicRenewals argumentVerifier;
    private DynamicRenewals reportCollection;
    private DynamicUpdateListener listener;
    private final Id<Node> nodeId = Id.random();

    @BeforeMethod
    public void setup()
    {
        initMocks(this);
        TestingReportCollectionFactory reportCollectionFactory = new TestingReportCollectionFactory();
        DynamicRenewals dynamicRenewals = reportCollectionFactory.createReportCollection(DynamicRenewals.class);
        argumentVerifier = reportCollectionFactory.getArgumentVerifier(dynamicRenewals);
        reportCollection = reportCollectionFactory.getReportCollection(dynamicRenewals);
        listener = new DynamicUpdateListener(instantSupplier, dynamicRenewals);
    }

    @Test
    public void testOldTombstone()
    {
        Service service = new Service(Id.random(), nodeId, "type", "pool", "location", ImmutableMap.of());
        listener.notifyUpdate(entry(nodeId.getBytes(), (List<Service>) null, 5, 5L, "127.0.0.1"), entry(nodeId.getBytes(), List.of(service), 8, 5L, "127.0.0.1"));
        verifyNoMoreInteractions(argumentVerifier);
    }

    @Test
    public void testNewTombstone()
    {
        Service service = new Service(Id.random(), nodeId, "type", "pool", "location", ImmutableMap.of());
        listener.notifyUpdate(entry(nodeId.getBytes(), List.of(service), 5, 5L, "127.0.0.1"), entry(nodeId.getBytes(), (List<Service>) null, 8, 5L, "127.0.0.1"));
        verifyNoMoreInteractions(argumentVerifier);
    }

    @Test
    public void testUpdated()
    {
        Service service1 = new Service(Id.random(), nodeId, "type1", "pool", "location", ImmutableMap.of());
        Service service2 = new Service(Id.random(), nodeId, "type2", "pool", "location", ImmutableMap.of());
        Service service3 = new Service(Id.random(), nodeId, "type3", "pool", "location", ImmutableMap.of());
        Service service4 = new Service(Id.random(), nodeId, "type4", "pool", "location", ImmutableMap.of());
        when(instantSupplier.get()).thenReturn(Instant.ofEpochMilli(9));
        listener.notifyUpdate(entry(nodeId.getBytes(), List.of(service1, service2, service3), 5, 5L, "127.0.0.1"), entry(nodeId.getBytes(), List.of(service2, service3, service4), 8, 5L, "127.0.0.1"));
        verify(argumentVerifier).renewedAfter("type2");
        verify(argumentVerifier).renewedAfter("type3");
        verifyNoMoreInteractions(argumentVerifier);
        verify(reportCollection.renewedAfter("type2")).add(4, MILLISECONDS);
        verifyNoMoreInteractions(reportCollection.renewedAfter("type2"));
        verify(reportCollection.renewedAfter("type3")).add(4, MILLISECONDS);
        verifyNoMoreInteractions(reportCollection.renewedAfter("type3"));
    }

    @Test
    public void testExpired()
    {
        Service service1 = new Service(Id.random(), nodeId, "type1", "pool", "location", ImmutableMap.of());
        Service service2 = new Service(Id.random(), nodeId, "type2", "pool", "location", ImmutableMap.of());
        Service service3 = new Service(Id.random(), nodeId, "type3", "pool", "location", ImmutableMap.of());
        Service service4 = new Service(Id.random(), nodeId, "type4", "pool", "location", ImmutableMap.of());
        when(instantSupplier.get()).thenReturn(Instant.ofEpochMilli(11));
        listener.notifyUpdate(entry(nodeId.getBytes(), List.of(service1, service2, service3), 5, 5L, "127.0.0.1"), entry(nodeId.getBytes(), List.of(service2, service3, service4), 8, 5L, "127.0.0.1"));
        verify(argumentVerifier).expiredFor("type2");
        verify(argumentVerifier).expiredFor("type3");
        verifyNoMoreInteractions(argumentVerifier);
        verify(reportCollection.expiredFor("type2")).add(1, MILLISECONDS);
        verifyNoMoreInteractions(reportCollection.renewedAfter("type2"));
        verify(reportCollection.expiredFor("type3")).add(1, MILLISECONDS);
        verifyNoMoreInteractions(reportCollection.renewedAfter("type3"));
    }
}

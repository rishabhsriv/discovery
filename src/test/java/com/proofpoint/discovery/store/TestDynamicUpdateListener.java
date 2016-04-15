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

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.proofpoint.discovery.Id;
import com.proofpoint.discovery.Node;
import com.proofpoint.discovery.Service;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.reporting.testing.TestingReportCollectionFactory;
import org.joda.time.DateTime;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class TestDynamicUpdateListener
{
    private static final JsonCodec<List<Service>> CODEC = JsonCodec.listJsonCodec(Service.class);

    @Mock
    private Supplier<DateTime> dateTimeSupplier;
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
        listener = new DynamicUpdateListener(dateTimeSupplier, dynamicRenewals);
    }

    @Test
    public void testOldTombstone()
    {
        Service service = new Service(Id.random(), nodeId, "type", "pool", "location", ImmutableMap.of());
        listener.notifyUpdate(new Entry(nodeId.getBytes(), null, 5, 5L), new Entry(nodeId.getBytes(), CODEC.toJsonBytes(ImmutableList.of(service)), 8, 5L));
        verifyNoMoreInteractions(argumentVerifier);
    }

    @Test
    public void testNewTombstone()
    {
        Service service = new Service(Id.random(), nodeId, "type", "pool", "location", ImmutableMap.of());
        listener.notifyUpdate(new Entry(nodeId.getBytes(), CODEC.toJsonBytes(ImmutableList.of(service)), 5, 5L), new Entry(nodeId.getBytes(), null, 8, 5L));
        verifyNoMoreInteractions(argumentVerifier);
    }

    @Test
    public void testUpdated()
    {
        Service service1 = new Service(Id.random(), nodeId, "type1", "pool", "location", ImmutableMap.of());
        Service service2 = new Service(Id.random(), nodeId, "type2", "pool", "location", ImmutableMap.of());
        Service service3 = new Service(Id.random(), nodeId, "type3", "pool", "location", ImmutableMap.of());
        Service service4 = new Service(Id.random(), nodeId, "type4", "pool", "location", ImmutableMap.of());
        when(dateTimeSupplier.get()).thenReturn(new DateTime(9));
        listener.notifyUpdate(new Entry(nodeId.getBytes(), CODEC.toJsonBytes(ImmutableList.of(service1, service2, service3)), 5, 5L), new Entry(nodeId.getBytes(), CODEC.toJsonBytes(ImmutableList.of(service2, service3, service4)), 8, 5L));
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
        when(dateTimeSupplier.get()).thenReturn(new DateTime(11));
        listener.notifyUpdate(new Entry(nodeId.getBytes(), CODEC.toJsonBytes(ImmutableList.of(service1, service2, service3)), 5, 5L), new Entry(nodeId.getBytes(), CODEC.toJsonBytes(ImmutableList.of(service2, service3, service4)), 8, 5L));
        verify(argumentVerifier).expiredFor("type2");
        verify(argumentVerifier).expiredFor("type3");
        verifyNoMoreInteractions(argumentVerifier);
        verify(reportCollection.expiredFor("type2")).add(1, MILLISECONDS);
        verifyNoMoreInteractions(reportCollection.renewedAfter("type2"));
        verify(reportCollection.expiredFor("type3")).add(1, MILLISECONDS);
        verifyNoMoreInteractions(reportCollection.renewedAfter("type3"));
    }
}

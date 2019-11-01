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
import com.proofpoint.units.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.proofpoint.discovery.DynamicServiceAnnouncement.toServiceWith;
import static java.util.stream.Stream.concat;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class TestDynamicStore
{
    private static final Duration MAX_AGE = new Duration(1, TimeUnit.MINUTES);

    protected TestingTimeSupplier currentTime;
    protected DynamicStore store;

    protected abstract DynamicStore initializeStore(DiscoveryConfig config, Supplier<Instant> timeSupplier);

    @BeforeMethod
    public void setup()
    {
        currentTime = new TestingTimeSupplier();
        DiscoveryConfig config = new DiscoveryConfig().setMaxAge(new Duration(1, TimeUnit.MINUTES));
        store = initializeStore(config, currentTime);
    }

    @Test
    public void testEmpty()
    {
        assertThat(store.getAll()).as("store should be empty").isEmpty();
    }

    @Test
    public void testPutSingle()
    {
        Id<Node> nodeId = Id.random();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        store.put(nodeId, blue);

        assertThat(store.getAll())
                .containsExactlyInAnyOrder(blue.getServiceAnnouncements().stream()
                        .map(toServiceWith(nodeId, blue.getLocation(), blue.getPool()))
                        .toArray(Service[]::new));
    }

    @Test
    public void testExpires()
    {
        Id<Node> nodeId = Id.random();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        store.put(nodeId, blue);
        advanceTimeBeyondMaxAge();
        assertThat(store.getAll()).isEmpty();
    }

    @Test
    public void testPutMultipleForSameNode()
    {
        Id<Node> nodeId = Id.random();
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111")),
                new DynamicServiceAnnouncement(Id.random(), "web", ImmutableMap.of("http", "http://localhost:2222")),
                new DynamicServiceAnnouncement(Id.random(), "monitoring", ImmutableMap.of("http", "http://localhost:3333"))
        ));

        store.put(nodeId, announcement);

        assertThat(store.getAll())
                .containsExactlyInAnyOrder(announcement.getServiceAnnouncements().stream()
                        .map(toServiceWith(nodeId, announcement.getLocation(), announcement.getPool()))
                        .toArray(Service[]::new));
    }

    @Test
    public void testReplace()
    {
        Id<Node> nodeId = Id.random();

        DynamicAnnouncement oldAnnouncement = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        DynamicAnnouncement newAnnouncement = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        store.put(nodeId, oldAnnouncement);
        currentTime.increment();
        store.put(nodeId, newAnnouncement);

        assertThat(store.getAll())
                .containsExactlyInAnyOrder(newAnnouncement.getServiceAnnouncements().stream()
                        .map(toServiceWith(nodeId, newAnnouncement.getLocation(), newAnnouncement.getPool()))
                        .toArray(Service[]::new));
    }

    @Test
    public void testReplaceExpired()
    {
        Id<Node> nodeId = Id.random();

        DynamicAnnouncement oldAnnouncement = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        DynamicAnnouncement newAnnouncement = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        store.put(nodeId, oldAnnouncement);
        advanceTimeBeyondMaxAge();
        store.put(nodeId, newAnnouncement);

        assertThat(store.getAll())
                .containsExactlyInAnyOrder(newAnnouncement.getServiceAnnouncements().stream()
                        .map(toServiceWith(nodeId, newAnnouncement.getLocation(), newAnnouncement.getPool()))
                        .toArray(Service[]::new));
    }

    @Test
    public void testPutMultipleForDifferentNodes()
    {
        Id<Node> blueNodeId = Id.random();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        Id<Node> redNodeId = Id.random();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "web", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        Id<Node> greenNodeId = Id.random();
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot3", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "monitoring", ImmutableMap.of("http", "http://localhost:3333"))
        ));

        store.put(blueNodeId, blue);
        store.put(redNodeId, red);
        store.put(greenNodeId, green);

        assertThat(store.getAll())
                .containsExactlyInAnyOrder(
                        concat(
                                blue.getServiceAnnouncements().stream().map(toServiceWith(blueNodeId, blue.getLocation(), blue.getPool())),
                                concat(
                                        red.getServiceAnnouncements().stream().map(toServiceWith(redNodeId, red.getLocation(), red.getPool())),
                                        green.getServiceAnnouncements().stream().map(toServiceWith(greenNodeId, green.getLocation(), green.getPool()))))
                                .toArray(Service[]::new));
    }

    @Test
    public void testGetByType()
    {
        Id<Node> blueNodeId = Id.random();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        Id<Node> redNodeId = Id.random();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        Id<Node> greenNodeId = Id.random();
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot3", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "monitoring", ImmutableMap.of("http", "http://localhost:3333"))
        ));

        store.put(blueNodeId, blue);
        store.put(redNodeId, red);
        store.put(greenNodeId, green);

        assertThat(store.get("storage"))
                .containsExactlyInAnyOrder(
                        concat(
                                blue.getServiceAnnouncements().stream().map(toServiceWith(blueNodeId, blue.getLocation(), blue.getPool())),
                                red.getServiceAnnouncements().stream().map(toServiceWith(redNodeId, red.getLocation(), red.getPool())))
                                .toArray(Service[]::new));

        assertThat(store.get("monitoring"))
                .containsExactlyInAnyOrder(green.getServiceAnnouncements().stream()
                        .map(toServiceWith(greenNodeId, green.getLocation(), green.getPool()))
                        .toArray(Service[]::new));
    }

    @Test
    public void testGetByTypeAndPool()
    {
        Id<Node> blueNodeId = Id.random();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        Id<Node> redNodeId = Id.random();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        Id<Node> greenNodeId = Id.random();
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot3", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "monitoring", ImmutableMap.of("http", "http://localhost:3333"))
        ));

        Id<Node> yellowNodeId = Id.random();
        DynamicAnnouncement yellow = new DynamicAnnouncement("testing", "poolB", "/US/West/SC4/rack1/host1/vm1/slot4", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:4444"))
        ));

        store.put(blueNodeId, blue);
        store.put(redNodeId, red);
        store.put(greenNodeId, green);
        store.put(yellowNodeId, yellow);

        assertThat(store.get("storage", "poolA"))
                .containsExactlyInAnyOrder(
                        concat(
                                blue.getServiceAnnouncements().stream().map(toServiceWith(blueNodeId, blue.getLocation(), blue.getPool())),
                                red.getServiceAnnouncements().stream().map(toServiceWith(redNodeId, red.getLocation(), red.getPool())))
                                .toArray(Service[]::new));

        assertThat(store.get("monitoring", "poolA"))
                .containsExactlyInAnyOrder(green.getServiceAnnouncements().stream()
                        .map(toServiceWith(greenNodeId, red.getLocation(), red.getPool()))
                        .toArray(Service[]::new));

        assertThat(store.get("storage", "poolB"))
                .containsExactlyInAnyOrder(yellow.getServiceAnnouncements().stream()
                        .map(toServiceWith(yellowNodeId, red.getLocation(), red.getPool()))
                        .toArray(Service[]::new));
    }

    @Test
    public void testGetAnnouncer()
    {
        Id<Node> blueNodeId = Id.random();
        DynamicAnnouncement blue = DynamicAnnouncement.copyOf(new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))
        ))).setAnnouncer("127.0.0.1").build();

        Id<Node> redNodeId = Id.random();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        store.put(blueNodeId, blue);
        store.put(redNodeId, red);

        assertThat(store.getAnnouncer(blueNodeId)).isNotNull().isEqualTo("127.0.0.1");
        assertThat(store.getAnnouncer(redNodeId)).isNull();
        assertThat(store.getAnnouncer(Id.random())).isNull();
    }

    @Test
    public void testDelete()
    {
        Id<Node> blueNodeId = Id.random();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111")),
                new DynamicServiceAnnouncement(Id.random(), "web", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        Id<Node> redNodeId = Id.random();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "monitoring", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        store.put(blueNodeId, blue);
        store.put(redNodeId, red);

        assertThat(store.getAll())
                .containsExactlyInAnyOrder(
                        concat(
                                blue.getServiceAnnouncements().stream().map(toServiceWith(blueNodeId, blue.getLocation(), blue.getPool())),
                                red.getServiceAnnouncements().stream().map(toServiceWith(redNodeId, red.getLocation(), red.getPool())))
                                .toArray(Service[]::new));

        currentTime.increment();

        store.delete(blueNodeId);

        assertThat(store.getAll())
                .containsExactlyInAnyOrder(red.getServiceAnnouncements().stream()
                        .map(toServiceWith(redNodeId, red.getLocation(), red.getPool()))
                        .toArray(Service[]::new));

        assertThat(store.get("storage")).isEmpty();
        assertThat(store.get("web", "poolA")).isEmpty();
    }

    @Test
    public void testDeleteThenReInsert()
    {
        Id<Node> redNodeId = Id.random();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "monitoring", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        store.put(redNodeId, red);
        assertThat(store.getAll())
                .containsExactlyInAnyOrder(red.getServiceAnnouncements().stream()
                        .map(toServiceWith(redNodeId, red.getLocation(), red.getPool()))
                        .toArray(Service[]::new));

        currentTime.increment();

        store.delete(redNodeId);

        currentTime.increment();

        store.put(redNodeId, red);

        assertThat(store.getAll())
                .containsExactlyInAnyOrder(red.getServiceAnnouncements().stream()
                        .map(toServiceWith(redNodeId, red.getLocation(), red.getPool()))
                        .toArray(Service[]::new));
    }

    @Test
    public void testCanHandleLotsOfAnnouncements()
    {
        ImmutableSet.Builder<Service> builder = ImmutableSet.builder();
        for (int i = 0; i < 5000; ++i) {
            Id<Node> id = Id.random();
            DynamicServiceAnnouncement serviceAnnouncement = new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111"));
            DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(serviceAnnouncement));

            store.put(id, announcement);
            builder.add(new Service(serviceAnnouncement.getId(),
                    id,
                    serviceAnnouncement.getType(),
                    announcement.getPool(),
                    announcement.getLocation(),
                    serviceAnnouncement.getProperties()));
        }

        assertThat(store.getAll().collect(Collectors.toList())).hasSameElementsAs(builder.build());
    }


    private void advanceTimeBeyondMaxAge()
    {
        currentTime.add(new Duration(MAX_AGE.toMillis() * 2, TimeUnit.MILLISECONDS));
    }
}

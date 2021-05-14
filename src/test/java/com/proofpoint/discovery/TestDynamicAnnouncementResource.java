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
import com.proofpoint.discovery.DiscoveryConfig.StringSet;
import com.proofpoint.discovery.store.RealTimeSupplier;
import com.proofpoint.node.NodeInfo;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.Response.Status.ACCEPTED;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestDynamicAnnouncementResource
{
    private InMemoryDynamicStore store;
    private AuthManager authManager;
    private DynamicAnnouncementResource resource;
    private HttpServletRequest servletRequest;

    @BeforeMethod
    public void setup()
    {
        store = new InMemoryDynamicStore(new DiscoveryConfig(), new RealTimeSupplier());
        authManager = mock(AuthManager.class);
        resource = new DynamicAnnouncementResource(store, new NodeInfo("testing"), new DiscoveryConfig(), authManager);
        servletRequest = mock(HttpServletRequest.class);
        when(servletRequest.getRemoteAddr()).thenReturn("127.0.0.1");
    }

    @Test
    public void testPutNew()
    {
        DynamicServiceAnnouncement serviceAnnouncement = new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111"));
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "alpha", "/a/b/c", ImmutableSet.of(
                serviceAnnouncement)
        );

        Id<Node> nodeId = Id.random();
        Response response = resource.put(nodeId, announcement, servletRequest);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(ACCEPTED.getStatusCode());

        assertThat(store.getAll()).hasSize(1);
        Service service = store.getAll().iterator().next();

        assertThat(service.getId()).isNotNull();
        assertThat(service.getNodeId()).isEqualTo(nodeId);
        assertThat(service.getLocation()).isEqualTo(announcement.getLocation());
        assertThat(service.getType()).isEqualTo(serviceAnnouncement.getType());
        assertThat(service.getPool()).isEqualTo(announcement.getPool());
        assertThat(service.getProperties()).isEqualTo(serviceAnnouncement.getProperties());
    }

    @Test
    public void testPutAuthFailed()
    {
        DynamicServiceAnnouncement serviceAnnouncement = new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111"));
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "alpha", "/a/b/c", ImmutableSet.of(
                serviceAnnouncement)
        );
        Id<Node> nodeId = Id.random();
        doThrow(ForbiddenException.class).when(authManager).checkAuthAnnounce(nodeId, announcement, servletRequest);
        assertThatExceptionOfType(ForbiddenException.class)
                .isThrownBy(() -> resource.put(nodeId, announcement, servletRequest))
                .withNoCause();
        assertThat(store.getAll()).isEmpty();
    }

    @Test
    public void testReplace()
    {
        Id<Node> nodeId = Id.random();
        DynamicAnnouncement previous = new DynamicAnnouncement("testing", "alpha", "/a/b/c", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("key", "existing"))
        ));

        store.put(nodeId, previous);

        DynamicServiceAnnouncement serviceAnnouncement = new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("key", "new"));
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "alpha", "/a/b/c", ImmutableSet.of(
                serviceAnnouncement)
        );

        Response response = resource.put(nodeId, announcement, servletRequest);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(ACCEPTED.getStatusCode());

        assertThat(store.getAll()).hasSize(1);
        Service service = store.getAll().iterator().next();

        assertThat(service.getId()).isNotNull();
        assertThat(service.getNodeId()).isEqualTo(nodeId);
        assertThat(service.getLocation()).isEqualTo(announcement.getLocation());
        assertThat(service.getType()).isEqualTo(serviceAnnouncement.getType());
        assertThat(service.getPool()).isEqualTo(announcement.getPool());
        assertThat(service.getProperties()).isEqualTo(serviceAnnouncement.getProperties());
    }

    @Test
    public void testEnvironmentConflict()
    {
        DynamicAnnouncement announcement = new DynamicAnnouncement("production", "alpha", "/a/b/c", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111")))
        );

        Id<Node> nodeId = Id.random();
        Response response = resource.put(nodeId, announcement, servletRequest);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(BAD_REQUEST.getStatusCode());

        assertThat(store.getAll()).isEmpty();
    }

    @Test
    public void testPutProxied()
    {
        resource = new DynamicAnnouncementResource(store, new NodeInfo("testing"),
                new DiscoveryConfig().setProxyProxiedTypes(StringSet.of("storage")), authManager);

        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "alpha", "/a/b/c", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111")))
        );

        Id<Node> nodeId = Id.random();
        Response response = resource.put(nodeId, announcement, servletRequest);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(FORBIDDEN.getStatusCode());

        assertThat(store.getAll()).isEmpty();
    }

    @Test
    public void testDeleteExisting()
    {
        Id<Node> blueNodeId = Id.random();
        DynamicServiceAnnouncement serviceAnnouncement = new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("key", "valueBlue"));
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "alpha", "/a/b/c", ImmutableSet.of(
                serviceAnnouncement
        ));

        Id<Node> redNodeId = Id.random();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "alpha", "/a/b/c", ImmutableSet.of(
                serviceAnnouncement
        ));

        store.put(redNodeId, red);
        store.put(blueNodeId, blue);

        resource.delete(blueNodeId, servletRequest);

        assertThat(store.getAll()).hasSize(1);
        Service service = store.getAll().iterator().next();

        assertThat(service.getId()).isNotNull();
        assertThat(service.getNodeId()).isEqualTo(redNodeId);
        assertThat(service.getLocation()).isEqualTo(red.getLocation());
        assertThat(service.getType()).isEqualTo(serviceAnnouncement.getType());
        assertThat(service.getPool()).isEqualTo(red.getPool());
        assertThat(service.getProperties()).isEqualTo(serviceAnnouncement.getProperties());
    }

    @Test
    public void testDeleteAuthFailed()
    {
        Id<Node> blueNodeId = Id.random();
        DynamicServiceAnnouncement serviceAnnouncement = new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("key", "valueBlue"));
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "alpha", "/a/b/c", ImmutableSet.of(
                serviceAnnouncement
        ));
        store.put(blueNodeId, blue);

        doThrow(ForbiddenException.class).when(authManager).checkAuthDelete(blueNodeId, servletRequest);
        assertThatExceptionOfType(ForbiddenException.class)
                .isThrownBy(() -> resource.delete(blueNodeId, servletRequest))
                .withNoCause();

        assertThat(store.getAll()).hasSize(1);
        Service service = store.getAll().iterator().next();
        assertThat(service.getId()).isNotNull();
        assertThat(service.getNodeId()).isEqualTo(blueNodeId);
        assertThat(service.getLocation()).isEqualTo(blue.getLocation());
        assertThat(service.getType()).isEqualTo(serviceAnnouncement.getType());
        assertThat(service.getPool()).isEqualTo(blue.getPool());
        assertThat(service.getProperties()).isEqualTo(serviceAnnouncement.getProperties());
    }

    @Test
    public void testDeleteMissing()
    {
        resource.delete(Id.random(), servletRequest);

        assertThat(store.getAll()).isEmpty();
    }

    @Test
    public void testMakesUpLocation()
    {
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "alpha", null, ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111")))
        );

        Id<Node> nodeId = Id.random();
        Response response = resource.put(nodeId, announcement, servletRequest);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(ACCEPTED.getStatusCode());

        assertThat(store.getAll()).hasSize(1);
        Service service = store.getAll().iterator().next();
        assertThat(service.getId()).isEqualTo(service.getId());
        assertThat(service.getLocation()).isNotNull();
    }
}

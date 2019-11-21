package com.proofpoint.discovery;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.proofpoint.audit.testing.TestingAuditLog;
import com.proofpoint.audit.testing.TestingAuditLogModule;
import com.proofpoint.bootstrap.LifeCycleManager;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.discovery.client.ServiceState;
import com.proofpoint.discovery.store.StoreConfig;
import com.proofpoint.json.JsonModule;
import com.proofpoint.testing.SerialScheduledExecutorService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.ForbiddenException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.proofpoint.audit.AuditLoggerBinder.auditLoggerBinder;
import static com.proofpoint.bootstrap.Bootstrap.bootstrapTest;
import static com.proofpoint.configuration.ConfigBinder.bindConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestIpHostnameAuthManager
{
    private final ServiceDescriptor discoveryDescriptor = new ServiceDescriptor(
            UUID.randomUUID(),
            UUID.randomUUID().toString(),
            "discovery",
            "general",
            "/location",
            ServiceState.RUNNING,
            ImmutableMap.of("http", "http://localhost:4111"));
    private final DiscoverySelector selector = new DiscoverySelector().add(discoveryDescriptor);
    private final TestingTimeSupplier timeSupplier = new TestingTimeSupplier();
    private final DiscoveryConfig discoveryConfig = new DiscoveryConfig();
    private InMemoryDynamicStore dynamicStore;
    private SerialScheduledExecutorService executor;
    private LifeCycleManager lifeCycleManager;
    private AuthManager authManager;
    private TestingAuditLog auditLog;

    @BeforeMethod
    public void beforeMethod()
            throws Exception
    {
        dynamicStore = new InMemoryDynamicStore(discoveryConfig, timeSupplier);
        executor = new SerialScheduledExecutorService();
        Injector injector = bootstrapTest()
                .withModules(
                        new JsonModule(),
                        new TestingAuditLogModule(),
                        (binder -> {
                            binder.bind(DynamicStore.class).toInstance(dynamicStore);
                            binder.bind(ServiceSelector.class).toInstance(selector);
                            bindConfig(binder).bind(StoreConfig.class);
                            binder.bind(ScheduledExecutorService.class).annotatedWith(ForAuthManager.class).toInstance(executor);
                            auditLoggerBinder(binder).bind(AuthAuditRecord.class);
                            binder.bind(AuthManager.class).to(IpHostnameAuthManager.class).in(Scopes.SINGLETON);
                        }))
                .initialize();
        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        authManager = injector.getInstance(AuthManager.class);
        auditLog = injector.getInstance(TestingAuditLog.class);
    }

    @AfterMethod
    public void afterMethod()
            throws Exception
    {
        lifeCycleManager.stop();
    }

    @Test
    public void testReplicate()
    {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        assertThatCode(() -> authManager.checkAuthReplicate(request)).doesNotThrowAnyException();
        assertThat(auditLog.getRecords()).isEmpty();
    }

    @Test
    public void testReplicateMismatch()
    {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("10.20.30.40");
        assertThatExceptionOfType(ForbiddenException.class)
                .isThrownBy(() -> authManager.checkAuthReplicate(request))
                .withMessageContaining("HTTP 403")
                .withNoCause();
        assertThat(auditLog.getRecords()).hasSize(1)
                .extracting("message")
                .contains("IP 10.20.30.40 tried to replicate as Discovery");
    }

    @Test
    public void testReplicateDiscoveryRemovedAdded()
    {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        assertThatCode(() -> authManager.checkAuthReplicate(request)).doesNotThrowAnyException();
        selector.clear();
        executor.elapseTime(6, TimeUnit.SECONDS);
        assertThatExceptionOfType(ForbiddenException.class)
                .isThrownBy(() -> authManager.checkAuthReplicate(request))
                .withMessageContaining("HTTP 403")
                .withNoCause();
        selector.add(discoveryDescriptor);
        executor.elapseTime(6, TimeUnit.SECONDS);
        assertThatCode(() -> authManager.checkAuthReplicate(request)).doesNotThrowAnyException();
        assertThat(auditLog.getRecords()).hasSize(1)
                .extracting("message")
                .contains("IP 127.0.0.1 tried to replicate as Discovery");
    }

    @Test
    public void testDelete()
    {
        Id<Node> nodeId = Id.random();
        DynamicAnnouncement toDelete = DynamicAnnouncement.copyOf(new DynamicAnnouncement("testing", "general", "/location", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:4111"))
        ))).setAnnouncer("10.20.30.40").build();
        dynamicStore.put(nodeId, toDelete);

        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("10.20.30.40");
        assertThatCode(() -> authManager.checkAuthDelete(nodeId, request)).doesNotThrowAnyException();
        //Tombstone
        dynamicStore.delete(nodeId);
        assertThatCode(() -> authManager.checkAuthDelete(nodeId, request)).doesNotThrowAnyException();
        assertThat(auditLog.getRecords()).isEmpty();
    }

    @Test
    public void testDeleteNoEntry()
    {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("10.20.30.40");
        assertThatCode(() -> authManager.checkAuthDelete(Id.random(), request)).doesNotThrowAnyException();
        assertThat(auditLog.getRecords()).isEmpty();
    }

    @Test
    public void testDeleteNullAnnouncer()
    {
        Id<Node> nodeId = Id.random();
        DynamicAnnouncement toDelete = new DynamicAnnouncement("testing", "general", "/location", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:4111"))
        ));
        dynamicStore.put(nodeId, toDelete);

        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("10.20.30.40");
        assertThatCode(() -> authManager.checkAuthDelete(nodeId, request)).doesNotThrowAnyException();
        assertThat(auditLog.getRecords()).isEmpty();
    }

    @Test
    public void testDeleteMismatch()
    {
        Id<Node> nodeId = Id.random();
        DynamicAnnouncement toDelete = DynamicAnnouncement.copyOf(new DynamicAnnouncement("testing", "general", "/location", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:4111"))
        ))).setAnnouncer("10.20.30.40").build();
        dynamicStore.put(nodeId, toDelete);

        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        assertThatExceptionOfType(ForbiddenException.class)
                .isThrownBy(() -> authManager.checkAuthDelete(nodeId, request))
                .withMessageContaining("HTTP 403")
                .withNoCause();
        assertThat(auditLog.getRecords()).hasSize(1)
                .extracting("message")
                .contains(String.format("IP 127.0.0.1 tried to delete node %s owned by IP 10.20.30.40", nodeId.toString()));
    }

    @Test
    public void testAnnounce()
    {
        Id<Node> nodeId = Id.random();
        DynamicServiceAnnouncement localhostServiceAnnouncement = new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:4111"));
        DynamicAnnouncement localhostAnnouncement = new DynamicAnnouncement("testing", "general", "/location", ImmutableSet.of(localhostServiceAnnouncement));
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        //Announce new
        assertThatCode(() -> authManager.checkAuthAnnounce(nodeId, localhostAnnouncement, request)).doesNotThrowAnyException();
        dynamicStore.put(nodeId, DynamicAnnouncement.copyOf(localhostAnnouncement).setAnnouncer("127.0.0.1").build());
        //Re-announce
        assertThatCode(() -> authManager.checkAuthAnnounce(nodeId, localhostAnnouncement, request)).doesNotThrowAnyException();
        //Announce tombstone
        dynamicStore.delete(nodeId);
        assertThatCode(() -> authManager.checkAuthAnnounce(nodeId, localhostAnnouncement, request)).doesNotThrowAnyException();
        assertThat(auditLog.getRecords()).isEmpty();
    }

    @Test
    public void testAnnounceNullAnnouncer()
    {
        Id<Node> nodeId = Id.random();
        DynamicServiceAnnouncement localhostServiceAnnouncement = new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:4111"));
        DynamicAnnouncement localhostAnnouncement = new DynamicAnnouncement("testing", "general", "/location", ImmutableSet.of(localhostServiceAnnouncement));
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        dynamicStore.put(nodeId, localhostAnnouncement);
        assertThatCode(() -> authManager.checkAuthAnnounce(nodeId, localhostAnnouncement, request)).doesNotThrowAnyException();
        assertThat(auditLog.getRecords()).isEmpty();
    }

    @Test
    public void testAnnounceMismatch()
    {
        Id<Node> nodeId = Id.random();
        DynamicServiceAnnouncement localhostServiceAnnouncement = new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:4111"));
        DynamicAnnouncement localhostAnnouncement = new DynamicAnnouncement("testing", "general", "/location", ImmutableSet.of(localhostServiceAnnouncement));
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("10.20.30.40");
        assertThatExceptionOfType(ForbiddenException.class)
                .isThrownBy(() -> authManager.checkAuthAnnounce(nodeId, localhostAnnouncement, request))
                .withMessageContaining("HTTP 403")
                .withNoCause();
        //Re-announce
        dynamicStore.put(nodeId, DynamicAnnouncement.copyOf(localhostAnnouncement).setAnnouncer("127.0.0.1").build());
        assertThatExceptionOfType(ForbiddenException.class)
                .isThrownBy(() -> authManager.checkAuthAnnounce(nodeId, localhostAnnouncement, request))
                .withMessageContaining("HTTP 403")
                .withNoCause();
        assertThat(auditLog.getRecords()).hasSize(2)
                .extracting("message")
                .contains("IP 10.20.30.40 tried to announce other host localhost",
                        String.format("IP 10.20.30.40 tried to re-announce node %s owned by IP 127.0.0.1", nodeId.toString()));
    }

    @Test
    public void testAnnounceMultipleServiceDifferentHost()
    {
        DynamicServiceAnnouncement localhostServiceAnnouncement = new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:4111"));
        DynamicServiceAnnouncement exampleServiceAnnouncement = new DynamicServiceAnnouncement(Id.random(), "example", ImmutableMap.of("http", "http://example.com:4111"));
        HttpServletRequest request = mock(HttpServletRequest.class);
        DynamicAnnouncement multipleAnnouncement = new DynamicAnnouncement("testing", "general", "/location", ImmutableSet.of(exampleServiceAnnouncement, localhostServiceAnnouncement));
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        assertThatExceptionOfType(ForbiddenException.class)
                .isThrownBy(() -> authManager.checkAuthAnnounce(Id.random(), multipleAnnouncement, request))
                .withMessageContaining("HTTP 403")
                .withNoCause();
        assertThat(auditLog.getRecords()).hasSize(1)
                .extracting("message")
                .contains("IP 127.0.0.1 tried to announce other host example.com");
    }

    @Test
    public void testAnnounceSingleServiceDifferentHosts()
    {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        DynamicServiceAnnouncement multiplePropsServiceAnnoucement = new DynamicServiceAnnouncement(Id.random(), "example", ImmutableMap.of("http", "http://example.com:4111", "https", "https://localhost:4111"));
        DynamicAnnouncement multiplePropsAnnouncement = new DynamicAnnouncement("testing", "general", "/location", ImmutableSet.of(multiplePropsServiceAnnoucement));
        assertThatExceptionOfType(ForbiddenException.class)
                .isThrownBy(() -> authManager.checkAuthAnnounce(Id.random(), multiplePropsAnnouncement, request))
                .withMessageContaining("HTTP 403")
                .withNoCause();
        assertThat(auditLog.getRecords()).hasSize(1)
                .extracting("message")
                .contains("IP 127.0.0.1 tried to announce other host example.com");
    }

    @Test
    public void testAnnounceInvalidProperty()
    {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");

        DynamicServiceAnnouncement invalidPropServiceAnnouncement = new DynamicServiceAnnouncement(Id.random(), "example", ImmutableMap.of("http", "http://example"));
        DynamicAnnouncement invalidPropAnnouncement = new DynamicAnnouncement("testing", "general", "/location", ImmutableSet.of(invalidPropServiceAnnouncement));
        assertThatExceptionOfType(ForbiddenException.class)
                .isThrownBy(() -> authManager.checkAuthAnnounce(Id.random(), invalidPropAnnouncement, request))
                .withMessageContaining("HTTP 403")
                .withNoCause();
        assertThat(auditLog.getRecords()).hasSize(1)
                .extracting("message")
                .contains("example: nodename nor servname provided, or not known");
    }

    private static class DiscoverySelector implements ServiceSelector
    {
        private final Set<ServiceDescriptor> serviceDescriptors = new HashSet<>();

        DiscoverySelector add(ServiceDescriptor serviceDescriptor)
        {
            serviceDescriptors.add(serviceDescriptor);
            return this;
        }

        void clear()
        {
            serviceDescriptors.clear();
        }

        @Override
        public String getType()
        {
            return "discovery";
        }

        @Override
        public String getPool()
        {
            return "general";
        }

        @Override
        public List<ServiceDescriptor> selectAllServices()
        {
            return ImmutableList.copyOf(serviceDescriptors);
        }
    }
}

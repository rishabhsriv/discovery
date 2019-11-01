package com.proofpoint.discovery;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceState;
import com.proofpoint.discovery.client.testing.StaticServiceSelector;
import com.proofpoint.discovery.store.StoreConfig;
import com.proofpoint.node.NodeInfo;
import com.proofpoint.testing.SerialScheduledExecutorService;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.ForbiddenException;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class TestIpHostnameAuthManager
{
    private InMemoryDynamicStore dynamicStore;
    private final StaticServiceSelector selector = new StaticServiceSelector(new ServiceDescriptor(
            UUID.randomUUID(),
            UUID.randomUUID().toString(),
            "discovery",
            "general",
            "/location",
            ServiceState.RUNNING,
            ImmutableMap.of("http", "http://localhost:4111")));
    private final NodeInfo nodeInfo = new NodeInfo("test_environment");
    private SerialScheduledExecutorService executor;

    @BeforeMethod
    public void beforeMethod()
    {
        initMocks(this);
        dynamicStore = new InMemoryDynamicStore(new DiscoveryConfig(), new TestingTimeSupplier());
        executor = new SerialScheduledExecutorService();
    }

    @Test
    public void testReplicate()
    {
        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, selector, nodeInfo, new StoreConfig(), executor);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        assertThatCode(() -> authManager.checkAuthReplicate(request)).doesNotThrowAnyException();
    }

    @Test
    public void testReplicateMismatch()
    {
        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, selector, nodeInfo, new StoreConfig(), executor);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("10.20.30.40");
        assertThatExceptionOfType(ForbiddenException.class)
                .isThrownBy(() -> authManager.checkAuthReplicate(request))
                .withMessageContaining("HTTP 403")
                .withNoCause();
    }

    @Test
    public void testReplicateEmptyDiscoveryList()
    {
        StaticServiceSelector selector = new StaticServiceSelector(new ServiceDescriptor(
                UUID.randomUUID(),
                nodeInfo.getNodeId(),
                "discovery",
                "general",
                "/location",
                ServiceState.RUNNING,
                ImmutableMap.of("http", "http://localhost:4111")));
        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, selector, nodeInfo, new StoreConfig(), executor);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        assertThatExceptionOfType(ForbiddenException.class)
                .isThrownBy(() -> authManager.checkAuthReplicate(request))
                .withMessageContaining("HTTP 403")
                .withNoCause();
    }

    @Test
    public void testReplicateEmptySelector()
    {
        StaticServiceSelector selector = new StaticServiceSelector();
        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, selector, nodeInfo, new StoreConfig(), executor);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        assertThatExceptionOfType(ForbiddenException.class)
                .isThrownBy(() -> authManager.checkAuthReplicate(request))
                .withMessageContaining("HTTP 403")
                .withNoCause();
    }

    @Test
    public void testDelete()
    {
        Id<Node> nodeId = Id.random();
        DynamicAnnouncement toDelete = DynamicAnnouncement.copyOf(new DynamicAnnouncement("testing", "general", "/location", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:4111"))
        ))).setAnnouncer("10.20.30.40").build();
        dynamicStore.put(nodeId, toDelete);

        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, selector, nodeInfo, new StoreConfig(), executor);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("10.20.30.40");
        assertThatCode(() -> authManager.checkAuthDelete(nodeId, request)).doesNotThrowAnyException();
    }

    @Test
    public void testDeleteNoEntry()
    {
        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, selector, nodeInfo, new StoreConfig(), executor);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("10.20.30.40");
        assertThatCode(() -> authManager.checkAuthDelete(Id.random(), request)).doesNotThrowAnyException();
    }

    @Test
    public void testDeleteNullAnnouncer()
    {
        Id<Node> nodeId = Id.random();
        DynamicAnnouncement toDelete = new DynamicAnnouncement("testing", "general", "/location", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:4111"))
        ));
        dynamicStore.put(nodeId, toDelete);

        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, selector, nodeInfo, new StoreConfig(), executor);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("10.20.30.40");
        assertThatCode(() -> authManager.checkAuthDelete(nodeId, request)).doesNotThrowAnyException();
    }

    @Test
    public void testDeleteMismatch()
    {
        Id<Node> nodeId = Id.random();
        DynamicAnnouncement toDelete = DynamicAnnouncement.copyOf(new DynamicAnnouncement("testing", "general", "/location", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:4111"))
        ))).setAnnouncer("10.20.30.40").build();
        dynamicStore.put(nodeId, toDelete);

        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, selector, nodeInfo, new StoreConfig(), executor);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        assertThatExceptionOfType(ForbiddenException.class)
                .isThrownBy(() -> authManager.checkAuthDelete(nodeId, request))
                .withMessageContaining("HTTP 403")
                .withNoCause();
    }

    @Test
    public void testAnnounce()
    {
        Id<Node> nodeId = Id.random();
        DynamicServiceAnnouncement localhostServiceAnnouncement = new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:4111"));
        DynamicAnnouncement localhostAnnouncement = new DynamicAnnouncement("testing", "general", "/location", ImmutableSet.of(localhostServiceAnnouncement));
        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, selector, nodeInfo, new StoreConfig(), executor);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        //Announce new
        assertThatCode(() -> authManager.checkAuthAnnounce(nodeId, localhostAnnouncement, request)).doesNotThrowAnyException();
        dynamicStore.put(nodeId, DynamicAnnouncement.copyOf(localhostAnnouncement).setAnnouncer("127.0.0.1").build());
        //Re-announce
        assertThatCode(() -> authManager.checkAuthAnnounce(nodeId, localhostAnnouncement, request)).doesNotThrowAnyException();
    }

    @Test
    public void testAnnounceNullAnnouncer()
    {
        Id<Node> nodeId = Id.random();
        DynamicServiceAnnouncement localhostServiceAnnouncement = new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:4111"));
        DynamicAnnouncement localhostAnnouncement = new DynamicAnnouncement("testing", "general", "/location", ImmutableSet.of(localhostServiceAnnouncement));
        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, selector, nodeInfo, new StoreConfig(), executor);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        dynamicStore.put(nodeId, localhostAnnouncement);
        assertThatCode(() -> authManager.checkAuthAnnounce(nodeId, localhostAnnouncement, request)).doesNotThrowAnyException();
    }

    @Test
    public void testAnnounceMismatch()
    {
        Id<Node> nodeId = Id.random();
        DynamicServiceAnnouncement localhostServiceAnnouncement = new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:4111"));
        DynamicAnnouncement localhostAnnouncement = new DynamicAnnouncement("testing", "general", "/location", ImmutableSet.of(localhostServiceAnnouncement));
        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, selector, nodeInfo, new StoreConfig(), executor);
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
    }

    @Test
    public void testAnnounceMultipleServiceDifferentHost()
    {
        DynamicServiceAnnouncement localhostServiceAnnouncement = new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:4111"));
        DynamicServiceAnnouncement exampleServiceAnnouncement = new DynamicServiceAnnouncement(Id.random(), "example", ImmutableMap.of("http", "http://example.com:4111"));
        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, selector, nodeInfo, new StoreConfig(), executor);
        HttpServletRequest request = mock(HttpServletRequest.class);
        DynamicAnnouncement multipleAnnouncement = new DynamicAnnouncement("testing", "general", "/location", ImmutableSet.of(exampleServiceAnnouncement, localhostServiceAnnouncement));
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        assertThatExceptionOfType(ForbiddenException.class)
                .isThrownBy(() -> authManager.checkAuthAnnounce(Id.random(), multipleAnnouncement, request))
                .withMessageContaining("HTTP 403")
                .withNoCause();
    }

    @Test
    public void testAnnounceSingleServiceDifferentHosts()
    {
        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, selector, nodeInfo, new StoreConfig(), executor);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        DynamicServiceAnnouncement multiplePropsServiceAnnoucement = new DynamicServiceAnnouncement(Id.random(), "example", ImmutableMap.of("http", "http://example.com:4111", "https", "https://localhost:4111"));
        DynamicAnnouncement multiplePropsAnnouncement = new DynamicAnnouncement("testing", "general", "/location", ImmutableSet.of(multiplePropsServiceAnnoucement));
        assertThatExceptionOfType(ForbiddenException.class)
                .isThrownBy(() -> authManager.checkAuthAnnounce(Id.random(), multiplePropsAnnouncement, request))
                .withMessageContaining("HTTP 403")
                .withNoCause();
    }

    @Test
    public void testAnnounceInvalidProperty()
    {
        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, selector, nodeInfo, new StoreConfig(), executor);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");

        DynamicServiceAnnouncement invalidPropServiceAnnouncement = new DynamicServiceAnnouncement(Id.random(), "example", ImmutableMap.of("http", "http://example"));
        DynamicAnnouncement invalidPropAnnouncement = new DynamicAnnouncement("testing", "general", "/location", ImmutableSet.of(invalidPropServiceAnnouncement));
        assertThatExceptionOfType(ForbiddenException.class)
                .isThrownBy(() -> authManager.checkAuthAnnounce(Id.random(), invalidPropAnnouncement, request))
                .withMessageContaining("HTTP 403")
                .withNoCause();
    }
}

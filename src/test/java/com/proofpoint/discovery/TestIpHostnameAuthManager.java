package com.proofpoint.discovery;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.ForbiddenException;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class TestIpHostnameAuthManager
{
    @Mock
    private ConfigStore configStore;

    private InMemoryDynamicStore dynamicStore;
    private final Service discovery1 = new Service(Id.random(), Id.random(), "discovery", "general", "/somewhere", ImmutableMap.of("http", "http://localhost:4111"));
    private final Service discovery2 = new Service(Id.random(), Id.random(), "discovery", "general", "/other", ImmutableMap.of("http", "http://127.0.0.1:4111"));

    @BeforeMethod
    public void beforeMethod()
    {
        initMocks(this);
        dynamicStore = new InMemoryDynamicStore(new DiscoveryConfig(), new TestingTimeSupplier());
        when(configStore.get("discovery")).thenReturn(Stream.of(discovery1, discovery2));
    }

    @Test
    public void testConstruct()
    {
        assertThatCode(() -> new IpHostnameAuthManager(dynamicStore, configStore)).doesNotThrowAnyException();
        when(configStore.get("discovery")).thenReturn(Stream.of());
        assertThatCode(() -> new IpHostnameAuthManager(dynamicStore, configStore)).doesNotThrowAnyException();
        when(configStore.get("discovery")).thenReturn(Stream.of(
                new Service(Id.random(), Id.random(), "discovery", "general", "/location", ImmutableMap.of())));
        assertThatCode(() -> new IpHostnameAuthManager(dynamicStore, configStore)).doesNotThrowAnyException();
        when(configStore.get("discovery")).thenReturn(Stream.of(
                new Service(Id.random(), Id.random(), "discovery", "general", "/location", ImmutableMap.of("http", "http://invalid"))));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new IpHostnameAuthManager(dynamicStore, configStore))
                .withMessage("Unable to build discovery IP list");
    }

    @Test
    public void testRead()
    {
        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, configStore);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        assertThatCode(() -> authManager.checkAuthRead(request)).doesNotThrowAnyException();
    }

    @Test
    public void testReplicate()
    {
        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, configStore);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        assertThatCode(() -> authManager.checkAuthReplicate(request)).doesNotThrowAnyException();
    }

    @Test
    public void testReplicateMismatch()
    {
        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, configStore);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("10.20.30.40");
        assertThatExceptionOfType(ForbiddenException.class)
                .isThrownBy(() -> authManager.checkAuthReplicate(request))
                .withMessageContaining("HTTP 403")
                .withNoCause();
    }

    @Test
    public void testReplicateInvalidIp()
    {
        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, configStore);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("10.10.10.10.10");
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

        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, configStore);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("10.20.30.40");
        assertThatCode(() -> authManager.checkAuthDelete(nodeId, request)).doesNotThrowAnyException();
        assertThatCode(() -> authManager.checkAuthDelete(Id.random(), request)).doesNotThrowAnyException();
    }

    @Test
    public void testDeleteMismatch()
    {
        Id<Node> nodeId = Id.random();
        DynamicAnnouncement toDelete = DynamicAnnouncement.copyOf(new DynamicAnnouncement("testing", "general", "/location", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:4111"))
        ))).setAnnouncer("10.20.30.40").build();
        dynamicStore.put(nodeId, toDelete);

        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, configStore);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        assertThatExceptionOfType(ForbiddenException.class)
                .isThrownBy(() -> authManager.checkAuthDelete(nodeId, request))
                .withMessageContaining("HTTP 403")
                .withNoCause();
    }

    @Test
    public void testDeleteInvalidIp()
    {
        Id<Node> nodeId = Id.random();
        DynamicAnnouncement toDelete = DynamicAnnouncement.copyOf(new DynamicAnnouncement("testing", "general", "/location", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:4111"))
        ))).setAnnouncer("10.20.30.40").build();
        dynamicStore.put(nodeId, toDelete);

        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, configStore);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("10.10.10.10.10");
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
        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, configStore);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        //Announce new
        assertThatCode(() -> authManager.checkAuthAnnounce(nodeId, localhostAnnouncement, request)).doesNotThrowAnyException();
        dynamicStore.put(nodeId, DynamicAnnouncement.copyOf(localhostAnnouncement).setAnnouncer("127.0.0.1").build());
        //Re-announce
        assertThatCode(() -> authManager.checkAuthAnnounce(nodeId, localhostAnnouncement, request)).doesNotThrowAnyException();
    }

    @Test
    public void testAnnounceMismatch()
    {
        Id<Node> nodeId = Id.random();
        DynamicServiceAnnouncement localhostServiceAnnouncement = new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:4111"));
        DynamicAnnouncement localhostAnnouncement = new DynamicAnnouncement("testing", "general", "/location", ImmutableSet.of(localhostServiceAnnouncement));
        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, configStore);
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
        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, configStore);
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
        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, configStore);
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
    public void testAnnounceInvalidIp()
    {
        Id<Node> nodeId = Id.random();
        DynamicServiceAnnouncement localhostServiceAnnouncement = new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:4111"));
        DynamicAnnouncement localhostAnnouncement = new DynamicAnnouncement("testing", "general", "/location", ImmutableSet.of(localhostServiceAnnouncement));
        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, configStore);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn("10.10.10.10.10");
        assertThatExceptionOfType(ForbiddenException.class)
                .isThrownBy(() -> authManager.checkAuthAnnounce(nodeId, localhostAnnouncement, request))
                .withMessageContaining("HTTP 403")
                .withNoCause();
    }

    @Test
    public void testAnnounceInvalidProperty()
    {
        AuthManager authManager = new IpHostnameAuthManager(dynamicStore, configStore);
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

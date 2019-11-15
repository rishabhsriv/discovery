package com.proofpoint.discovery;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.proofpoint.discovery.DiscoveryConfig.StringSet;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.http.client.Request;
import com.proofpoint.http.client.Response;
import com.proofpoint.http.client.testing.TestingHttpClient;
import com.proofpoint.http.client.testing.TestingHttpClient.Processor;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.net.ConnectException;
import java.net.URI;
import java.util.Set;

import static com.proofpoint.discovery.Services.services;
import static com.proofpoint.http.client.testing.TestingResponse.mockResponse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

public class TestProxyStore
{
    @Test
    public void testNoProxy()
    {
        Injector injector = mock(Injector.class);
        ProxyStore proxyStore = new ProxyStore(new DiscoveryConfig(), injector);
        Set<Service> services = ImmutableSet.of(new Service(Id.random(), Id.random(), "type", "pool", "/location", ImmutableMap.of("key", "value")));

        assertThat(proxyStore.filterAndGetAll(services)).isEqualTo(services);
        assertThat(proxyStore.get("foo")).isNull();
        assertThat(proxyStore.get("foo", "bar")).isNull();
        verifyNoMoreInteractions(injector);
    }

    @Test
    public void testProxy()
            throws InterruptedException
    {
        Service service1 = new Service(Id.random(), Id.random(), "storage", "pool1", "/location/1", ImmutableMap.of("key", "value"));
        Service service2 = new Service(Id.random(), Id.random(), "storage", "pool2", "/location/2", ImmutableMap.of("key2", "value2"));
        Service service3 = new Service(Id.random(), Id.random(), "customer", "general", "/location/3", ImmutableMap.of("key3", "value3"));

        DiscoveryConfig config = new DiscoveryConfig()
                .setProxyProxiedTypes(StringSet.of("storage", "customer", "auth"))
                .setProxyEnvironment("upstream")
                .setProxyUris(DiscoveryConfig.UriSet.of(URI.create("http://discovery.example.com")));
        Injector injector = mock(Injector.class);
        HttpClient httpClient = new TestingHttpClient(new DiscoveryProcessor(config, new Service[]{service1, service2, service3}));
        when(injector.getInstance(Key.get(HttpClient.class, ForProxyStore.class))).thenReturn(httpClient);
        ProxyStore proxyStore = new ProxyStore(config, injector);
        Thread.sleep(100);

        Service service4 = new Service(Id.random(), Id.random(), "storage", "pool1", "/location/4", ImmutableMap.of("key4", "value4"));
        Service service5 = new Service(Id.random(), Id.random(), "auth", "pool3", "/location/5", ImmutableMap.of("key5", "value5"));
        Service service6 = new Service(Id.random(), Id.random(), "event", "general", "/location/6", ImmutableMap.of("key6", "value6"));

        assertThat(proxyStore.filterAndGetAll(ImmutableSet.of(service4, service5, service6)))
                .containsExactlyInAnyOrder(service1, service2, service3, service6);

        assertThat(proxyStore.get("storage")).containsExactlyInAnyOrder(service1, service2);
        assertThat(proxyStore.get("customer")).containsExactly(service3);
        assertThat(proxyStore.get("auth")).isEmpty();
        assertThat(proxyStore.get("event")).isNull();

        assertThat(proxyStore.get("storage", "pool1")).containsExactly(service1);
        assertThat(proxyStore.get("storage", "pool2")).containsExactly(service2);
        assertThat(proxyStore.get("customer", "general")).containsExactly(service3);
        assertThat(proxyStore.get("customer", "pool3")).isEmpty();
        assertThat(proxyStore.get("auth", "pool3")).isEmpty();
        assertThat(proxyStore.get("event", "general")).isNull();
    }

    @Test
    public void testProxyStatic()
            throws InterruptedException
    {
        Service service1 = new Service(Id.random(), null, "storage", "pool1", "/location/1", ImmutableMap.of("key", "value"));
        Service service2 = new Service(Id.random(), null, "storage", "pool2", "/location/2", ImmutableMap.of("key2", "value2"));
        Service service3 = new Service(Id.random(), null, "customer", "general", "/location/3", ImmutableMap.of("key3", "value3"));

        DiscoveryConfig config = new DiscoveryConfig()
                .setProxyProxiedTypes(StringSet.of("storage", "customer", "auth"))
                .setProxyEnvironment("upstream")
                .setProxyUris(DiscoveryConfig.UriSet.of(URI.create("http://discovery.example.com")));
        Injector injector = mock(Injector.class);
        HttpClient httpClient = new TestingHttpClient(new DiscoveryProcessor(config, new Service[]{service1, service2, service3}));
        when(injector.getInstance(Key.get(HttpClient.class, ForProxyStore.class))).thenReturn(httpClient);
        ProxyStore proxyStore = new ProxyStore(config, injector);
        Thread.sleep(100);

        Service service4 = new Service(Id.random(), null, "storage", "pool1", "/location/4", ImmutableMap.of("key4", "value4"));
        Service service5 = new Service(Id.random(), null, "auth", "pool3", "/location/5", ImmutableMap.of("key5", "value5"));
        Service service6 = new Service(Id.random(), null, "event", "general", "/location/6", ImmutableMap.of("key6", "value6"));

        assertThat(proxyStore.filterAndGetAll(ImmutableSet.of(service4, service5, service6)))
                .containsExactlyInAnyOrder(service1, service2, service3, service6);

        assertThat(proxyStore.get("storage")).containsExactlyInAnyOrder(service1, service2);
        assertThat(proxyStore.get("customer")).containsExactly(service3);
        assertThat(proxyStore.get("auth")).isEmpty();
        assertThat(proxyStore.get("event")).isNull();

        assertThat(proxyStore.get("storage", "pool1")).containsExactly(service1);
        assertThat(proxyStore.get("storage", "pool2")).containsExactly(service2);
        assertThat(proxyStore.get("customer", "general")).containsExactly(service3);
        assertThat(proxyStore.get("customer", "pool3")).isEmpty();
        assertThat(proxyStore.get("auth", "pool3")).isEmpty();
        assertThat(proxyStore.get("event", "general")).isNull();
    }

    @Test
    public void testProxyDown()
            throws InterruptedException
    {
        DiscoveryConfig config = new DiscoveryConfig()
                .setProxyProxiedTypes(StringSet.of("storage"))
                .setProxyEnvironment("upstream")
                .setProxyUris(DiscoveryConfig.UriSet.of(URI.create("http://discovery.example.com")));
        Injector injector = mock(Injector.class);
        HttpClient httpClient = new TestingHttpClient(request -> {throw new ConnectException();});
        when(injector.getInstance(Key.get(HttpClient.class, ForProxyStore.class))).thenReturn(httpClient);
        ProxyStore proxyStore = new ProxyStore(config, injector);
        Thread.sleep(100);

        Service service4 = new Service(Id.random(), Id.random(), "storage", "pool1", "/location/4", ImmutableMap.of("key4", "value4"));

        assertThat(proxyStore.filterAndGetAll(ImmutableSet.of(service4))).isEmpty();

        assertThat(proxyStore.get("storage")).isEmpty();
        assertThat(proxyStore.get("storage", "pool1")).isEmpty();
    }

    private static class DiscoveryProcessor
            implements TestingHttpClient.Processor
    {
        private final DiscoveryConfig config;
        private final ImmutableSet<Service> services;

        DiscoveryProcessor(DiscoveryConfig config, Service[] services)
        {
            this.config = config;
            this.services = ImmutableSet.copyOf(services);
        }

        @Nonnull
        @Override
        public Response handle(Request request)
        {
            assertThat(request.getMethod()).isEqualTo("GET");
            URI uri = request.getUri();
            assertTrue(uri.toString().startsWith("v1/service/"), "uri " + uri.toString() + " starts with expected prefix");
            String type = uri.toASCIIString().substring(11);
            if (type.endsWith("/")) {
                type = type.substring(0, type.length() - 1);
            }
            assertTrue(config.getProxyProxiedTypes().contains(type), "type " + type + " in configured proxy types");

            Builder<Service> builder = ImmutableSet.builder();
            for (Service service : services) {
                if (type.equals(service.getType())) {
                    builder.add(service);
                }
            }
            final Services filteredServices = services(config.getProxyEnvironment(), builder.build());

            return mockResponse()
                    .jsonBody(filteredServices)
                    .build();
        }
    }
}

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
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.Set;
import java.util.stream.Collectors;

import static com.proofpoint.discovery.Services.services;
import static com.proofpoint.http.client.testing.TestingResponse.mockResponse;
import static com.proofpoint.testing.Assertions.assertEqualsIgnoreOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestProxyStore
{
    @Test
    public void testNoProxy()
    {
        Injector injector = mock(Injector.class);
        ProxyStore proxyStore = new ProxyStore(new DiscoveryConfig(), injector);
        Set<Service> services = ImmutableSet.of(new Service(Id.random(), Id.random(), "type", "pool", "/location", ImmutableMap.of("key", "value")));

        assertEquals(proxyStore.filterAndGetAll(services), services);
        assertEquals(proxyStore.get("foo"), null);
        assertEquals(proxyStore.get("foo", "bar"), null);
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

        assertEqualsIgnoreOrder(proxyStore.filterAndGetAll(ImmutableSet.of(service4, service5, service6)),
                ImmutableSet.of(service1, service2, service3, service6));

        assertEqualsIgnoreOrder(proxyStore.get("storage").collect(Collectors.toList()), ImmutableSet.of(service1, service2));
        assertEqualsIgnoreOrder(proxyStore.get("customer").collect(Collectors.toList()), ImmutableSet.of(service3));
        assertEqualsIgnoreOrder(proxyStore.get("auth").collect(Collectors.toList()), ImmutableSet.<Service>of());
        assertNull(proxyStore.get("event"));

        assertEquals(proxyStore.get("storage", "pool1").collect(Collectors.toList()), ImmutableSet.of(service1));
        assertEquals(proxyStore.get("storage", "pool2").collect(Collectors.toList()), ImmutableSet.of(service2));
        assertEquals(proxyStore.get("customer", "general").collect(Collectors.toList()), ImmutableSet.of(service3));
        assertEquals(proxyStore.get("customer", "pool3").collect(Collectors.toList()), ImmutableSet.<Service>of());
        assertEquals(proxyStore.get("auth", "pool3").collect(Collectors.toList()), ImmutableSet.<Service>of());
        assertNull(proxyStore.get("event", "general"));
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

        assertEqualsIgnoreOrder(proxyStore.filterAndGetAll(ImmutableSet.of(service4, service5, service6)),
                ImmutableSet.of(service1, service2, service3, service6));

        assertEqualsIgnoreOrder(proxyStore.get("storage").collect(Collectors.toList()), ImmutableSet.of(service1, service2));
        assertEqualsIgnoreOrder(proxyStore.get("customer").collect(Collectors.toList()), ImmutableSet.of(service3));
        assertEqualsIgnoreOrder(proxyStore.get("auth").collect(Collectors.toList()), ImmutableSet.<Service>of());
        assertNull(proxyStore.get("event"));

        assertEquals(proxyStore.get("storage", "pool1").collect(Collectors.toList()), ImmutableSet.of(service1));
        assertEquals(proxyStore.get("storage", "pool2").collect(Collectors.toList()), ImmutableSet.of(service2));
        assertEquals(proxyStore.get("customer", "general").collect(Collectors.toList()), ImmutableSet.of(service3));
        assertEquals(proxyStore.get("customer", "pool3").collect(Collectors.toList()), ImmutableSet.<Service>of());
        assertEquals(proxyStore.get("auth", "pool3").collect(Collectors.toList()), ImmutableSet.<Service>of());
        assertNull(proxyStore.get("event", "general"));
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
            assertEquals(request.getMethod(), "GET");
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

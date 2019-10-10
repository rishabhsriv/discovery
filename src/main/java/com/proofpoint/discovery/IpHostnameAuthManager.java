package com.proofpoint.discovery;

import com.proofpoint.discovery.store.Entry;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.ForbiddenException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class IpHostnameAuthManager implements AuthManager
{
    private final DynamicStore dynamicStore;
    private final Set<InetAddress> discoveryHosts;

    @Inject
    public IpHostnameAuthManager(DynamicStore dynamicStore, ConfigStore configStore)
    {
        this.dynamicStore = requireNonNull(dynamicStore, "dynamicStore is null");
        discoveryHosts = configStore.get("discovery")
                .map(s -> s.getProperties().get("http"))
                .filter(Objects::nonNull)
                .map(URI::create)
                .map(s -> {
                    try {
                        return InetAddress.getAllByName(s.getHost());
                    }
                    catch (UnknownHostException e) {
                        throw new IllegalArgumentException("Unable to build discovery IP list");
                    }
                })
                .flatMap(Arrays::stream)
                .collect(Collectors.toSet());
    }

    @Override
    public void checkAuthAnnounce(Id<Node> nodeId, DynamicAnnouncement announcement, HttpServletRequest request)
    {
        try {
            // InetAddress will check the remoteAddr IP for validity, but will not do a DNS lookup
            InetAddress requesterAddress = InetAddress.getByName(request.getRemoteAddr());
            Entry nodeEntry = dynamicStore.get(nodeId);
            if (nodeEntry != null && !requesterAddress.equals(InetAddress.getByName(nodeEntry.getAnnouncer()))) {
                //NodeId was previously announced by a different IP
                throw new ForbiddenException();
            }
            Set<DynamicServiceAnnouncement> newAnnouncements = announcement.getServiceAnnouncements();
            for (DynamicServiceAnnouncement newAnnouncement : newAnnouncements) {
                Map<String, String> properties = newAnnouncement.getProperties();
                if (invalidHostnameIp(properties.get("http"), requesterAddress) ||
                        invalidHostnameIp(properties.get("https"), requesterAddress) ||
                        invalidHostnameIp(properties.get("admin"), requesterAddress)) {
                    //Service is announcing a host with a different set of IPs
                    throw new ForbiddenException();
                }
            }
        }
        catch (UnknownHostException e) {
            //Unable to validate an IP or look up a hostname
            throw new ForbiddenException();
        }
    }

    @Override
    public void checkAuthDelete(Id<Node> nodeId, HttpServletRequest request)
    {
        try {
            InetAddress requesterAddress = InetAddress.getByName(request.getRemoteAddr());
            Entry nodeEntry = dynamicStore.get(nodeId);
            if (nodeEntry != null && !requesterAddress.equals(InetAddress.getByName(nodeEntry.getAnnouncer()))) {
                throw new ForbiddenException();
            }
        }
        catch (UnknownHostException e) {
            throw new ForbiddenException();
        }
    }

    @Override
    public void checkAuthRead(HttpServletRequest request)
    {
        //Anyone can read announced services.
    }

    @Override
    public void checkAuthReplicate(HttpServletRequest request)
    {
        try {
            InetAddress requesterAddress = InetAddress.getByName(request.getRemoteAddr());
            if (!discoveryHosts.contains(requesterAddress)) {
                throw new ForbiddenException();
            }
        }
        catch (UnknownHostException e) {
            throw new ForbiddenException();
        }
    }

    private boolean invalidHostnameIp(String hostname, InetAddress requesterAddress)
    {
        if (hostname == null) {
            return false;
        }
        try {
            URI hostUri = URI.create(hostname);
            InetAddress[] hostIps = InetAddress.getAllByName(hostUri.getHost());
            return !Arrays.asList(hostIps).contains(requesterAddress);
        }
        catch (UnknownHostException e) {
            return true;
        }
    }
}

package com.proofpoint.discovery;

import com.proofpoint.discovery.store.Entry;
import com.proofpoint.log.Logger;

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
    private static final Logger logger = Logger.get(IpHostnameAuthManager.class);

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
                logger.error("IP %s attempted to re-announce node %s owned by IP %s", requesterAddress.getHostAddress(), nodeId.toString(), nodeEntry.getAnnouncer());
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
            logger.error(e);
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
                logger.error("IP %s tried to delete node %s owned by IP %s", requesterAddress.getHostAddress(), nodeId.toString(), nodeEntry.getAnnouncer());
                throw new ForbiddenException();
            }
        }
        catch (UnknownHostException e) {
            logger.error(e);
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
                logger.error("IP %s tried to replicate as Discovery", requesterAddress.getHostAddress());
                throw new ForbiddenException();
            }
        }
        catch (UnknownHostException e) {
            logger.error(e);
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
            if (!Arrays.asList(hostIps).contains(requesterAddress)) {
                logger.error("IP %s tried to announce other host %s", requesterAddress.getHostAddress(), hostUri.getHost());
                return true;
            }
            return false;
        }
        catch (UnknownHostException e) {
            logger.error(e);
            return true;
        }
    }
}

package com.proofpoint.discovery;

import javax.servlet.http.HttpServletRequest;

/*
 * Implementation should throw a WebApplicationException with an appropriate response code on auth failure
 */
public interface AuthManager
{
    void checkAuthAnnounce(Id<Node> nodeId, DynamicAnnouncement announcement, HttpServletRequest request);

    void checkAuthDelete(Id<Node> nodeId, HttpServletRequest request);

    void checkAuthReplicate(HttpServletRequest request);
}

package com.proofpoint.discovery;

import javax.servlet.http.HttpServletRequest;

public interface AuthManager
{
    void checkAuthAnnounce(Id<Node> nodeId, DynamicAnnouncement announcement, HttpServletRequest request);

    void checkAuthDelete(Id<Node> nodeId, HttpServletRequest request);

    void checkAuthRead(HttpServletRequest request);

    void checkAuthReplicate(HttpServletRequest request);
}

package com.proofpoint.discovery;

import javax.servlet.http.HttpServletRequest;

public class AllowAllAuthManager implements AuthManager
{
    @Override
    public void checkAuthAnnounce(Id<Node> nodeId, DynamicAnnouncement announcement, HttpServletRequest request)
    {
    }

    @Override
    public void checkAuthDelete(Id<Node> nodeId, HttpServletRequest request)
    {
    }

    @Override
    public void checkAuthRead(HttpServletRequest request)
    {
    }

    @Override
    public void checkAuthReplicate(HttpServletRequest request)
    {
    }
}

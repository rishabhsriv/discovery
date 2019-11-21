package com.proofpoint.discovery;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;

@AutoValue
public abstract class AuthAuditRecord
{
    public static AuthAuditRecord auditRecord(String format, Object... args)
    {
        return new AutoValue_AuthAuditRecord(String.format(format, args));
    }

    @JsonProperty
    public abstract String getMessage();
}

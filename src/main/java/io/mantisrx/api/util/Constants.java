package io.mantisrx.api.util;

import lombok.experimental.UtilityClass;

@UtilityClass
public class Constants {
    public static final String numMessagesCounterName = "numSinkMessages";
    public static final String numDroppedMessagesCounterName = "numDroppedSinkMessages";
    public static final String numBytesCounterName = "numSinkBytes";
    public static final String numDroppedBytesCounterName = "numDroppedSinkBytes";

    public static final String TWO_NEWLINES = "\r\n\r\n";
    public static final String SSE_DATA_PREFIX = "data: ";

    public static final long TunnelPingIntervalSecs = 12;
    public static final String TunnelPingMessage = "MantisApiTunnelPing";
    public static final String TunnelPingParamName = "MantisApiTunnelPingEnabled";

    public static final String OriginRegionTagName = "originRegion";
    public static final String TagsParamName = "MantisApiTag";
    public static final String TagNameValDelimiter = ":";
}

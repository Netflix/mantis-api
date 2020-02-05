package io.mantisrx.api.filters;

import com.netflix.zuul.filters.http.HttpOutboundSyncFilter;
import com.netflix.zuul.message.http.HttpResponseMessage;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.util.AsciiString;

public class OutboundHeaders extends HttpOutboundSyncFilter {

    @Override
    public boolean shouldFilter(HttpResponseMessage msg) {
        return true;
    }

    @Override
    public HttpResponseMessage apply(HttpResponseMessage resp) {
        addHeaderIfMissing(resp, HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
        addHeaderIfMissing(resp, HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS,
                "Origin, X-Requested-With, Accept, Content-Type, Cache-Control");
        addHeaderIfMissing(resp, HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS,
                "GET, OPTIONS, PUT, POST, DELETE");
        return resp;
    }

    private void addHeaderIfMissing(HttpResponseMessage resp, AsciiString name, String value) {
        if (resp.getHeaders().get(name.toString()).size() == 0) {
            resp.getHeaders().add(name.toString(), value);
        }
    }

    @Override
    public int filterOrder() {
        return 0;
    }
}

package io.mantisrx.api.filters;

import com.netflix.zuul.filters.http.HttpOutboundSyncFilter;
import com.netflix.zuul.message.http.HttpRequestMessage;
import com.netflix.zuul.message.http.HttpResponseMessage;
import com.netflix.zuul.message.http.HttpResponseMessageImpl;
import com.netflix.zuul.stats.status.StatusCategoryUtils;
import com.netflix.zuul.stats.status.ZuulStatusCategory;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;

public class OutboundHeaders extends HttpOutboundSyncFilter {

    @Override
    public boolean shouldFilter(HttpResponseMessage msg) {
        return true;
    }

    @Override
    public HttpResponseMessage apply(HttpResponseMessage resp) {
        resp.getHeaders().add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN.toString(), "*");
        resp.getHeaders().add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS.toString(), "Origin, X-Requested-With, Accept, Content-Type, Cache-Control");
        resp.getHeaders().add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS.toString(),
                "GET, OPTIONS, PUT, POST, DELETE");
        StatusCategoryUtils.setStatusCategory(resp.getContext(), ZuulStatusCategory.SUCCESS);
        return resp;
    }

    @Override
    public int filterOrder() {
        return 0;
    }
}

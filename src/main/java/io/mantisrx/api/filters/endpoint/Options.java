package io.mantisrx.api.filters.endpoint;

import com.netflix.zuul.filters.http.HttpSyncEndpoint;
import com.netflix.zuul.message.http.HttpRequestMessage;
import com.netflix.zuul.message.http.HttpResponseMessage;
import com.netflix.zuul.message.http.HttpResponseMessageImpl;
import com.netflix.zuul.stats.status.StatusCategoryUtils;
import com.netflix.zuul.stats.status.ZuulStatusCategory;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;

public class Options extends HttpSyncEndpoint {

    @Override
    public HttpResponseMessage apply(HttpRequestMessage request) {
        HttpResponseMessage resp = new HttpResponseMessageImpl(request.getContext(), request, HttpResponseStatus.NO_CONTENT.code());
        resp.getHeaders().add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN.toString(), "*");
        resp.getHeaders().add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS.toString(), "Origin, X-Requested-With, Accept, Content-Type, Cache-Control");
        resp.getHeaders().add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS.toString(),
                "GET, OPTIONS, PUT, POST, DELETE");
        StatusCategoryUtils.setStatusCategory(request.getContext(), ZuulStatusCategory.SUCCESS);
        return resp;
    }
}

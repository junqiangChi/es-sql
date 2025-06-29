package com.cjq.plugin;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

public class EsPluginWebAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "es_plugin_page";
    }

    @Override
    public List<Route> routes() {
        return Collections.singletonList(new Route(RestRequest.Method.GET, "/es_sql_plugin"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        InputStream inputStream = getClass().getResourceAsStream("/sql-plugin.html");
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        int nRead;
        byte[] data = new byte[1024];
        while ((nRead = inputStream.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }

        buffer.flush();
        byte[] content = buffer.toByteArray();
        return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, "text/html", content));

    }
}

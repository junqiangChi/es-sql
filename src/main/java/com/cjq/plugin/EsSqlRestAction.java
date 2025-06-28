package com.cjq.plugin;

import com.cjq.action.ActionPlan;
import com.cjq.action.ActionPlanFactory;
import com.cjq.domain.EqlParserDriver;
import com.cjq.plan.logical.Delete;
import com.cjq.plan.logical.LogicalPlan;
import com.cjq.plan.logical.Query;
import org.apache.commons.lang3.StringEscapeUtils;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;

public class EsSqlRestAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "es_sql";
    }

    @Override
    public List<Route> routes() {
        return Collections.unmodifiableList(Arrays.asList(
                new Route(RestRequest.Method.POST, "/_es_sql"),
                new Route(RestRequest.Method.POST, "/_es_sql/explain"),
                new Route(RestRequest.Method.GET, "/_es_sql"),
                new Route(RestRequest.Method.GET, "/_es_sql/explain"))
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        try (XContentParser parser = request.contentOrSourceParamParser()) {
            parser.mapStrings().forEach((k, v) -> request.params().putIfAbsent(k, v));
        } catch (IOException e) {
            HashMap<String, String> resultMap = new HashMap<String, String>() {
                {
                    put("sql", "select * from test");
                }
            };
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, XContentType.JSON.mediaType(),
                    "Please use json format params, eg: " + formatJson(resultMap)));
        }
        String sql = request.param("sql");
        EqlParserDriver eqlParserDriver = new EqlParserDriver();
        LogicalPlan plan = eqlParserDriver.parser(sql);
        ActionPlanFactory actionPlanFactory = ActionPlanFactory.getInstance();
        ActionPlan action = actionPlanFactory.createAction(plan);
        ActionRequest actionRequest = action.explain();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().prettyPrint();
        if (request.path().endsWith("/explain")) {
            return explain(actionRequest, plan, xContentBuilder);
        }
        return execute(client, actionRequest, plan, xContentBuilder);
    }

    private RestChannelConsumer explain(ActionRequest request, LogicalPlan plan, XContentBuilder xContentBuilder) {
        if (plan instanceof Query) {
            SearchSourceBuilder source = ((SearchRequest) request).source();
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, xContentBuilder.value(source)));
        } else {
            HashMap<String, String> resultMap = new HashMap<String, String>() {
                {
                    put("error", "Only support query syntax to explain");
                }
            };
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, XContentType.JSON.mediaType(),
                    formatJson(resultMap)));
        }
    }

    private RestChannelConsumer execute(NodeClient client, ActionRequest request, LogicalPlan plan, XContentBuilder xContentBuilder) {
        if (plan instanceof Query) {
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, xContentBuilder.value(client.search((SearchRequest) request).actionGet())));
        } else if (plan instanceof Delete) {
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, xContentBuilder.value(client.delete((DeleteRequest) request).actionGet())));
        }
        HashMap<String, String> resultMap = new HashMap<String, String>() {
            {
                put("error", "Not support the logicalPlan: " + plan.getClass());
            }
        };
        return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, XContentType.JSON.mediaType(), formatJson(resultMap)));
    }

    private String formatJson(Map<String, ?> map) {
        StringBuilder json = new StringBuilder("{\n");
        boolean first = true;
        for (Map.Entry<String, ?> entry : map.entrySet()) {
            if (!first) {
                json.append(",\n");
            }
            json.append("  \"").append(entry.getKey()).append("\": ")
                    .append("\"").append(entry.getValue()).append("\"");
            first = false;
        }
        return json.toString();
    }

}
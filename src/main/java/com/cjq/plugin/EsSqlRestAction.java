package com.cjq.plugin;

import com.cjq.action.ActionPlan;
import com.cjq.action.ActionPlanFactory;
import com.cjq.domain.EqlParserDriver;
import com.cjq.handler.HandlerFactory;
import com.cjq.handler.ResponseHandler;
import com.cjq.jdbc.ObjectResult;
import com.cjq.plan.logical.*;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
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
                new Route(RestRequest.Method.GET, "/_es_sql/explain"),
                new Route(RestRequest.Method.GET, "/sql_plugin"),
                new Route(RestRequest.Method.POST, "/web_sql_query"))
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
        System.out.println("path: " + request.path());
        String sql = request.param("sql");
        EqlParserDriver eqlParserDriver = new EqlParserDriver();
        LogicalPlan plan = eqlParserDriver.parser(sql);
        ActionPlanFactory actionPlanFactory = ActionPlanFactory.getInstance();
        ActionPlan action = actionPlanFactory.createAction(null, plan);
        ActionRequest actionRequest = action.explain();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().prettyPrint();
        if (request.path().endsWith("/explain")) {
            return explain(actionRequest, plan, xContentBuilder);
        } else if (request.path().endsWith("web_sql_query")) {
            return webExecute(client, actionRequest, plan);
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
        } else if (plan instanceof Show) {
            GetIndexResponse getIndexResponse = client.admin().indices().getIndex((GetIndexRequest) request).actionGet();
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, xContentBuilder.value(getIndexResponse)));
        } else if (plan instanceof Drop) {
            AcknowledgedResponse acknowledgedResponse = client.admin().indices().delete((DeleteIndexRequest) request).actionGet();
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, xContentBuilder.value(acknowledgedResponse)));
        }
        HashMap<String, String> resultMap = new HashMap<String, String>() {
            {
                put("error", "Not support the logicalPlan: " + plan.getClass());
            }
        };
        return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, XContentType.JSON.mediaType(), formatJson(resultMap)));
    }

    private RestChannelConsumer webExecute(NodeClient client, ActionRequest request, LogicalPlan plan) {
        if (request instanceof SearchRequest || request instanceof GetIndexRequest) {
            HandlerFactory handlerFactory = HandlerFactory.getInstance();
            ResponseHandler handler = handlerFactory.createHandler(plan, new Properties());
            ObjectResult objectResult;
            if (request instanceof SearchRequest) {
                SearchResponse response = client.search((SearchRequest) request).actionGet();
                objectResult = handler.handle(response);
                return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, XContentType.JSON.mediaType(),
                        objectResult.toJsonString()));
            } else {
                GetIndexResponse getIndexResponse = client.admin().indices().getIndex((GetIndexRequest) request).actionGet();
                objectResult = handler.handle(getIndexResponse);
            }
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, XContentType.JSON.mediaType(),
                    objectResult.toJsonString()));
        }
        throw new IllegalArgumentException("Only support query sql");
    }


    private String formatJson(Map<String, ?> map) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        for (Map.Entry<String, ?> entry : map.entrySet()) {
            builder.field(entry.getKey());
            builder.value(entry.getValue());
        }
        builder.endObject();
        BytesReference bytesReference = BytesReference.bytes(builder);
        return bytesReference.utf8ToString();
    }
}
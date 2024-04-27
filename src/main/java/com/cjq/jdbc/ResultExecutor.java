package com.cjq.jdbc;

import com.cjq.common.Constant;
import com.cjq.common.EsJdbcConfig;
import com.cjq.domain.Client;
import com.cjq.domain.HandleRequest;
import com.cjq.exception.EsSqlParseException;
import com.cjq.plan.logical.LogicalPlan;
import com.cjq.plan.logical.Query;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ResultExecutor {
    private Client client;
    private LogicalPlan plan;
    private Properties properties;
    private boolean isIncludeIndex;
    private boolean isIncludeDocID;
    private boolean isIncludeType;
    private boolean isIncludeVersion;

    public ResultExecutor(Client client, LogicalPlan plan, Properties properties) {
        this.client = client;
        this.plan = plan;
        this.properties = properties;
        setDocInclude();
    }

    private void setDocInclude() {
        this.isIncludeIndex = Boolean.parseBoolean(properties.getProperty(EsJdbcConfig.INCLUDE_INDEX));
        this.isIncludeDocID = Boolean.parseBoolean(properties.getProperty(EsJdbcConfig.INCLUDE_DOC_ID));
        this.isIncludeType = Boolean.parseBoolean(properties.getProperty(EsJdbcConfig.INCLUDE_TYPE));
        this.isIncludeVersion = Boolean.parseBoolean(properties.getProperty(EsJdbcConfig.INCLUDE_VERSION));
    }

    public ObjectResult getObjectResultSet() throws IOException {
        if (plan instanceof Query) {
            Query query = (Query) plan;
            RestHighLevelClient restHighLevelClient = client.getClient();
            return getObjectResult(query, restHighLevelClient);
        }
        throw new EsSqlParseException("This sql is not query type: " + plan.toString());
    }

    private ObjectResult getObjectResult(Query query, RestHighLevelClient restHighLevelClient) throws IOException {
        HandleRequest handleRequest = new HandleRequest();
        SearchResponse searchResponse = handleRequest.search(query, restHighLevelClient);
        SearchHit[] hits = searchResponse.getHits().getHits();
        List<String> headers = new ArrayList<>();
        List<List<Object>> rows = new ArrayList<>();
        setResult(hits, headers, rows);
        return new ObjectResult(headers, rows);
    }

    private void setResult(SearchHit[] hits, List<String> headers, List<List<Object>> rows) {
        for (SearchHit hit : hits) {
            Map<String, Object> doc = hit.getSourceAsMap();
            if (isIncludeIndex) {
                doc.put(Constant._INDEX, hit.getIndex());
            }
            if (isIncludeDocID) {
                doc.put(Constant._ID, hit.getId());
            }
            if (isIncludeType) {
                doc.put(Constant._TYPE, hit.getType());
            }
            if (isIncludeVersion) {
                doc.put(Constant._VERSION, hit.getVersion());
            }
            headers.addAll(doc.keySet());
            rows.add(new ArrayList<>(doc.values()));
        }
    }

}

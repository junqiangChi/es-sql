package com.cjq.jdbc;

import com.cjq.action.BaseAction;
import com.cjq.action.DefaultQueryAction;
import com.cjq.common.Constant;
import com.cjq.common.ElasticsearchJdbcConfig;
import com.cjq.domain.Client;
import com.cjq.exception.EsSqlParseException;
import com.cjq.plan.logical.Field;
import com.cjq.plan.logical.LogicalPlan;
import com.cjq.plan.logical.Query;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class HandleResult {
    private final Client client;
    private final LogicalPlan plan;
    private final Properties properties;
    private boolean isIncludeIndex;
    private boolean isIncludeDocID;
    private boolean isIncludeType;
    private boolean isIncludeScore;

    public HandleResult(Client client, LogicalPlan plan, Properties properties) {
        this.client = client;
        this.plan = plan;
        this.properties = properties;
        setDocInclude();
    }

    private void setDocInclude() {
        this.isIncludeIndex = Boolean.parseBoolean(properties.getProperty(ElasticsearchJdbcConfig.INCLUDE_INDEX.getName(),
                ElasticsearchJdbcConfig.INCLUDE_INDEX.getDefaultValue()));
        this.isIncludeDocID = Boolean.parseBoolean(properties.getProperty(ElasticsearchJdbcConfig.INCLUDE_DOC_ID.getName(),
                ElasticsearchJdbcConfig.INCLUDE_DOC_ID.getDefaultValue()));
        this.isIncludeType = Boolean.parseBoolean(properties.getProperty(ElasticsearchJdbcConfig.INCLUDE_TYPE.getName(),
                ElasticsearchJdbcConfig.INCLUDE_TYPE.getDefaultValue()));
        this.isIncludeScore = Boolean.parseBoolean(properties.getProperty(ElasticsearchJdbcConfig.INCLUDE_SCORE.getName(),
                ElasticsearchJdbcConfig.INCLUDE_SCORE.getDefaultValue()));
    }

    public ObjectResult getObjectResultSet() throws IOException {
        BaseAction action = getAction();
        ActionRequest actionRequest = action.buildRequest(this.plan);
        return getObjectResult(actionRequest, this.client.getClient());
    }

    private BaseAction getAction() {
        if (plan instanceof Query) {
            return new DefaultQueryAction();
        }
        throw new EsSqlParseException("Unsupported sql type!");
    }

    private ObjectResult getObjectResult(ActionRequest request, RestHighLevelClient restHighLevelClient) throws IOException {
        if (plan instanceof Query) {
            Query query = (Query) plan;
            SearchResponse searchResponse = restHighLevelClient.search((SearchRequest) request, RequestOptions.DEFAULT);
            return handleSearchResponse(query, searchResponse);
        }
        throw new EsSqlParseException("Unsupported sql type!");
    }

    private ObjectResult handleSearchResponse(Query query, SearchResponse searchResponse) {
        SearchHit[] hits = searchResponse.getHits().getHits();
        List<Field> fields = query.getSelect().getFields();
        boolean flat = fields.size() == 1 && fields.get(0).getField().equals("*");
        List<Map<String, Object>> docsAsMap = new ArrayList<>();
        Set<String> hitFieldNames = new HashSet<>();
        List<String> header = createHeadersAndFillDocsMap(query, flat, hits, docsAsMap, hitFieldNames);
        Map<String, String> fieldNameMap = new HashMap<>();
        if (fields.size() > 1 || !fields.get(0).getField().equals("*")) {
            for (Field field : fields) {
                if (!field.isConstant()) {
                    fieldNameMap.put(field.getField(), field.getAlias());
                }
            }
        }
        List<Field> constantFields = fields.stream().filter(Field::isConstant).collect(Collectors.toList());
        List<List<Object>> rows = createLinesFromDocs(flat, docsAsMap, header, hitFieldNames);
        header = header.stream().map(f -> StringUtils.isNotBlank(fieldNameMap.get(f)) ? fieldNameMap.get(f) : f)
                .collect(Collectors.toList());
        setConstantField(constantFields, header, rows);
        return new ObjectResult(header, rows);
    }


    private List<String> createHeadersAndFillDocsMap(Query query, boolean flat, SearchHit[] hits,
                                                     List<Map<String, Object>> docsAsMap, Set<String> hitFieldNames) {
        Set<String> headers = new LinkedHashSet<>();
        List<String> fieldNames = query.getSelect().getFields().stream().map(Field::getField).collect(Collectors.toList());
        String indexAlias = query.getFrom().getAlias();
        for (SearchHit hit : hits) {
            Map<String, Object> doc = hit.getSourceAsMap() != null ? hit.getSourceAsMap() : new HashMap<>();
            if (isIncludeScore) {
                doc.put(Constant._SCORE, hit.getScore());
            }
            if (isIncludeType) {
                doc.put(Constant._TYPE, hit.getType());
            }
            if (isIncludeDocID) {
                doc.put(Constant._ID, hit.getId());
            }
            if (isIncludeIndex) {
                if (StringUtils.isNotBlank(indexAlias)) {
                    doc.put(Constant._INDEX, indexAlias);
                } else {
                    doc.put(Constant._INDEX, hit.getIndex());
                }
            }
            mergeHeaders(headers, doc, flat);

            Map<String, DocumentField> fields = hit.getFields();
            for (DocumentField searchHitField : fields.values()) {
                List<Object> values = Optional.ofNullable(searchHitField.getValues()).orElse(Collections.emptyList());
                int size = values.size();
                doc.put(searchHitField.getName(), size == 1 ? values.get(0) : size > 1 ? values : null);
                hitFieldNames.add(searchHitField.getName());
                headers.add(searchHitField.getName());
            }

            docsAsMap.add(doc);
        }
        List<String> list = new ArrayList<>(headers);
        if (!fieldNames.isEmpty()) {
            list.sort((o1, o2) -> {
                int i1 = fieldNames.indexOf(o1);
                int i2 = fieldNames.indexOf(o2);
                return Integer.compare(i1 < 0 ? Integer.MAX_VALUE : i1, i2 < 0 ? Integer.MAX_VALUE : i2);
            });
        }
        return list;
    }


    private List<List<Object>> createLinesFromDocs(boolean flat, List<Map<String, Object>> docsAsMap,
                                                   List<String> headers, Set<String> hitFieldNames) {
        List<List<Object>> objectLines = new ArrayList<>();
        for (Map<String, Object> doc : docsAsMap) {
            List<Object> lines = new ArrayList<>();
            for (String header : headers) {
                lines.add(findFieldValue(header, doc, flat, hitFieldNames));
            }
            objectLines.add(lines);
        }
        return objectLines;
    }

    private void mergeHeaders(Set<String> headers, Map<String, Object> doc, boolean flat) {
        if (!flat) {
            headers.addAll(doc.keySet());
            return;
        }
        mergeFieldNamesRecursive(headers, doc, "");
    }

    private Object findFieldValue(String header, Map<String, Object> doc, boolean flat, Set<String> hitFieldNames) {
        if (flat && header.contains(".") && !hitFieldNames.contains(header)) {
            String[] split = header.split("\\.");
            Object innerDoc = doc;
            for (String innerField : split) {
                if (!(innerDoc instanceof Map)) {
                    return null;
                }
                innerDoc = ((Map<String, Object>) innerDoc).get(innerField);
                if (innerDoc == null) {
                    return null;
                }

            }
            return innerDoc;
        } else {
            if (doc.containsKey(header)) {
                return doc.get(header);
            }
        }
        return null;
    }

    private void mergeFieldNamesRecursive(Set<String> headers, Map<String, Object> doc, String prefix) {
        for (Map.Entry<String, Object> field : doc.entrySet()) {
            Object value = field.getValue();
            if (value instanceof Map) {
                mergeFieldNamesRecursive(headers, (Map<String, Object>) value, prefix + field.getKey() + ".");
            } else {
                headers.add(prefix + field.getKey());
            }
        }
    }

    private void setConstantField(List<Field> constantFields, List<String> headers, List<List<Object>> values) {
        if (constantFields != null) {
            for (Field constantField : constantFields) {
                headers.add(constantField.getAlias());
                for (List<Object> value : values) {
                    value.add(constantField.getConstantValue());
                }
            }
        }
    }
}

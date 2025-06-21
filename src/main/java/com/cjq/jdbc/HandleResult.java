package com.cjq.jdbc;

import com.cjq.action.AggQueryActionPlan;
import com.cjq.action.DefaultQueryActionPlan;
import com.cjq.common.Constant;
import com.cjq.common.ElasticsearchJdbcConfig;
import com.cjq.common.FieldFunction;
import com.cjq.domain.Client;
import com.cjq.exception.EsSqlParseException;
import com.cjq.plan.logical.Field;
import com.cjq.plan.logical.LogicalPlan;
import com.cjq.plan.logical.Query;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.cjq.common.Constant.GROUP_BY_PREFIX;

public class HandleResult {
    private static final Logger LOG = LoggerFactory.getLogger(HandleResult.class);
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
        this.isIncludeIndex = Boolean.parseBoolean(properties.getProperty(ElasticsearchJdbcConfig.INCLUDE_INDEX.getName(), ElasticsearchJdbcConfig.INCLUDE_INDEX.getDefaultValue()));
        this.isIncludeDocID = Boolean.parseBoolean(properties.getProperty(ElasticsearchJdbcConfig.INCLUDE_DOC_ID.getName(), ElasticsearchJdbcConfig.INCLUDE_DOC_ID.getDefaultValue()));
        this.isIncludeType = Boolean.parseBoolean(properties.getProperty(ElasticsearchJdbcConfig.INCLUDE_TYPE.getName(), ElasticsearchJdbcConfig.INCLUDE_TYPE.getDefaultValue()));
        this.isIncludeScore = Boolean.parseBoolean(properties.getProperty(ElasticsearchJdbcConfig.INCLUDE_SCORE.getName(), ElasticsearchJdbcConfig.INCLUDE_SCORE.getDefaultValue()));
    }

    public ObjectResult getObjectResultSet() throws IOException {
        DefaultQueryActionPlan action = getAction();
        ActionRequest actionRequest = action.explain();
        LOG.debug("request: {}", actionRequest);
        return getObjectResultSet(actionRequest);
    }

    private ObjectResult getObjectResultSet(ActionRequest request) throws IOException {
        ActionResponse actionResponse;
        if (plan instanceof Query) {
            actionResponse = this.client.getClient().search((SearchRequest) request, RequestOptions.DEFAULT);
        } else {
            throw new EsSqlParseException("Unsupported sql type!");
        }
        LOG.debug("response: {}", actionResponse);
        return extractResults(actionResponse);
    }

    private DefaultQueryActionPlan getAction() {
        if (plan instanceof Query) {
            Query query = (Query) plan;
            if (query.isAgg()) {
                return new AggQueryActionPlan(plan);
            } else {
                return new DefaultQueryActionPlan(plan);
            }
        }
        throw new EsSqlParseException("Unsupported sql type!");
    }


    private ObjectResult extractResults(ActionResponse actionResponse) {
        if (plan instanceof Query) {
            return handleSearchResponse((Query) plan, (SearchResponse) actionResponse);
        }
        throw new EsSqlParseException("Unsupported SQL type with " + plan.getClass());
    }

    private void handleNoGroupByAggResponse(List<Field> fields, Aggregations aggregations, List<String> header, List<List<Object>> lines) {
        aggregations.forEach(a -> header.add(a.getName()));
        ArrayList<Object> line = new ArrayList<>();
        for (Field field : fields) {
            FieldFunction funcName = field.getFuncName();
            String funcFieldName = field.getAlias() != null ? field.getAlias() : field.getFuncName() + "(" + field.getFieldName() + ")";

            switch (funcName) {
                case COUNT:
                    Cardinality countAgg = aggregations.get(funcFieldName);
                    line.add(countAgg.getValue());
                    break;
                case SUM:
                    Sum sumAgg = aggregations.get(funcFieldName);
                    line.add(sumAgg.getValue());
                    break;
                case AVG:
                    Avg avgAgg = aggregations.get(funcFieldName);
                    line.add(avgAgg.getValue());
                    break;
                case MAX:
                    Max maxAgg = aggregations.get(funcFieldName);
                    line.add(maxAgg.getValue());
                    break;
                case MIN:
                    Min minAgg = aggregations.get(funcFieldName);
                    line.add(minAgg.getValue());
                    break;
            }
        }
        lines.add(line);
    }

    private void setValueToMap(Map<String, List<Object>> groupValues, String fieldName, Object value) {
        if (groupValues.get(fieldName) == null) {
            ArrayList<Object> values = new ArrayList<>();
            values.add(value);
            groupValues.put(fieldName, values);
        } else {
            groupValues.get(fieldName).add(value);
        }
    }

    private void handlerTermsBuckets(Terms terms, List<Field> groupByFields, int level, Query query, Map<String, List<Object>> groupValues) {
        for (Terms.Bucket bucket : terms.getBuckets()) {
            Field field = groupByFields.get(level);
            setValueToMap(groupValues, field.getFieldName(), bucket.getKeyAsString());
            // 还有下一级
            if (level < groupByFields.size() - 1) {
                String nextAggName = GROUP_BY_PREFIX + groupByFields.get(level + 1).getFieldName();
                Terms nextTerms = bucket.getAggregations().get(nextAggName);
                handlerTermsBuckets(nextTerms, groupByFields, level + 1, query, groupValues);
            } else {
                // 最后一级分组
                List<Field> funcFields = query.getSelect().getFields().stream().filter(Field::isFunction).collect(Collectors.toList());
                for (Field funcField : funcFields) {
                    String funcFieldName = funcField.getAlias() != null ? funcField.getAlias() : funcField.getFuncName() + "(" + funcField.getFieldName() + ")";
                    switch (funcField.getFuncName()) {
                        case COUNT:
                            Cardinality countAgg = bucket.getAggregations().get(funcFieldName);
                            setValueToMap(groupValues, funcFieldName, countAgg.getValue());
                            break;
                        case SUM:
                            Sum sumAgg = bucket.getAggregations().get(funcFieldName);
                            setValueToMap(groupValues, funcFieldName, sumAgg.getValue());
                            break;
                        case AVG:
                            Avg avgAgg = bucket.getAggregations().get(funcFieldName);
                            setValueToMap(groupValues, funcFieldName, avgAgg.getValue());
                            break;
                        case MAX:
                            Max maxAgg = bucket.getAggregations().get(funcFieldName);
                            setValueToMap(groupValues, funcFieldName, maxAgg.getValue());
                            break;
                        case MIN:
                            Min minAgg = bucket.getAggregations().get(funcFieldName);
                            setValueToMap(groupValues, funcFieldName, minAgg.getValue());
                            break;
                    }
                }
            }
        }
    }

    private List<List<Object>> convertMapToList(Map<String, List<Object>> map) {
        List<String> keys = new ArrayList<>(map.keySet());
        if (keys.isEmpty()) {
            return new ArrayList<>();
        }
        int size = map.get(keys.get(0)).size();
        List<List<Object>> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            List<Object> subList = new ArrayList<>();
            for (String key : keys) {
                subList.add(map.get(key).get(i));
            }
            result.add(subList);
        }
        return result;
    }

    private ObjectResult handleSearchResponse(Query query, SearchResponse searchResponse) {
        if (query.isAgg()) {
            List<Field> fields = query.getSelect().getFields();
            Aggregations aggregations = searchResponse.getAggregations();

            if (query.getGroupBy() != null) {
                List<Field> groupByFields = query.getGroupBy().getGroupByFields();
                Terms rootTerms = searchResponse.getAggregations().get(GROUP_BY_PREFIX + groupByFields.get(0).getFieldName());
                HashMap<String, List<Object>> groupValues = new HashMap<>();
                handlerTermsBuckets(rootTerms, groupByFields, 0, query, groupValues);
                List<String> header = query.getSelect().getFields().stream().map(field -> field.isFunction() ? field.getAlias() != null ? field.getAlias() : field.getFuncName() + "(" + field.getFieldName() + ")" : field.getFieldName()).collect(Collectors.toList());
                List<List<Object>> lines = new ArrayList<>();
                if (!groupValues.isEmpty()) {
                    lines = convertMapToList(groupValues);
                }
                return new ObjectResult(header, lines);
            } else {
                List<String> header = new ArrayList<>();
                List<List<Object>> lines = new ArrayList<>();
                handleNoGroupByAggResponse(fields, aggregations, header, lines);
                return new ObjectResult(header, lines);
            }
        } else {
            SearchHit[] hits = searchResponse.getHits().getHits();
            List<Field> fields = query.getSelect().getFields();
            boolean flat = fields.size() == 1 && fields.get(0).getFieldName().equals("*");
            List<Map<String, Object>> docsAsMap = new ArrayList<>();
            Set<String> hitFieldNames = new HashSet<>();
            List<String> header = createHeadersAndFillDocsMap(query, flat, hits, docsAsMap, hitFieldNames);
            Map<String, String> fieldNameMap = new HashMap<>();
            if (fields.size() > 1 || !fields.get(0).getFieldName().equals("*")) {
                for (Field field : fields) {
                    if (!field.isConstant()) {
                        fieldNameMap.put(field.getFieldName(), field.getAlias());
                    }
                }
            }

            List<Field> constantFields = fields.stream().filter(Field::isConstant).collect(Collectors.toList());
            List<List<Object>> rows = createLinesFromDocs(flat, docsAsMap, header, hitFieldNames);
            header = header.stream().map(f -> StringUtils.isNotBlank(fieldNameMap.get(f)) ? fieldNameMap.get(f) : f).collect(Collectors.toList());
            setConstantField(constantFields, header, rows);
            return new ObjectResult(header, rows);
        }
    }


    private List<String> createHeadersAndFillDocsMap(Query query, boolean flat, SearchHit[] hits, List<Map<String, Object>> docsAsMap, Set<String> hitFieldNames) {
        Set<String> headers = new LinkedHashSet<>();
        List<String> fieldNames = query.getSelect().getFields().stream().map(Field::getFieldName).collect(Collectors.toList());
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


    private List<List<Object>> createLinesFromDocs(boolean flat, List<Map<String, Object>> docsAsMap, List<String> headers, Set<String> hitFieldNames) {
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

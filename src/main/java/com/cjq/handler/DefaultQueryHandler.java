package com.cjq.handler;

import com.cjq.common.Constant;
import com.cjq.common.ElasticsearchJdbcConfig;
import com.cjq.jdbc.HandlerResult;
import com.cjq.plan.logical.Field;
import com.cjq.plan.logical.Query;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;

import java.util.*;
import java.util.stream.Collectors;

import static com.cjq.common.Constant.*;

public class DefaultQueryHandler implements ResponseHandler {
    protected final Query query;
    private final Properties properties;
    private boolean isIncludeIndex;
    private boolean isIncludeDocID;
    private boolean isIncludeType;
    private boolean isIncludeScore;

    public DefaultQueryHandler(Query query, Properties properties) {
        this.query = query;
        this.properties = properties;
        setDocInclude();
    }

    @Override
    public HandlerResult handle(ActionResponse response) {
        SearchResponse searchResponse = (SearchResponse) response;
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
        return new HandlerResult(header, rows);
    }

    private void setDocInclude() {
        this.isIncludeIndex = Boolean.parseBoolean(properties.getProperty(ElasticsearchJdbcConfig.INCLUDE_INDEX.getName(),
                ElasticsearchJdbcConfig.INCLUDE_INDEX.getDefaultValue()))
                || query.getSelect().getFields().stream().map(Field::getFieldName).collect(Collectors.toList()).contains(_INDEX);
        this.isIncludeDocID = Boolean.parseBoolean(properties.getProperty(ElasticsearchJdbcConfig.INCLUDE_DOC_ID.getName(),
                ElasticsearchJdbcConfig.INCLUDE_DOC_ID.getDefaultValue()))
                || query.getSelect().getFields().stream().map(Field::getFieldName).collect(Collectors.toList()).contains(_ID)
                || (query.getSelect().getFields().size() == 1 && query.getSelect().getFields().get(0).getFieldName().equals("*"));
        this.isIncludeType = Boolean.parseBoolean(properties.getProperty(ElasticsearchJdbcConfig.INCLUDE_TYPE.getName(),
                ElasticsearchJdbcConfig.INCLUDE_TYPE.getDefaultValue()))
                || query.getSelect().getFields().stream().map(Field::getFieldName).collect(Collectors.toList()).contains(_TYPE);
        this.isIncludeScore = Boolean.parseBoolean(properties.getProperty(ElasticsearchJdbcConfig.INCLUDE_SCORE.getName(),
                ElasticsearchJdbcConfig.INCLUDE_SCORE.getDefaultValue()))
                || query.getSelect().getFields().stream().map(Field::getFieldName).collect(Collectors.toList()).contains(_SCORE);
    }

    protected void setConstantField(List<Field> constantFields, List<String> headers, List<List<Object>> values) {
        if (constantFields != null) {
            for (Field constantField : constantFields) {
                headers.add(constantField.getAlias());
                for (List<Object> value : values) {
                    value.add(constantField.getConstantValue());
                }
            }
        }
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

    private List<String> createHeadersAndFillDocsMap(Query query, boolean flat, SearchHit[] hits, List<Map<String, Object>> docsAsMap, Set<String> hitFieldNames) {
        Set<String> headers = new LinkedHashSet<>();
        List<String> fieldNames = query.getSelect().getFields().stream().map(Field::getFieldName).collect(Collectors.toList());
        String indexAlias = query.getFrom().getAlias();
        for (SearchHit hit : hits) {
            Map<String, Object> doc = hit.getSourceAsMap() != null ? hit.getSourceAsMap() : new HashMap<>();
            LinkedHashMap<String, Object> source = new LinkedHashMap<>();
            if (isIncludeScore) {
                source.put(Constant._SCORE, hit.getScore());
            }
            if (isIncludeType) {
                source.put(Constant._TYPE, hit.getType());
            }
            if (isIncludeDocID) {
                source.put(_ID, hit.getId());
            }
            if (isIncludeIndex) {
                if (StringUtils.isNotBlank(indexAlias)) {
                    source.put(Constant._INDEX, indexAlias);
                } else {
                    source.put(Constant._INDEX, hit.getIndex());
                }
            }

            source.putAll(doc);
            mergeHeaders(headers, source, flat);

            Map<String, DocumentField> fields = hit.getFields();
            for (DocumentField searchHitField : fields.values()) {
                List<Object> values = Optional.ofNullable(searchHitField.getValues()).orElse(Collections.emptyList());
                int size = values.size();
                source.put(searchHitField.getName(), size == 1 ? values.get(0) : size > 1 ? values : null);
                hitFieldNames.add(searchHitField.getName());
                headers.add(searchHitField.getName());
            }
            docsAsMap.add(source);
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

    private void mergeHeaders(Set<String> headers, Map<String, Object> doc, boolean flat) {
        if (!flat) {
            headers.addAll(doc.keySet());
            return;
        }
        mergeFieldNamesRecursive(headers, doc, "");
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
}

package com.cjq.handler;

import com.cjq.common.FieldFunction;
import com.cjq.jdbc.ObjectResult;
import com.cjq.plan.logical.Field;
import com.cjq.plan.logical.Query;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.*;

import java.util.*;
import java.util.stream.Collectors;

import static com.cjq.common.Constant.GROUP_BY_PREFIX;

public class AggQueryHandler extends DefaultQueryHandler {
    public AggQueryHandler(Query query, Properties properties) {
        super(query, properties);
    }

    @Override
    public ObjectResult handle(ActionResponse response) {
        SearchResponse searchResponse = (SearchResponse) response;
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
}

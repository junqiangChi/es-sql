package com.cjq.handler;

import com.cjq.common.ElasticsearchJdbcConfig;
import com.cjq.common.FieldFunction;
import com.cjq.exception.ErrorCode;
import com.cjq.exception.EsSqlParseException;
import com.cjq.exception.ExceptionUtils;
import com.cjq.jdbc.HandlerResult;
import com.cjq.plan.logical.*;
import com.cjq.utils.RequireUtil;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.cjq.common.Constant.*;

/**
 * Default query response handler
 * Processes search responses with unified exception handling
 */
public class DefaultQueryHandler implements ResponseHandler {
    protected final Query query;
    private final Properties properties;
    private boolean isStar;
    private boolean isIncludeIndex;
    private boolean isIncludeDocID;
    private boolean isIncludeType;
    private boolean isIncludeScore;

    /**
     * Constructs a new DefaultQueryHandler
     *
     * @param query      the query to handle
     * @param properties configuration properties
     */
    public DefaultQueryHandler(Query query, Properties properties) {
        ExceptionUtils.validateNotNull(query, "Query");
        ExceptionUtils.validateNotNull(properties, "Properties");
        this.query = query;
        this.properties = properties;
        this.isStar = query.getSelect().getFields().size() == 1 && STAR.equals(query.getSelect().getFields().get(0).getFieldName());
        setDocInclude();
    }

    /**
     * Sets document inclusion flags based on configuration and query fields
     */
    private void setDocInclude() {
        this.isIncludeIndex = Boolean.parseBoolean(properties.getProperty(ElasticsearchJdbcConfig.INCLUDE_INDEX.getName(),
                ElasticsearchJdbcConfig.INCLUDE_INDEX.getDefaultValue())) ||
                query.getSelect().getFields().stream().map(Field::getFieldName).collect(Collectors.toList()).contains(_INDEX);
        this.isIncludeDocID = Boolean.parseBoolean(properties.getProperty(ElasticsearchJdbcConfig.INCLUDE_DOC_ID.getName(),
                ElasticsearchJdbcConfig.INCLUDE_DOC_ID.getDefaultValue())) ||
                query.getSelect().getFields().stream().map(Field::getFieldName).collect(Collectors.toList()).contains(_ID) || isStar;
        this.isIncludeType = Boolean.parseBoolean(properties.getProperty(ElasticsearchJdbcConfig.INCLUDE_TYPE.getName(),
                ElasticsearchJdbcConfig.INCLUDE_TYPE.getDefaultValue())) ||
                query.getSelect().getFields().stream().map(Field::getFieldName).collect(Collectors.toList()).contains(_TYPE);
        this.isIncludeScore = Boolean.parseBoolean(properties.getProperty(ElasticsearchJdbcConfig.INCLUDE_SCORE.getName(),
                ElasticsearchJdbcConfig.INCLUDE_SCORE.getDefaultValue())) ||
                query.getSelect().getFields().stream().map(Field::getFieldName).collect(Collectors.toList()).contains(_SCORE);
    }

    @Override
    public HandlerResult handle(ActionResponse response) {
        ExceptionUtils.validateNotNull(response, "Response");
        ExceptionUtils.validateCondition(response instanceof SearchResponse, "Response must be a SearchResponse");
        return ExceptionUtils.executeWithExceptionHandling(() -> {
            SearchResponse searchResponse = (SearchResponse) response;
            SearchHit[] hits = searchResponse.getHits().getHits();
            if (isStar) {
                return handleStar(hits);
            }
            return handleGeneralField(hits);
        }, ErrorCode.DATA_PARSING_ERROR, "Failed to handle search response");
    }


    /**
     * Handle general field
     *
     * @param hits hit array
     * @return handler result
     */
    protected HandlerResult handleGeneralField(SearchHit[] hits) {
        List<String> headers = new ArrayList<>();
        List<List<Object>> lines = new ArrayList<>();
        setEsSystemFieldHeader(headers);
        int esFieldSize = headers.size();
        List<Field> fields = query.getSelect().getFields();
        Map<String, Class<? extends Field>> fieldTypeMap = new HashMap<>();
        mergeHeader(headers, fields, fieldTypeMap);

        setLines(hits, headers, lines, esFieldSize, fieldTypeMap, fields);

        return new HandlerResult(headers, lines);
    }


    /**
     * merge header
     *
     * @param headers      header list
     * @param fields       field list
     * @param fieldTypeMap field type map
     */
    private void mergeHeader(List<String> headers, List<Field> fields, Map<String, Class<? extends Field>> fieldTypeMap) {
        AtomicInteger fieldPos = new AtomicInteger(1);
        for (Field field : fields) {
            if (field instanceof FunctionField) {
                String alias = field.getAlias();
                if (alias != null) {
                    headers.add(alias);
                } else {
                    headers.add("_c" + (fieldPos.getAndIncrement()));
                }
            } else if (field instanceof ConstantField) {
                headers.add(field.getAlias());
            } else {
                String alias = field.getAlias();
                if (alias != null) {
                    headers.add(alias);
                } else {
                    headers.add(field.getFieldName());
                }
            }
            fieldTypeMap.put(headers.get(headers.size() - 1), field.getClass());
        }
    }

    /**
     * Process hits to lines
     *
     * @param hits         hit array
     * @param headers      header list
     * @param lines        line list
     * @param esFieldSize  es system field size
     * @param fieldTypeMap field type map
     * @param fields       field list
     */
    private void setLines(SearchHit[] hits, List<String> headers, List<List<Object>> lines, int esFieldSize, Map<String, Class<? extends Field>> fieldTypeMap, List<Field> fields) {
        for (SearchHit hit : hits) {
            List<Object> line = new ArrayList<>();
            Map<String, Object> source = hit.getSourceAsMap();
            setEsSystemFieldLine(hit, line, query.getFrom().getAlias());
            for (int i = 0; i < fields.size(); i++) {
                String header = headers.get(i + esFieldSize);
                Field field = fields.get(i);
                setFieldValue(fieldTypeMap, header, line, field, source);
            }
            lines.add(line);
        }
    }

    /**
     * Set value for line
     *
     * @param fieldTypeMap field type map
     * @param header       header
     * @param line         line
     * @param field        field
     * @param source       source
     */
    private void setFieldValue(Map<String, Class<? extends Field>> fieldTypeMap, String header, List<Object> line, Field field, Map<String, Object> source) {
        Class<? extends Field> fieldClass = fieldTypeMap.get(header);
        if (fieldClass.equals(ConstantField.class)) {
            line.add(((ConstantField) field).getConstantValue());
        } else if (fieldClass.equals(FunctionField.class)) {
            line.add(getFunctionValue(field, getValue((FunctionField) field, source)));
        } else if (fieldClass.equals(FunctionField.CaseWhenThenFunctionField.class)) {
            line.add(getCaseWhenThenFunctionField((FunctionField.CaseWhenThenFunctionField) field, source));
        } else if (fieldClass.equals(FunctionField.MultipleFieldValueFunctionField.class)) {
            line.add(getMultipleFieldValueFunctionField((FunctionField.MultipleFieldValueFunctionField) field, source));
        } else if (fieldClass.equals(FunctionField.MultipleValueFunctionField.class)) {
            line.add(getMultipleValueFunctionField((FunctionField.MultipleValueFunctionField) field, source));
        } else {
            line.add(ifNestedField(field, source));
        }
    }

    /**
     * Get {@link Field} or {@link Value} value from source
     *
     * @param functionField function field
     * @param source        source
     * @return value
     */
    private Object getValue(FunctionField functionField, Map<String, Object> source) {
        if (functionField.getValue() != null) {
            return functionField.getValue().getText();
        } else {
            return source.get(functionField.getFieldName());
        }
    }

    /**
     * If nested field, get value recursively
     *
     * @param filed  field
     * @param source source
     * @return value
     */
    private Object ifNestedField(Field filed, Map<String, Object> source) {
        String fieldName = filed.getFieldName();
        if (filed.isNested()) {
            List<Object> nestedValues = new ArrayList<>();
            recursiveNestedField(source, fieldName, nestedValues);
            return nestedValues;
        } else {
            return source.get(fieldName);
        }
    }

    /**
     * Recursive nested field to extract value
     *
     * @param source       doc source
     * @param fieldName    field name
     * @param nestedValues nested values
     */
    private void recursiveNestedField(Map<String, Object> source, String fieldName, List<Object> nestedValues) {
        String[] split = fieldName.split("\\.");
        if (split.length > 1 && source.get(split[0]) instanceof List) {
            List<Map<String, Object>> list = (List<Map<String, Object>>) source.get(split[0]);
            for (Map<String, Object> tmpMap : list) {
                recursiveNestedField(tmpMap, split[1], nestedValues);
            }
        } else {
            nestedValues.add(source.get(fieldName));
        }
    }


    /**
     * Get results of general functions
     *
     * @param field function field
     * @param value value
     * @return function result
     */
    private Object getFunctionValue(Field field, Object value) {
        if (value == null) {
            return null;
        }
        FunctionField functionField = (FunctionField) field;
        FieldFunction funcName = functionField.getFuncName();
        switch (funcName) {
            case LOWER:
                return value.toString().toLowerCase();
            case UPPER:
                return value.toString().toUpperCase();
            case LENGTH:
                return value.toString().length();
            case TRIM:
                return value.toString().trim();
            case ABS:
                return Math.abs(RequireUtil.requireCastType(value, Number.class).doubleValue());
            case CEIL:
                return Math.ceil(RequireUtil.requireCastType(value, Number.class).doubleValue());
            case FLOOR:
                return Math.floor(RequireUtil.requireCastType(value, Number.class).doubleValue());
            case ROUND:
                return Math.round(RequireUtil.requireCastType(value, Number.class).doubleValue());
            case SQRT:
                return Math.sqrt(RequireUtil.requireCastType(value, Number.class).doubleValue());
            case RAND:
                Random random = new Random(RequireUtil.requireCastType(value, Long.class));
                return random.nextInt();
            default:
                return value;
        }
    }


    /**
     * Get results of multiple field value function
     *
     * @param multipleFieldValueFunctionField multiple field value function field
     * @param source                          source
     * @return function result
     */
    private Object getMultipleFieldValueFunctionField(FunctionField.MultipleFieldValueFunctionField multipleFieldValueFunctionField,
                                                      Map<String, Object> source) {
        FieldFunction funcName = multipleFieldValueFunctionField.getFuncName();
        switch (funcName) {
            case CONCAT:
            case CONCAT_WS:
                return getConcatFunctionField(multipleFieldValueFunctionField, source);
            case IFNULL:
            case COALESCE:
                return getIfNullCoalesceFunctionField(multipleFieldValueFunctionField, source);
            default:
                throw new EsSqlParseException(ErrorCode.UNSUPPORTED_FUNCTION, funcName.toString());
        }
    }

    /**
     * Get results of multiple value function
     *
     * @param multipleValueFunctionField multiple value function field
     * @param source                     source
     * @return function result
     */
    private Object getMultipleValueFunctionField(FunctionField.MultipleValueFunctionField multipleValueFunctionField,
                                                 Map<String, Object> source) {
        LogicalPlan logicalPlan = multipleValueFunctionField.getLogicalPlan();
        FieldFunction funcName = multipleValueFunctionField.getFuncName();
        List<Value> values = multipleValueFunctionField.getValues();
        switch (funcName) {
            case SUBSTRING:
                return getSubStringFunctionValue(logicalPlan, values, source);
            case REPLACE:
                return getReplaceFunctionValue(logicalPlan, values, source);
            case MOD:
                return getModFunctionValue(logicalPlan, values, source);
            case POW:
                return getPowFunctionValue(logicalPlan, values, source);
            case IF:
                return getIfFunctionValue(logicalPlan, values, source);
            default:
                throw new EsSqlParseException(ErrorCode.UNSUPPORTED_FUNCTION, funcName.toString());
        }
    }

    /**
     * Get results of IF function
     *
     * @param logicalPlan logical plan
     * @param values      values
     * @param source      source
     * @return function result
     */
    private Object getIfFunctionValue(LogicalPlan logicalPlan, List<Value> values,
                                      Map<String, Object> source) {
        return getValue(logicalPlan, source, Boolean.class) ? values.get(0).getText() : values.get(1).getText();
    }

    /**
     * Get results of POW function
     *
     * @param logicalPlan logical plan
     * @param values      values
     * @param source      source
     * @return function result
     */
    private double getPowFunctionValue(LogicalPlan logicalPlan, List<Value> values,
                                       Map<String, Object> source) {
        List<Number> ints = values.stream().map(v -> RequireUtil.requireCastType(v.getText(), Number.class))
                .collect(Collectors.toList());
        Number value = getValue(logicalPlan, source, Number.class);
        return Math.pow(value.doubleValue(), ints.get(0).doubleValue());
    }

    /**
     * Get results of MOD function
     *
     * @param logicalPlan logical plan
     * @param values      values
     * @param source      source
     * @return function result
     */
    private int getModFunctionValue(LogicalPlan logicalPlan, List<Value> values,
                                    Map<String, Object> source) {
        List<Integer> ints = values.stream().map(v -> RequireUtil.requireCastType(v.getText(), Integer.class))
                .collect(Collectors.toList());
        Number value = getValue(logicalPlan, source, Number.class);
        return (int) (value.doubleValue() % ints.get(0));
    }

    /**
     * Get results of REPLACE function
     *
     * @param logicalPlan logical plan
     * @param values      values
     * @param source      source
     * @return function result
     */
    private String getReplaceFunctionValue(LogicalPlan logicalPlan, List<Value> values,
                                           Map<String, Object> source) {
        List<String> strings = values.stream().map(v -> RequireUtil.requireCastType(v.getText(), String.class))
                .collect(Collectors.toList());
        String value = getValue(logicalPlan, source, String.class);
        return value.replace(strings.get(0), strings.get(1));
    }

    /**
     * Get results of SUBSTRING function
     *
     * @param logicalPlan logical plan
     * @param values      values
     * @param source      source
     * @return function result
     */

    private String getSubStringFunctionValue(LogicalPlan logicalPlan, List<Value> values,
                                             Map<String, Object> source) {
        List<Integer> ints = values.stream().map(v -> RequireUtil.requireCastType(v.getText(), Integer.class))
                .collect(Collectors.toList());
        String value = getValue(logicalPlan, source, String.class);
        if (ints.get(0) >= value.length()) {
            return "";
        }
        return ints.size() == 2 ? value.substring(ints.get(0), Math.min(ints.get(0) + ints.get(1), value.length())) :
                value.substring(ints.get(0));
    }

    /**
     * Get value from source
     *
     * @param logicalPlan logical plan
     * @param source      source
     * @param aClass      class type
     * @param <T>         class type
     * @return value
     */
    private <T> T getValue(LogicalPlan logicalPlan, Map<String, Object> source, Class<T> aClass) {
        T value;
        if (logicalPlan instanceof Field) {
            Field field = (Field) logicalPlan;
            String fieldName = field.getFieldName();
            value = RequireUtil.requireCastType(source.get(fieldName), aClass);
        } else if (logicalPlan instanceof Value) {
            value = RequireUtil.requireCastType(((Value) logicalPlan).getText(), aClass);
        } else if (logicalPlan instanceof Where) {
            Where where = (Where) logicalPlan;
            return RequireUtil.requireCastType(handleCondition(where.getCondition(), source), aClass);
        } else {
            throw new EsSqlParseException(ErrorCode.UNSUPPORTED_PLAN_TYPE, logicalPlan.getClass().toString());
        }
        return value;
    }

    /**
     * Get results of case when then functions
     *
     * @param caseWhenThenFunctionField case when then function field
     * @param source                    source
     * @return function result
     */
    private Object getCaseWhenThenFunctionField(FunctionField.CaseWhenThenFunctionField caseWhenThenFunctionField,
                                                Map<String, Object> source) {
        List<Where> wheres = caseWhenThenFunctionField.getWheres();
        for (int i = 0; i < wheres.size(); i++) {
            Condition condition = wheres.get(i).getCondition();
            if (handleCondition(condition, source)) {
                return caseWhenThenFunctionField.getThen().get(i).getText();
            }
        }
        return caseWhenThenFunctionField.getElseValue().getText();
    }


    /**
     * Handle condition
     *
     * @param condition condition
     * @param source    source
     * @return boolean result
     */
    private boolean handleCondition(Condition condition, Map<String, Object> source) {
        Field field = condition.getField();
        Value value = condition.getValue();
        Object object = source.get(field.getFieldName());
        List<Object> values;
        if (field instanceof FunctionField) {
            FunctionField functionField = (FunctionField) field;
            if (functionField.getFuncName().isAggFunction()) {
                throw new EsSqlParseException(ErrorCode.UNSUPPORTED_AGGREGATION_FUNCTION, functionField.getFieldName());
            }
            if (field.isNested()) {
                throw new EsSqlParseException(ErrorCode.UNSUPPORTED_FUNCTION, "Nested filed don't support function with caseWhen");
            }
            object = getFunctionValue(field, object);
        } else {
            object = ifNestedField(field, source);
        }
        switch (condition.getOpera()) {
            case GT:
                checkCondition(value.getText() instanceof Number && object instanceof Number, condition);
                return (Double) value.getText() > (Double) object;
            case LT:
                checkCondition(value.getText() instanceof Number && object instanceof Number, condition);
                return (Double) value.getText() < (Double) object;
            case GTE:
                checkCondition(value.getText() instanceof Number && object instanceof Number, condition);
                return (Double) value.getText() >= (Double) object;
            case LTE:
                checkCondition(value.getText() instanceof Number && object instanceof Number, condition);
                return (Double) value.getText() <= (Double) object;
            case EQ:
                return Objects.equals(value.getText(), object);
            case NIN:
                values = getValues(condition);
                return !values.contains(object);
            case IN:
                values = getValues(condition);
                return values.contains(object);
            case NREGEXP:
                RequireUtil.requireNotNull(object);
                return !object.toString().matches(value.getText().toString());
            case REGEXP:
                RequireUtil.requireNotNull(object);
                return object.toString().matches(value.getText().toString());
            case IS:
                return object == null;
            case ISN:
                return object != null;
            default:
                throw new EsSqlParseException(ErrorCode.UNKNOWN_WHERE_TYPE, String.valueOf(condition.getOpera()));
        }
    }


    /**
     * Check condition
     *
     * @param isException is exception
     * @param condition   condition
     */
    private void checkCondition(boolean isException, Condition condition) {
        if (isException) {
            throw new EsSqlParseException(ErrorCode.UNKNOWN_WHERE_TYPE, String.valueOf(condition.getOpera()));
        }
    }

    /**
     * Get all values for a condition
     *
     * @param condition condition
     * @return value list
     */
    private List<Object> getValues(Condition condition) {
        List<Object> values = new ArrayList<>();
        Value currentValue = condition.getValue();

        values.add(currentValue.getText());
        while (currentValue.getPlan() != null) {
            currentValue = (Value) currentValue.getPlan();
            values.add(currentValue.getText());
        }

        return values;
    }

    /**
     * Get results of concat functions
     *
     * @param multipleFieldValueFunctionField concat function field
     * @param source                          source
     * @return function result
     */
    private String getConcatFunctionField(FunctionField.MultipleFieldValueFunctionField multipleFieldValueFunctionField, Map<String, Object> source) {
        FieldFunction funcName = multipleFieldValueFunctionField.getFuncName();
        List<LogicalPlan> concatFields = multipleFieldValueFunctionField.getMultipleLogicalPlan();
        String separator = "";
        int indexFrom = 0;
        if (funcName.equals(FieldFunction.CONCAT_WS)) {
            separator = ((Value) concatFields.get(0)).getText().toString();
            indexFrom++;
        }
        List<String> values = new ArrayList<>();
        for (int i = indexFrom; i < concatFields.size(); i++) {
            if (concatFields.get(i) instanceof Field) {
                Field field = (Field) concatFields.get(i);
                Object obj = source.get(field.getFieldName());
                if (field instanceof FunctionField) {
                    obj = getFunctionValue(field, obj);
                }
                values.add(obj == null ? "NULL" : obj.toString());
            } else {
                values.add(((Value) concatFields.get(i)).getText().toString());
            }
        }
        return String.join(separator, values);
    }


    /**
     * Get results of IFNULL or COALESCE functions
     *
     * @param multipleFieldValueFunctionField function field
     * @param source                          source
     * @return function result
     */
    private Object getIfNullCoalesceFunctionField(FunctionField.MultipleFieldValueFunctionField multipleFieldValueFunctionField,
                                                  Map<String, Object> source) {
        List<LogicalPlan> multipleLogicalPlan = multipleFieldValueFunctionField.getMultipleLogicalPlan();
        for (LogicalPlan logicalPlan : multipleLogicalPlan) {
            if (logicalPlan instanceof Value) {
                if (((Value) logicalPlan).getText() != null) {
                    return ((Value) logicalPlan).getText();
                }
            } else if (logicalPlan instanceof Field) {
                Object obj = ifNestedField((Field) logicalPlan, source);
                if (obj != null) {
                    return obj;
                }
            }
        }
        return null;
    }

    /**
     * Handle star function
     *
     * @param hits search hits
     * @return handler result
     */
    private HandlerResult handleStar(SearchHit[] hits) {
        List<String> headers = new ArrayList<>();
        List<List<Object>> lines = new ArrayList<>();
        setEsSystemFieldHeader(headers);
        int esFieldSize = headers.size();
        HashSet<String> hitHeaders = getHitHeaders(hits);
        headers.addAll(hitHeaders);
        for (SearchHit hit : hits) {
            Map<String, Object> source = hit.getSourceAsMap();
            List<Object> line = new ArrayList<>();
            setEsSystemFieldLine(hit, line, query.getFrom().getAlias());
            for (int i = esFieldSize; i < headers.size(); i++) {
                line.add(source.get(headers.get(i)));
            }
            lines.add(line);
        }
        return new HandlerResult(headers, lines);
    }

    /**
     * Set es system field header
     *
     * @param header header
     */
    private void setEsSystemFieldHeader(List<String> header) {
        if (isIncludeScore) {
            header.add(_SCORE);
        }
        if (isIncludeType) {
            header.add(_SCORE);
        }
        if (isIncludeDocID) {
            header.add(_ID);
        }
        if (isIncludeIndex) {
            header.add(_INDEX);
        }
    }

    /**
     * Set es system field in line
     *
     * @param hit        result hit
     * @param line       line
     * @param indexAlias index alias
     */
    private void setEsSystemFieldLine(SearchHit hit, List<Object> line, String indexAlias) {
        if (isIncludeScore) {
            line.add(hit.getScore());
        }
        if (isIncludeType) {
            line.add(hit.getType());
        }
        if (isIncludeDocID) {
            line.add(hit.getId());
        }
        if (isIncludeIndex) {
            if (StringUtils.isNotBlank(indexAlias)) {
                line.add(indexAlias);
            } else {
                line.add(hit.getIndex());
            }
        }
    }

    /**
     * Get result headers
     *
     * @param hits hits
     * @return hit headers
     */
    private HashSet<String> getHitHeaders(SearchHit[] hits) {
        HashSet<String> header = new HashSet<>();
        Arrays.stream(hits).forEach(hit -> {
            header.addAll(hit.getSourceAsMap().keySet());
        });
        return header;
    }


    /**
     * Sets constant field values in the result
     *
     * @param fields the list of fields in the query
     * @param header the list of header names
     * @param rows   the list of rows
     */
    protected void setConstantFields(List<Field> fields, List<String> header, List<List<Object>> rows) {
        Map<String, String> fieldNameMap = new HashMap<>();
        if (fields.size() > 1 || !fields.get(0).getFieldName().equals("*")) {
            for (Field field : fields) {
                if (!(field instanceof ConstantField)) {
                    fieldNameMap.put(field.getFieldName(), field.getAlias());
                }
            }
        }
        List<ConstantField> constantFields = fields.stream().filter(f -> f instanceof ConstantField).map(f -> (ConstantField) f).collect(Collectors.toList());

        header = header.stream().map(f -> StringUtils.isNotBlank(fieldNameMap.get(f)) ? fieldNameMap.get(f) : f).collect(Collectors.toList());
        for (ConstantField constantField : constantFields) {
            header.add(constantField.getFieldName());
            for (List<Object> value : rows) {
                value.add(constantField.getConstantValue());
            }
        }
    }
}

package com.cjq.action;

import com.cjq.common.Constant;
import com.cjq.common.WhereOpr;
import com.cjq.exception.ErrorCode;
import com.cjq.exception.EsSqlParseException;
import com.cjq.plan.logical.*;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class DefaultQueryActionPlan implements ActionPlan {

    protected final Query query;
    protected final SearchRequest searchRequest = new SearchRequest();
    protected final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    private Limit limit;

    public DefaultQueryActionPlan(LogicalPlan logicalPlan) {
        this.query = (Query) logicalPlan;
        checkQuerySql();
    }

    public DefaultQueryActionPlan() {
        this.query = null;
    }

    private void checkQuerySql() {
        List<Field> fields = query.getSelect().getFields();
        HashSet<String> fieldSet = new HashSet<>();

        for (Field field : fields) {
            String fieldName = field.getAlias() != null ? field.getAlias() : field.getFieldName();
            if (fieldSet.contains(fieldName)) {
                throw new EsSqlParseException(ErrorCode.DUPLICATE_FIELD + fieldName);
            }
            if (field instanceof FunctionField && ((FunctionField) field).getFuncName().isAggFunction() && !field.isNested()) {
                throw new EsSqlParseException(ErrorCode.UNSUPPORTED_NESTED_FUNCTION, fieldName);
            }
            fieldSet.add(fieldName);
        }
    }

    @Override
    public ActionRequest explain() {
        setFrom(query.getFrom());
        setSelect(query.getSelect());
        setWhere(query.getWhere());
        processLogicalPlanChain();
        setLimit();
        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    }

    private void processLogicalPlanChain() {
        LogicalPlan currentPlan = query.getPlan();
        while (currentPlan != null) {
            if (currentPlan instanceof OrderBy) {
                setOrderBy((OrderBy) currentPlan);
            } else if (currentPlan instanceof Limit) {
                limit = (Limit) currentPlan;
            }
            currentPlan = currentPlan.getPlan();
        }
    }

    protected void setSelect(Select select) {
        List<Field> allFields = select.getFields();

        if (allFields.size() == 1 && Constant.STAR.equals(allFields.get(0).getFieldName())) {
            searchSourceBuilder.fetchSource(new FetchSourceContext(true, null, null));
            return;
        }

        List<Field> fields = allFields.stream().filter(f -> !(f instanceof ConstantField)).collect(Collectors.toList());
        List<String> addFields = new ArrayList<>();
        fields.forEach(field -> {
            if (field instanceof FunctionField.CaseWhenThenFunctionField) {
                FunctionField.CaseWhenThenFunctionField caseWhenThenFunctionField = (FunctionField.CaseWhenThenFunctionField) field;
                List<Where> wheres = caseWhenThenFunctionField.getWheres();
                for (Where where : wheres) {
                    Field conditionField = where.getCondition().getField();
                    addFields.add(conditionField.getFieldName());
                }
            } else if (field instanceof FunctionField.MultipleFieldValueFunctionField) {
                FunctionField.MultipleFieldValueFunctionField multipleFieldValueFunctionField = (FunctionField.MultipleFieldValueFunctionField) field;
                multipleFieldValueFunctionField.getMultipleLogicalPlan().stream().filter(f -> f instanceof Field)
                        .map(f -> ((Field) f).getFieldName())
                        .forEach(addFields::add);
            } else if (field instanceof FunctionField.MultipleValueFunctionField) {
                FunctionField.MultipleValueFunctionField multipleValueFunctionField = (FunctionField.MultipleValueFunctionField) field;
                if (multipleValueFunctionField.getLogicalPlan() instanceof Field) {
                    addFields.add(((Field) multipleValueFunctionField.getLogicalPlan()).getFieldName());
                }
            } else {
                addFields.add(field.getFieldName());
            }
        });

        List<String> fieldNames = addFields.stream().filter(Objects::nonNull).distinct().collect(Collectors.toList());
        String[] fieldsToFetch = fieldNames.toArray(new String[0]);

        if (fieldsToFetch.length > 0) {
            searchSourceBuilder.fetchSource(new FetchSourceContext(true, fieldsToFetch, null));
        }
    }

    protected void setFrom(From from) {
        searchRequest.indices(from.getIndex());
    }

    protected void setGroupBy(GroupBy groupBy) {
        // Nothing to do for default query
    }

    protected void setLimit() {
        if (limit == null) {
            searchSourceBuilder.from(Constant.DEFAULT_FROM);
            searchSourceBuilder.size(Constant.DEFAULT_SIZE);
        }
    }

    protected void setWhere(Where where) {
        if (where == null) {
            return;
        }

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        Where currentWhere = where;

        do {
            Condition condition = currentWhere.getCondition();
            QueryBuilder subQueryBuilder = buildCondition(condition);

            if (currentWhere.getOpr() == WhereOpr.OR) {
                boolQueryBuilder.should(subQueryBuilder);
            } else {
                boolQueryBuilder.must(subQueryBuilder);
            }

            currentWhere = currentWhere.getNextWhere();
        } while (currentWhere != null);

        searchSourceBuilder.query(boolQueryBuilder);
    }

    protected void setOrderBy(OrderBy orderBy) {
        orderBy.getSorts().stream()
                .filter(sort -> sort != null)
                .forEach(sort -> searchSourceBuilder.sort(sort.field,
                        SortOrder.valueOf(sort.getOrderType().toString())));
    }

    /**
     * Build condition query
     */
    public QueryBuilder buildCondition(Condition condition) {
        Field field = condition.getField();
        if (field instanceof FunctionField) {
            throw new EsSqlParseException(ErrorCode.UNSUPPORTED_FUNCTION, field.toString());
        }
        String fieldName = field.getFieldName();
        Object value = condition.getValue().getText();

        switch (condition.getOpera()) {
            case GT:
                return QueryBuilders.rangeQuery(fieldName).gt(value);
            case LT:
                return QueryBuilders.rangeQuery(fieldName).lt(value);
            case GTE:
                return QueryBuilders.rangeQuery(fieldName).gte(value);
            case LTE:
                return QueryBuilders.rangeQuery(fieldName).lte(value);
            case EQ:
            case MATCH_PHRASE:
                return QueryBuilders.matchPhraseQuery(fieldName, value);
            case MATCH:
                return QueryBuilders.matchQuery(fieldName, value).operator(Operator.AND);
            case TERM:
                return QueryBuilders.termQuery(fieldName, value);
            case NIN:
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.termsQuery(fieldName, getValues(condition)));
            case IN:
                return QueryBuilders.termsQuery(fieldName, getValues(condition));
            case NBETWEEN:
                return QueryBuilders.boolQuery()
                        .mustNot(QueryBuilders.rangeQuery(fieldName)
                                .gte(value)
                                .lte(((Value) condition.getValue().getPlan()).getText()));
            case BETWEEN:
                return QueryBuilders.rangeQuery(fieldName)
                        .gte(value)
                        .lte(((Value) condition.getValue().getPlan()).getText());
            case NLIKE:
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.wildcardQuery(fieldName,
                        value.toString().replace(Constant.PERCENT_SYMBOL, Constant.WILDCARD_SYMBOL)));
            case LIKE:
                return QueryBuilders.wildcardQuery(fieldName,
                        value.toString().replace(Constant.PERCENT_SYMBOL, Constant.WILDCARD_SYMBOL));
            case NREGEXP:
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.regexpQuery(fieldName, value.toString()));
            case REGEXP:
                return QueryBuilders.regexpQuery(fieldName, value.toString());
            case IS:
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(fieldName));
            default:
                throw new EsSqlParseException(ErrorCode.UNKNOWN_WHERE_TYPE, String.valueOf(condition.getOpera()));
        }
    }

    /**
     * Get all values for a condition
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
}

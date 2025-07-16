package com.cjq.action;

import com.cjq.common.Constant;
import com.cjq.common.WhereOpr;
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
            if (!fieldSet.add(fieldName)) {
                throw new EsSqlParseException(Constant.ERROR_DUPLICATE_FIELD + fieldName);
            }
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
        List<Field> fields = select.getFields();
        
        if (fields.size() == 1 && Constant.WILDCARD_ALL.equals(fields.get(0).getFieldName())) {
            searchSourceBuilder.fetchSource(new FetchSourceContext(true, null, null));
            return;
        }
        
        String[] fieldsToFetch = fields.stream()
                .filter(field -> !field.isConstant())
                .map(Field::getFieldName)
                .toArray(String[]::new);
                
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
        String field = condition.getField();
        Object value = condition.getValue().getText();

        switch (condition.getOpera()) {
            case GT:
                return QueryBuilders.rangeQuery(field).gt(value);
            case LT:
                return QueryBuilders.rangeQuery(field).lt(value);
            case GTE:
                return QueryBuilders.rangeQuery(field).gte(value);
            case LTE:
                return QueryBuilders.rangeQuery(field).lte(value);
            case EQ:
            case MATCH_PHRASE:
                return QueryBuilders.matchPhraseQuery(field, value);
            case MATCH:
                return QueryBuilders.matchQuery(field, value).operator(Operator.AND);
            case TERM:
                return QueryBuilders.termQuery(field, value);
            case NIN:
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.termsQuery(field, getValues(condition)));
            case IN:
                return QueryBuilders.termsQuery(field, getValues(condition));
            case NBETWEEN:
                return QueryBuilders.boolQuery()
                        .mustNot(QueryBuilders.rangeQuery(field)
                                .gte(value)
                                .lte(((Value) condition.getValue().getPlan()).getText()));
            case BETWEEN:
                return QueryBuilders.rangeQuery(field)
                        .gte(value)
                        .lte(((Value) condition.getValue().getPlan()).getText());
            case NLIKE:
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.wildcardQuery(field,
                        value.toString().replace(Constant.PERCENT_SYMBOL, Constant.WILDCARD_SYMBOL)));
            case LIKE:
                return QueryBuilders.wildcardQuery(field,
                        value.toString().replace(Constant.PERCENT_SYMBOL, Constant.WILDCARD_SYMBOL));
            case NREGEXP:
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.regexpQuery(field, value.toString()));
            case REGEXP:
                return QueryBuilders.regexpQuery(field, value.toString());
            case IS:
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(field));
            default:
                throw new EsSqlParseException(Constant.ERROR_UNKNOWN_WHERE_TYPE + condition.getOpera());
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

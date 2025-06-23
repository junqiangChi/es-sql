package com.cjq.action;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class DefaultQueryActionPlan implements ActionPlan {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultQueryActionPlan.class);
    protected Query query;
    protected SearchRequest searchRequest = new SearchRequest();
    protected SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    public DefaultQueryActionPlan(LogicalPlan logicalPlan) {
        this.query = (Query) logicalPlan;
        checkQuerySql();
    }

    public DefaultQueryActionPlan() {
    }

    private void checkQuerySql() {
        List<Field> fields = query.getSelect().getFields();
        HashSet<String> fieldSet = new HashSet<>();
        for (Field field : fields) {
            if (field.getAlias() != null) {
                if (fieldSet.contains(field.getAlias())) {
                    throw new EsSqlParseException("Duplicate field: " + field);
                }
                fieldSet.add(field.getAlias());
            } else {
                if (fieldSet.contains(field.getFieldName())) {
                    throw new EsSqlParseException("Duplicate field: " + field);
                }
                fieldSet.add(field.getFieldName());
            }
        }
    }

    @Override
    public ActionRequest explain() {
        setFrom(query.getFrom());
        setSelect(query.getSelect());
        setWhere(query.getWhere());
        LogicalPlan tmpLogicalPlan = query.getPlan();
        while (tmpLogicalPlan != null) {
            if (tmpLogicalPlan instanceof OrderBy) {
                setOrderBy((OrderBy) tmpLogicalPlan);
            }
            if (tmpLogicalPlan instanceof Limit) {
                setLimit((Limit) tmpLogicalPlan);
            }
            tmpLogicalPlan = tmpLogicalPlan.getPlan();
        }
        LOG.debug("searchQuery: {}", searchSourceBuilder);
        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    }

    protected void setSelect(Select select) {
        if (select.getFields().size() == 1 && "*".equals(select.getFields().get(0).getFieldName())) {
            searchSourceBuilder.fetchSource(new FetchSourceContext(true, null, null));
            return;
        }
        String[] fieldsToFetch = select.getFields()
                .stream()
                .filter(f -> !f.isConstant())
                .map(Field::getFieldName).toArray(String[]::new);
        if (fieldsToFetch.length > 0) {
            searchSourceBuilder.fetchSource(new FetchSourceContext(true, fieldsToFetch, null));
        }
    }

    protected void setFrom(From from) {
        searchRequest.indices(from.getIndex());
    }

    protected void setGroupBy(GroupBy groupBy) {
        // Nothing to do
    }

    protected void setLimit(Limit limit) {
        searchSourceBuilder.from(limit.getFrom());
        searchSourceBuilder.size(limit.getSize());
    }

    protected void setWhere(Where where) {
        if (where == null) {
            return;
        }
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        Where tmpWhere = where;
        do {
            Condition condition = tmpWhere.getCondition();
            QueryBuilder subBoolQueryBuilder = buildCondition(condition);
            if (tmpWhere.getOpr() == WhereOpr.OR) {
                boolQueryBuilder.should(subBoolQueryBuilder);
            } else {
                boolQueryBuilder.must(subBoolQueryBuilder);
            }
            tmpWhere = where.getNextWhere();
        } while (tmpWhere != null);
        searchSourceBuilder.query(boolQueryBuilder);
    }

    protected void setOrderBy(OrderBy orderBy) {
        for (Sort sort : orderBy.getSorts()) {
            if (sort != null) {
                searchSourceBuilder.sort(sort.field, SortOrder.valueOf(sort.getOrderType().toString()));
            }
        }
    }

    /**
     * @param condition
     * @return
     */
    private QueryBuilder buildCondition(Condition condition) {
        switch (condition.getOpera()) {
            case GT:
                return QueryBuilders.rangeQuery(condition.getField()).gt(condition.getValue().getText());
            case LT:
                return QueryBuilders.rangeQuery(condition.getField()).lt(condition.getValue().getText());
            case GTE:
                return QueryBuilders.rangeQuery(condition.getField()).gte(condition.getValue().getText());
            case LTE:
                return QueryBuilders.rangeQuery(condition.getField()).lte(condition.getValue().getText());
            case EQ:
            case MATCH_PHRASE:
                return QueryBuilders.matchPhraseQuery(condition.getField(), condition.getValue().getText());
            case MATCH:
                return QueryBuilders.matchQuery(condition.getField(),
                        condition.getValue().getText()).operator(Operator.AND);
            case TERM:
                return QueryBuilders.termQuery(condition.getField(), condition.getValue().getText());
            case NIN:
                QueryBuilders.boolQuery().mustNot(QueryBuilders.termsQuery(condition.getField(), getValues(condition)));
            case IN:
                return QueryBuilders.termsQuery(condition.getField(), getValues(condition));
            case NBETWEEN:
                return QueryBuilders.boolQuery()
                        .mustNot(QueryBuilders.rangeQuery(condition.getField())
                                .gte(condition.getValue().getText())
                                .lte(((Value) condition.getValue().getPlan()).getText()));
            case BETWEEN:
                return QueryBuilders.rangeQuery(condition.getField())
                        .gte(condition.getValue().getText())
                        .lte(((Value) condition.getValue().getPlan()).getText());
            case NLIKE:
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.wildcardQuery(condition.getField(),
                        condition.getValue().getText().toString().replace("%", "*")));
            case LIKE:
                return QueryBuilders.wildcardQuery(condition.getField(),
                        condition.getValue().getText().toString().replace("%", "*"));
            case NREGEXP:
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.regexpQuery(condition.getField(),
                        condition.getValue().getText().toString()));
            case REGEXP:
                return QueryBuilders.regexpQuery(condition.getField(), condition.getValue().getText().toString());
            case IS:
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(condition.getField()));
            default:
                throw new EsSqlParseException("unknown where type");
        }
    }

    private List<Object> getValues(Condition condition) {
        ArrayList<Object> values = new ArrayList<>();
        Value v = condition.getValue();
        values.add(v.getText());
        while (v.getPlan() != null) {
            v = (Value) v.getPlan();
            values.add(v.getText());
        }
        return values;
    }
}

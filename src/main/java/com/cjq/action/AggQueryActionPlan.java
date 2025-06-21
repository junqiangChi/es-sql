package com.cjq.action;

import com.cjq.exception.EsSqlParseException;
import com.cjq.plan.logical.*;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.cjq.common.Constant.GROUP_BY_PREFIX;

public class AggQueryActionPlan extends DefaultQueryActionPlan {
    private List<Field> funcFields;
    private OrderBy orderBy;

    public AggQueryActionPlan(LogicalPlan logicalPlan) {
        super(logicalPlan);
    }

    @Override
    public ActionRequest explain() {
        checkAgg();
        ActionRequest actionRequest = super.explain();
        funcFields = query.getSelect().getFields().stream().filter(Field::isFunction).collect(Collectors.toList());
        setGroupBy(query.getGroupBy());
        return actionRequest;
    }

    private void checkAgg() {
        if (query.getGroupBy() == null) {
            for (Field field : query.getSelect().getFields()) {
                if (field.getFuncName() == null) {
                    throw new EsSqlParseException("The Aggregation SQL can only have aggregation function fields without groupBy filed");
                }
            }
        } else {
            List<Field> groupByFields = query.getGroupBy().getGroupByFields();
            List<String> groupByFieldNames = groupByFields.stream().map(Field::getFieldName).collect(Collectors.toList());
            List<Field> normalField = query.getSelect().getFields().stream().filter(f -> f.getFuncName() == null)
                    .collect(Collectors.toList());
            for (Field field : normalField) {
                if (!groupByFieldNames.contains(field.getFieldName())) {
                    throw new EsSqlParseException("The filed " + field.getFieldName() + "is not aggregation field");
                }
            }
        }
    }

    @Override
    protected void setSelect(Select select) {
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
    }


    @Override
    protected void setOrderBy(OrderBy orderBy) {
        this.orderBy = orderBy;
    }

    @Override
    protected void setGroupBy(GroupBy groupBy) {
        if (groupBy == null) {
            for (Field funcField : funcFields) {
                searchSourceBuilder.aggregation(getAggFunction(funcField));
            }
        } else {
            List<Field> groupByFields = groupBy.getGroupByFields();
            TermsAggregationBuilder firstAggregation = null;
            TermsAggregationBuilder lastAggregation = null;
            for (Field groupByField : groupByFields) {
                if (lastAggregation == null) {
                    lastAggregation = AggregationBuilders.terms(GROUP_BY_PREFIX + groupByField.getFieldName())
                            .field(groupByField.getFieldName())
                            .size(10000);
                    firstAggregation = lastAggregation;
                } else {
                    TermsAggregationBuilder tmpAggregation = AggregationBuilders.terms(GROUP_BY_PREFIX + groupByField.getFieldName())
                            .field(groupByField.getFieldName())
                            .size(10000);
                    lastAggregation.subAggregation(tmpAggregation);
                    lastAggregation = tmpAggregation;
                }
            }
            if (orderBy != null) {
                List<Sort> sorts = orderBy.getSorts();
                applySort(sorts, lastAggregation);
            }
            for (Field funcField : funcFields) {
                AggregationBuilder aggFunction = getAggFunction(funcField);
                lastAggregation.subAggregation(aggFunction);
            }
            searchSourceBuilder.aggregation(firstAggregation);
        }
        searchSourceBuilder.size(0);
    }

    private void applySort(List<Sort> sorts, TermsAggregationBuilder aggregation) {
        List<BucketOrder> orders = new ArrayList<>();
        List<Field> fields = query.getSelect().getFields();
        Set<String> sortableFields = new HashSet<>();
        fields.forEach(field -> {
            if (field.getAlias() != null) {
                sortableFields.add(field.getAlias());
            } else {
                sortableFields.add(field.getFieldName());
            }
        });
        for (Field groupByField : query.getGroupBy().getGroupByFields()) {
            sortableFields.add(groupByField.getFieldName());
        }
        List<String> funcFieldName = funcFields.stream()
                .filter(Field::isFunction)
                .map(Field::getAlias)
                .collect(Collectors.toList());
        for (Sort sort : sorts) {
            String aggName = sort.getField();
            boolean isFuncField = funcFieldName.contains(aggName);
            boolean isAsc = sort.getOrderType().equals(Sort.OrderType.ASC);
            if (sortableFields.contains(aggName)) {
                if (isFuncField) {
                    BucketOrder.aggregation(aggName, isAsc);
                } else {
                    orders.add(BucketOrder.key(isAsc));
                }
            } else {
                throw new EsSqlParseException("Sort field only support name or alias");
            }
        }
        if (!orders.isEmpty()) {
            aggregation.order(orders);
        }
    }

    private AggregationBuilder getAggFunction(Field funcField) {
        String funcFieldName = funcField.getAlias() != null ? funcField.getAlias() : funcField.getFuncName() +
                "(" + funcField.getFieldName() + ")";
        switch (funcField.getFuncName()) {
            case COUNT:
                String field = funcField.getFieldName().equals("*") ? "_index" : funcField.getFieldName();
                return AggregationBuilders.count(funcFieldName).missing(0).field(field);
            case MAX:
                return AggregationBuilders.max(funcFieldName).missing(0).field(funcField.getFieldName());
            case MIN:
                return AggregationBuilders.min(funcFieldName).missing(0).field(funcField.getFieldName());
            case AVG:
                return AggregationBuilders.avg(funcFieldName).missing(0).field(funcField.getFieldName());
            case SUM:
                return AggregationBuilders.sum(funcFieldName).missing(0).field(funcField.getFieldName());
            default:
                throw new EsSqlParseException("The function " + funcField.getFuncName() + " is not supported yet");
        }
    }
}

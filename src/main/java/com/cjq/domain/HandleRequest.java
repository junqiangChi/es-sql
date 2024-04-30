package com.cjq.domain;

import com.cjq.common.WhereOpr;
import com.cjq.plan.logical.*;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;

public class HandleRequest {
    public SearchResponse search(Query query, RestHighLevelClient client) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        setFrom(searchRequest, query.getFrom());
        setSelect(searchSourceBuilder, query.getSelect());
        setWhere(searchSourceBuilder, query.getWhere());
        if (query.getPlan() instanceof Limit) {
            setLimit(searchSourceBuilder, (Limit) query.getPlan());
        }
        searchRequest.source(searchSourceBuilder);
        return client.search(searchRequest, RequestOptions.DEFAULT);
    }

    private void setSelect(SearchSourceBuilder searchSourceBuilder, Select select) {
        String[] fieldsToFetch = select.getFields().stream().map(Field::getField).toArray(String[]::new);
        if (fieldsToFetch.length > 0) {
            searchSourceBuilder.fetchSource(new FetchSourceContext(true, fieldsToFetch, null));
        }
    }

    private void setFrom(SearchRequest searchRequest, From from) {
        searchRequest.indices(from.getIndex());
    }

    private void setLimit(SearchSourceBuilder searchSourceBuilder, Limit limit) {
        searchSourceBuilder.size(limit.getNum());
    }

    private void setWhere(SearchSourceBuilder searchSourceBuilder, Where where) {
        if (where == null) {
            return;
        }
        if (where.getOpr() == null) {
            searchSourceBuilder.query(setWhereType(where.getCondition()));
        } else {
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            do {
                Condition condition = where.getCondition();
                if (where.getOpr() == WhereOpr.AND) {
                    boolQueryBuilder.must(setWhereType(condition));
                } else if (where.getOpr() == WhereOpr.OR) {
                    boolQueryBuilder.should(setWhereType(condition));
                }

                where = where.getNextWhere();
            } while (where != null);
            searchSourceBuilder.query(boolQueryBuilder);
        }
    }

    /**
     * @param condition
     * @return
     */
    private QueryBuilder setWhereType(Condition condition) {
        switch (condition.getOpera()) {
            case GT:
                return new RangeQueryBuilder(condition.getField()).gt(condition.getValue().getText());
            case LT:
                return null;
            case GTE:
                return null;
            case LTE:
                return null;
            case EQ:
            case MATCH_PHRASE:
                return new MatchPhraseQueryBuilder(condition.getField(), condition.getValue().getText());
            case MATCH:
                return new MatchQueryBuilder(condition.getField(), condition.getValue().getText());
            case TERM:
                return new TermQueryBuilder(condition.getField(), condition.getValue().getText());
            case IN:
                return null;
            case NIN:
                return null;
            case BETWEEN:
                return null;
            case NBETWEEN:
                return null;
            case LIKE:
                return new WildcardQueryBuilder(condition.getField(),
                        condition.getValue().getText().toString().replace("%", "*"));
            case NLIKE:
                return null;
            case REGEXP:
                return null;
            default:
                throw new RuntimeException("unknown where type");

        }
    }
}

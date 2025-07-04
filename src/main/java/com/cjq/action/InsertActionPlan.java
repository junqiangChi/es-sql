package com.cjq.action;

import com.cjq.domain.Client;
import com.cjq.exception.EsSqlParseException;
import com.cjq.plan.logical.Field;
import com.cjq.plan.logical.Insert;
import com.cjq.plan.logical.LogicalPlan;
import com.cjq.plan.logical.Value;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;

import java.io.IOException;
import java.util.*;

public class InsertActionPlan implements ActionPlan {
    private final Client client;
    private final Insert insert;

    public InsertActionPlan(Client client, LogicalPlan plan) {
        this.client = client;
        this.insert = (Insert) plan;
        checkSql();
    }

    private void checkSql() {
        int fieldSize = insert.getFields() != null ? insert.getFields().size() : 0;
        HashSet<Integer> rowValueSize = new HashSet<>();

        insert.getValues().forEach(row -> {
            rowValueSize.add(row.size());
            if (fieldSize > 0 && row.size() != fieldSize) {
                throw new EsSqlParseException("The number of insert fields is different with the number of values, line: " + row);
            }
            if (rowValueSize.size() > 1) {
                throw new EsSqlParseException("Different number of values, the line: " + row);
            }
        });
    }

    @Override
    public ActionRequest explain() {
        if (insert.getFields() == null) {
            setFields();
        }
        BulkRequest bulkRequest = new BulkRequest();
        setIndexRequest(bulkRequest);
        return bulkRequest;
    }

    private void setFields() {
        GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices(insert.getFrom().getIndex());
        try {
            GetMappingsResponse mappingsResponse = client.getClient().indices()
                    .getMapping(getMappingsRequest, RequestOptions.DEFAULT);
            Map<String, MappingMetadata> mappings = mappingsResponse.mappings();
            MappingMetadata mappingMetadata = mappings.get(insert.getFrom().getIndex());
            Map<String, Object> properties = (Map<String, Object>) mappingMetadata.sourceAsMap().get("properties");
            ArrayList<Field> fields = new ArrayList<>();
            insert.setFields(fields);
            extractFieldsRecursive("", properties, fields);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void extractFieldsRecursive(String parentPath,
                                        Map<String, Object> properties,
                                        List<Field> fields) {
        if (properties == null) return;

        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            String fieldName = entry.getKey();
            String fullPath = parentPath.isEmpty() ? fieldName : parentPath + "." + fieldName;
            fields.add(new Field(fullPath));

            Map<String, Object> fieldProps = (Map<String, Object>)
                    ((Map<String, Object>) entry.getValue()).get("properties");
            if (fieldProps != null && !fieldProps.isEmpty()) {
                extractFieldsRecursive(fullPath, fieldProps, fields);
            }
        }
    }


    private void setIndexRequest(BulkRequest bulkRequest) {
        for (List<Value> row : insert.getValues()) {
            IndexRequest indexRequest = new IndexRequest();
            indexRequest.index(insert.getFrom().getIndex());
            Map<String, Object> source = getSource(indexRequest, row);
            indexRequest.source(source);
            bulkRequest.add(indexRequest);
        }
    }

    private Map<String, Object> getSource(IndexRequest indexRequest, List<Value> row) {
        List<Field> fields = insert.getFields();
        HashMap<String, Object> source = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            if (i == insert.getIdPosition()) {
                indexRequest.id(row.get(i).getText().toString());
                continue;
            }
            source.put(fields.get(i).getFieldName(), row.get(i).getText());
        }
        return source;
    }
}

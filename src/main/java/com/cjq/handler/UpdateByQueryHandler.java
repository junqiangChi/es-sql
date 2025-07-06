package com.cjq.handler;

import com.cjq.exception.ElasticsearchExecuteException;
import com.cjq.jdbc.HandlerResult;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.index.reindex.BulkByScrollResponse;

import java.util.ArrayList;
import java.util.List;

public class UpdateByQueryHandler implements ResponseHandler {
    @Override
    public HandlerResult handle(ActionResponse response) {
        BulkByScrollResponse bulkByScrollResponse = (BulkByScrollResponse) response;
        List<BulkItemResponse.Failure> bulkFailures = bulkByScrollResponse.getBulkFailures();
        StringBuilder errorMsg = new StringBuilder();
        errorMsg.append("UpdateByQuery operation failed, ");
        if (bulkFailures.size() > 0) {
            errorMsg.append("[");
            for (int i = 0; i < bulkFailures.size(); i++) {
                errorMsg.append("[doc_id: ").append(bulkFailures.get(i).getId()).append(", ");
                errorMsg.append("status: ").append(bulkFailures.get(i).getStatus()).append(", ");
                errorMsg.append("reason: ").append(bulkFailures.get(i).getMessage()).append("]");
                if (i < bulkFailures.size() - 1){
                    errorMsg.append(", ");
                }
            }
            errorMsg.append("]");
            throw new ElasticsearchExecuteException(errorMsg.toString());
        }
        return new HandlerResult(new ArrayList<>(),new ArrayList<>()).setSuccess(true);
    }
}

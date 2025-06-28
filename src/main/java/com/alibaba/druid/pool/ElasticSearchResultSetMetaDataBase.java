package com.alibaba.druid.pool;

import com.alibaba.druid.util.jdbc.ResultSetMetaDataBase;

import java.util.List;

public class ElasticSearchResultSetMetaDataBase extends ResultSetMetaDataBase {

    public ElasticSearchResultSetMetaDataBase(List<String> headers) {
        ColumnMetaData columnMetaData;
        for (String column : headers) {
            columnMetaData = new ColumnMetaData();
            columnMetaData.setColumnLabel(column);
            columnMetaData.setColumnName(column);
            getColumns().add(columnMetaData);
        }
    }

}

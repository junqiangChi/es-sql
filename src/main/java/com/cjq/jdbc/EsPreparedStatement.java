package com.cjq.jdbc;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;

import java.sql.ResultSet;
import java.sql.SQLException;

public class EsPreparedStatement extends AvaticaStatement {
    protected EsPreparedStatement(AvaticaConnection connection, Meta.StatementHandle h, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        super(connection, h, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    protected EsPreparedStatement(AvaticaConnection connection, Meta.StatementHandle h, int resultSetType, int resultSetConcurrency, int resultSetHoldability, Meta.Signature signature) {
        super(connection, h, resultSetType, resultSetConcurrency, resultSetHoldability, signature);
    }

    @Override
    public Meta.StatementType getStatementType() {
        return super.getStatementType();
    }

    @Override
    protected void executeInternal(String sql) throws SQLException {
        super.executeInternal(sql);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return super.execute(sql);
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return super.executeQuery(sql);
    }

    @Override
    public synchronized void close() throws SQLException {
        super.close();
    }

}

package com.cjq.jdbc;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;

public class EsPreparedStatement extends AvaticaStatement {
    protected EsPreparedStatement(AvaticaConnection connection, Meta.StatementHandle h, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        super(connection, h, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    protected EsPreparedStatement(AvaticaConnection connection, Meta.StatementHandle h, int resultSetType, int resultSetConcurrency, int resultSetHoldability, Meta.Signature signature) {
        super(connection, h, resultSetType, resultSetConcurrency, resultSetHoldability, signature);
    }

}

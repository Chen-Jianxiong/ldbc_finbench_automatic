package org.ldbcouncil.finbench.impls.common.operationhandlers;

import org.ldbcouncil.finbench.driver.*;

public interface SingletonOperationHandler<
        TOperationResult,
        TOperation extends Operation<TOperationResult>,
        TDbConnectionState extends DbConnectionState>
        extends OperationHandler<TOperation, TDbConnectionState> {

    @Override
    void executeOperation(TOperation operation, TDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException;

    String getQueryString(TDbConnectionState state, TOperation operation);

}

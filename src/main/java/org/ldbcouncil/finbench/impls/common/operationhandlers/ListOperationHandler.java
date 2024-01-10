package org.ldbcouncil.finbench.impls.common.operationhandlers;

import org.ldbcouncil.finbench.driver.*;

import java.util.List;

public interface ListOperationHandler<
        TOperationResult,
        TOperation extends Operation<List<TOperationResult>>,
        TDbConnectionState extends DbConnectionState>
        extends OperationHandler<TOperation, TDbConnectionState> {

    @Override
    void executeOperation(TOperation operation, TDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException;

    String getQueryString(TDbConnectionState state, TOperation operation);

}

package org.ldbcouncil.finbench.impls.common.operationhandlers;

import org.ldbcouncil.finbench.driver.*;
import org.ldbcouncil.finbench.driver.workloads.transaction.LdbcNoResult;

import java.util.List;

public interface MultipleUpdateOperationHandler<
        TOperation extends Operation<LdbcNoResult>,
        TDbConnectionState extends DbConnectionState>
        extends OperationHandler<TOperation, TDbConnectionState> {

    @Override
    void executeOperation(TOperation operation, TDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException;

    List<String> getQueryString(TDbConnectionState state, TOperation operation);

}

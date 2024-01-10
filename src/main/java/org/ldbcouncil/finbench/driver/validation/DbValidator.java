package org.ldbcouncil.finbench.driver.validation;

import org.ldbcouncil.finbench.driver.*;
import org.ldbcouncil.finbench.driver.runtime.ConcurrentErrorReporter;

import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.Set;

import static java.lang.String.format;

public class DbValidator {
    /**
     * Validate the database using generated validation parameters.
     *
     * @param validationParameters  Iterator of validation parameters created using 'create_validation' mode
     * @param db                    The database connector
     * @param validationParamsCount Total validation parameters
     * @param workload              The workload to use, e.g. @see org.ldbcouncil.finbench.driver.workloads
     *                              .interactive.LdbcSnbInteractiveWorkload
     * @return DbValidationResult
     * @throws WorkloadException
     */
    public DbValidationResult validate(Iterator<ValidationParam> validationParameters,
                                       Db db,
                                       int validationParamsCount,
                                       Workload workload) throws WorkloadException {
        System.out.println("----");
        DecimalFormat numberFormat = new DecimalFormat("###,###,###,###,###");
        DbValidationResult dbValidationResult = new DbValidationResult(db);
        // 创建简Simple结果记录器，其中包含了并发错误记录器
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ResultReporter resultReporter = new ResultReporter.SimpleResultReporter(errorReporter);

        Set<Class> operationMap = workload.enabledValidationOperations();

        // 记录验证参数 处理、崩溃、不正确、跳过 的数量
        int validationParamsProcessedSoFar = 0;
        int validationParamsCrashedSoFar = 0;
        int validationParamsIncorrectSoFar = 0;
        int validationParamsSkippedSoFar = 0;

        Operation operation = null;
        // 验证全部结果
        while (true) {
            if (null != operation) {
                System.out.println(format(
                    "Processed %s / %s -- Crashed %s -- Incorrect %s -- Skipped %s -- Currently processing %s...",
                    numberFormat.format(validationParamsProcessedSoFar),
                    numberFormat.format(validationParamsCount),
                    numberFormat.format(validationParamsCrashedSoFar),
                    numberFormat.format(validationParamsIncorrectSoFar),
                    numberFormat.format(validationParamsSkippedSoFar),
                    operation.getClass().getSimpleName()
                ));
                System.out.flush();
            }

            if (!validationParameters.hasNext()) {
                break;
            }

            ValidationParam validationParam = validationParameters.next();
            operation = validationParam.operation();
            Object expectedOperationResult = validationParam.operationResult();

            // Skip disabled operation
            if (!operationMap.contains(operation.getClass())) {
                validationParamsSkippedSoFar++;
                continue;
            }

            // 实例化操作处理器
            OperationHandlerRunnableContext handlerRunner;
            try {
                handlerRunner = db.getOperationHandlerRunnableContext(operation);
            } catch (Throwable e) {
                dbValidationResult.reportMissingHandlerForOperation(operation);
                continue;
            }

            try {
                OperationHandler handler = handlerRunner.operationHandler();
                DbConnectionState dbConnectionState = handlerRunner.dbConnectionState();
                // 获取不同操作的cypher语句，并执行事务，记录结果到resultReporter中
                handler.executeOperation(operation, dbConnectionState, resultReporter);
                if (null == resultReporter.result()) {
                    throw new DbException(
                        format("Db returned null result for: %s", operation.getClass().getSimpleName()));
                }
            } catch (Throwable e) {
                // Not necessary, but perhaps useful for debugging
                e.printStackTrace();
                validationParamsCrashedSoFar++;
                dbValidationResult
                    .reportUnableToExecuteOperation(operation, ConcurrentErrorReporter.stackTraceToString(e));
                continue;
            } finally {
                validationParamsProcessedSoFar++;
                handlerRunner.cleanup();
            }

            Object actualOperationResult = resultReporter.result();

            // 验证结果
            if (!actualOperationResult.equals(expectedOperationResult)) {
                validationParamsIncorrectSoFar++;
                dbValidationResult
                    .reportIncorrectResultForOperation(operation, expectedOperationResult, actualOperationResult);
                continue;
            }

            dbValidationResult.reportSuccessfulExecution(operation);
        }
        System.out.println("\n----");
        return dbValidationResult;
    }
}

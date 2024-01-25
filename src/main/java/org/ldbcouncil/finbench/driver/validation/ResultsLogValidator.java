package org.ldbcouncil.finbench.driver.validation;

import org.ldbcouncil.finbench.driver.csv.simple.SimpleCsvFileReader;
import org.ldbcouncil.finbench.driver.runtime.metrics.OperationMetricsSnapshot;
import org.ldbcouncil.finbench.driver.runtime.metrics.WorkloadResultsSnapshot;
import org.ldbcouncil.finbench.driver.temporal.TemporalUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.ldbcouncil.finbench.driver.validation.ResultsLogValidationResult.ValidationErrorType;

/**
 * ResultsLogValidator.java
 * This class computes and validates the results of the benchmark. Compute reads the result file
 * and records any delayed operation. Validate checks from the computed summary if it exceeds the
 * threshold.
 */
public class ResultsLogValidator {
    private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();

    /**
     * Validates the result from the benchmark. Checks the amount of
     *
     * @param summary                 Summary of delayed operations previously computed
     * @param tolerances              The resultsLogValidationTolerances object including the tolerated delay count.
     * @param recordDelayedOperations Record the late operations even when the total late operations are under the
     *                                threshold
     * @return ResultsLogValidationResult containing the list of delayed operations
     */
    public ResultsLogValidationResult validate(
            ResultsLogValidationSummary summary,
            ResultsLogValidationTolerances tolerances,
            boolean recordDelayedOperations,
            WorkloadResultsSnapshot workloadResults) {

        ResultsLogValidationResult result = new ResultsLogValidationResult();

        Map<String, Long> operationCountPerTypeMap = new HashMap<>();
        for (OperationMetricsSnapshot metric : workloadResults.allMetrics()) {
            operationCountPerTypeMap.put(metric.name(), metric.count());
        }

        for (String operationType : summary.excessiveDelayCountPerType().keySet()) {
            // 单独计算各个类型的可容忍延迟计数
            Long cnt = operationCountPerTypeMap.get(operationType);
            long allowedLateOperations = Math.round(
                    (cnt == null ? 0 : cnt) * tolerances.toleratedExcessiveDelayCountPercentage());
            if (recordDelayedOperations
                    && summary.excessiveDelayCountPerType().get(operationType) > allowedLateOperations) {
                result.aboveThreshold();
                result.addError(
                        ValidationErrorType.TOO_MANY_LATE_OPERATIONS,
                        format("Late Count for %s (%s) > (%s) Tolerated Late Count",
                                operationType,
                                summary.excessiveDelayCountPerType().get(operationType),
                                allowedLateOperations
                        )
                );
            }
        }
        return result;
    }

    /**
     * validate方法自动化测试版
     */
    public ResultsLogValidationResult validateAutomatic(
            ResultsLogValidationSummary summary,
            ResultsLogValidationTolerances tolerances,
            WorkloadResultsSnapshot workloadResults) {

        ResultsLogValidationResult result = new ResultsLogValidationResult();

        Map<String, Long> operationCountPerTypeMap = new HashMap<>();
        for (OperationMetricsSnapshot metric : workloadResults.allMetrics()) {
            operationCountPerTypeMap.put(metric.name(), metric.count());
        }

        for (String operationType : summary.excessiveDelayCountPerType().keySet()) {
            // 单独计算各个类型的可容忍延迟计数
            Long cnt = operationCountPerTypeMap.get(operationType);
            long allowedLateOperations = Math.round(
                    (cnt == null ? 0 : cnt) * tolerances.toleratedExcessiveDelayCountPercentage());
            if (summary.excessiveDelayCountPerType().get(operationType) > allowedLateOperations) {
                result.addError(
                        ValidationErrorType.TOO_MANY_LATE_OPERATIONS,
                        format("Late Count for %s (%s) > (%s) Tolerated Late Count",
                                operationType,
                                summary.excessiveDelayCountPerType().get(operationType),
                                allowedLateOperations
                        )
                );
            }
        }

        result.setThroughput(workloadResults.throughput());
        result.setOperationCount(workloadResults.totalOperationCount());
        // 判断是否超时
        if (summary.excessiveDelayCount() > tolerances.toleratedExcessiveDelayCount()) {
            result.aboveThreshold();
        }
        // 计算及时率
        result.computeOnTimeRatio(summary.excessiveDelayCount());
        return result;
    }

    /**
     * Loads the benchmark result file and uses the ResultsLogValidationSummaryCalculator to record delayed
     * operations.
     *
     * @param resultsLog                     The File object to the operation result log CSV-file.
     * @param excessiveDelayThresholdAsMilli The delay threshold when an operation is considered delayed.
     * @return Summary of the delayed operations in a ResultsLogValidationSummary object
     * @throws ValidationException When the result CSV file could not be opened or invalid delay is computed.
     */
    public ResultsLogValidationSummary compute(File resultsLog, long excessiveDelayThresholdAsMilli)
            throws ValidationException {
        // 读取基准测试结果文件，计算最大延迟 max(实际开始时间 - 预计开始时间)
        long maxDelayAsMilli = maxDelayAsMilli(resultsLog);
        ResultsLogValidationSummaryCalculator calculator = new ResultsLogValidationSummaryCalculator(
                maxDelayAsMilli,
                excessiveDelayThresholdAsMilli
        );

        try (SimpleCsvFileReader reader = new SimpleCsvFileReader(
                resultsLog,
                SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING)) {
            // skip headers
            reader.next();
            while (reader.hasNext()) {
                String[] row = reader.next();
                String operationType = row[0];
                long scheduledStartTimeAsMilli = Long.parseLong(row[1]);
                long actualStartTimeAsMilli = Long.parseLong(row[2]);
                // duration
                // result code
                long delayAsMilli = actualStartTimeAsMilli - scheduledStartTimeAsMilli;
                // 记录延迟（毫秒）到直方图，如果该次操作延迟超过阈值，则还会将此次操作记录在对应操作类型的直方图中，并计数
                calculator.recordDelay(operationType, delayAsMilli);
            }
        } catch (FileNotFoundException e) {
            throw new ValidationException(format("Error opening results log: %s", resultsLog.getAbsolutePath()), e);
        }
        // Create summary
        return calculator.snapshot();
    }

    /**
     * Calculates the maximum delay in the results used to place results in the Histogram object.
     *
     * @param resultsLog The File object to the operation result log CSV-file.
     * @return maximum delay found in the result file.
     * @throws ValidationException When the delay is invalid (negative)
     */
    private long maxDelayAsMilli(File resultsLog) throws ValidationException {
        long maxDelayAsMilli = 0;
        try (SimpleCsvFileReader reader = new SimpleCsvFileReader(
                resultsLog,
                SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING)) {
            // skip headers
            reader.next();
            while (reader.hasNext()) {
                String[] row = reader.next();
                // operation type
                long scheduledStartTimeAsMilli = Long.parseLong(row[1]);
                long actualStartTimeAsMilli = Long.parseLong(row[2]);
                // duration
                // result code
                long delayAsMilli = actualStartTimeAsMilli - scheduledStartTimeAsMilli;
                if (delayAsMilli < 0) {
                    throw new ValidationException(
                            format("Delay can not be negative\n" + "Delay: %s (ms) / %s\n"
                                            + "Scheduled Start Time: %s (ms) / %s\n" + "Actual Start Time: %s (ms) / %s",
                                    delayAsMilli,
                                    TEMPORAL_UTIL.milliDurationToString(delayAsMilli),
                                    scheduledStartTimeAsMilli,
                                    TEMPORAL_UTIL.milliTimeToTimeString(scheduledStartTimeAsMilli),
                                    actualStartTimeAsMilli,
                                    TEMPORAL_UTIL.milliTimeToTimeString(actualStartTimeAsMilli)
                            )
                    );
                }
                if (delayAsMilli > maxDelayAsMilli) {
                    maxDelayAsMilli = delayAsMilli;
                }
            }
        } catch (FileNotFoundException e) {
            throw new ValidationException(format("Error opening results log: %s", resultsLog.getAbsolutePath()), e);
        }
        return maxDelayAsMilli;
    }
}

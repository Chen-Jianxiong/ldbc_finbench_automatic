package org.ldbcouncil.finbench.driver.validation;

import org.HdrHistogram.Histogram;

import java.util.HashMap;
import java.util.Map;

/**
 * ResultsLogValidationSummaryCalculator.java
 * Stores per operation the delayed operations into a histogram (method RecordDelay)
 * and creates a summary of the delayed operations using (method snapshot)
 */
class ResultsLogValidationSummaryCalculator {
    private final Histogram delays;
    private final Map<String, Histogram> delaysPerType;
    private final Map<String, Long> delaysAboveThresholdPerType;
    private final long maxDelayAsMilli;
    private final long excessiveDelayThresholdAsMilli;
    private long delaysAboveThreshold;

    ResultsLogValidationSummaryCalculator(long maxDelayAsMilli, long excessiveDelayThresholdAsMilli) {
        this.maxDelayAsMilli = maxDelayAsMilli;
        this.excessiveDelayThresholdAsMilli = excessiveDelayThresholdAsMilli;
        this.delays = new Histogram(1, Math.max(2, maxDelayAsMilli), 5);
        this.delaysPerType = new HashMap<>();
        this.delaysAboveThresholdPerType = new HashMap<>();
        this.delaysAboveThreshold = 0;
    }

    /**
     * Stores the operation in Histogram object if the delay exceeded the threshold.
     *
     * @param operationType The operation type
     * @param delayAsMilli  The delay in milliseconds for the operation
     */
    void recordDelay(String operationType, long delayAsMilli) {
        // 所有操作的延迟记录
        delays.recordValue(delayAsMilli);

        // 不同操作类型的直方图 map
        Histogram delayForType = delaysPerType.get(operationType);
        if (null == delayForType) {
            delayForType = new Histogram(1, Math.max(2, maxDelayAsMilli), 5);
            delaysPerType.put(operationType, delayForType);
        }
        delayForType.recordValue(delayAsMilli);

        // 不同操作类型超过阈值的数量
        Long delaysAboveThresholdForType = delaysAboveThresholdPerType.get(operationType);
        if (null == delaysAboveThresholdForType) {
            delaysAboveThresholdForType = 0L;
            delaysAboveThresholdPerType.put(operationType, delaysAboveThresholdForType);
        }
        // 如果该次操作延迟超过阈值(1秒），则为该操作类型计数
        if (delayAsMilli > excessiveDelayThresholdAsMilli) {
            delaysAboveThreshold++;
            delaysAboveThresholdPerType.put(operationType, delaysAboveThresholdForType + 1);
        }
    }

    /**
     * Creates a summary with statistics of the delays
     *
     * @return ResultsLogValidationSummary object
     */
    ResultsLogValidationSummary snapshot() {
        Map<String, Long> minDelayAsMilliPerType = new HashMap<>();
        Map<String, Long> maxDelayAsMilliPerType = new HashMap<>();
        Map<String, Long> meanDelayAsMilliPerType = new HashMap<>();
        for (String operationType : delaysPerType.keySet()) {
            minDelayAsMilliPerType.put(
                operationType,
                delaysPerType.get(operationType).getMinValue()
            );
            maxDelayAsMilliPerType.put(
                operationType,
                delaysPerType.get(operationType).getMaxValue()
            );
            meanDelayAsMilliPerType.put(
                operationType,
                Math.round(Math.ceil(delaysPerType.get(operationType).getMean()))
            );
        }
        return new ResultsLogValidationSummary(
            excessiveDelayThresholdAsMilli,
            delaysAboveThreshold,
            delaysAboveThresholdPerType,
            delays.getMinValue(),
            delays.getMaxValue(),
            Math.round(delays.getMean()),
            minDelayAsMilliPerType,
            maxDelayAsMilliPerType,
            meanDelayAsMilliPerType
        );
    }
}

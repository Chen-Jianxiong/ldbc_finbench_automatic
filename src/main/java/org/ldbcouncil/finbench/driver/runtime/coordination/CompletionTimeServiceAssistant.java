package org.ldbcouncil.finbench.driver.runtime.coordination;

import org.ldbcouncil.finbench.driver.runtime.ConcurrentErrorReporter;
import org.ldbcouncil.finbench.driver.runtime.scheduling.Spinner;
import org.ldbcouncil.finbench.driver.temporal.TimeSource;

import java.util.List;

public class CompletionTimeServiceAssistant {
    public void writeInitiatedAndCompletedTimesToAllWriters(
        CompletionTimeService completionTimeService,
        long timeAsMilli) throws CompletionTimeException {
        List<CompletionTimeWriter> writers = completionTimeService.getAllWriters();
        for (CompletionTimeWriter writer : writers) {
            writer.submitInitiatedTime(timeAsMilli);
            writer.submitCompletedTime(timeAsMilli);
        }
    }

    public boolean waitForCompletionTime(
        TimeSource timeSource,
        long completionTimeToWaitForAsMilli,
        long timeoutDurationAsMilli,
        CompletionTimeService completionTimeService,
        ConcurrentErrorReporter errorReporter) throws CompletionTimeException {
        long sleepDurationAsMilli = 100;
        long timeoutTimeAsMilli = timeSource.nowAsMilli() + timeoutDurationAsMilli;
        // 循环获取完成时间（操作执行完成），超时时间为 5 秒
        while (timeSource.nowAsMilli() < timeoutTimeAsMilli) {
            long currentCompletionTimeAsMilli = completionTimeService.completionTimeAsMilli();
            if (-1 == currentCompletionTimeAsMilli) {
                continue;
            }
            // 若时间已经超过设定的等待时间（不是超时时间），就直接返回 true
            if (completionTimeToWaitForAsMilli <= currentCompletionTimeAsMilli) {
                return true;
            }
            if (errorReporter.errorEncountered()) {
                throw new CompletionTimeException("Encountered error while waiting for CT");
            }
            // 若还未到设定的等待时间（提前完成），则会休眠100毫秒（减少旋转时的CPU负载）
            Spinner.powerNap(sleepDurationAsMilli);
        }
        return false;
    }

    public SynchronizedCompletionTimeService newSynchronizedCompletionTimeService() throws CompletionTimeException {
        return new SynchronizedCompletionTimeService();
    }

    public ThreadedQueuedCompletionTimeService newThreadedQueuedCompletionTimeService(
        TimeSource timeSource,
        ConcurrentErrorReporter errorReporter) throws CompletionTimeException {
        return new ThreadedQueuedCompletionTimeService(timeSource, errorReporter);
    }
}

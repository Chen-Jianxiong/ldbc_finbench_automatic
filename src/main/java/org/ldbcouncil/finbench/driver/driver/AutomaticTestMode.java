package org.ldbcouncil.finbench.driver.driver;


import com.google.common.base.Charsets;
import org.ldbcouncil.finbench.driver.Db;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.Workload;
import org.ldbcouncil.finbench.driver.WorkloadException;
import org.ldbcouncil.finbench.driver.WorkloadStreams;
import org.ldbcouncil.finbench.driver.control.ConsoleAndFileDriverConfiguration;
import org.ldbcouncil.finbench.driver.control.ControlService;
import org.ldbcouncil.finbench.driver.generator.GeneratorFactory;
import org.ldbcouncil.finbench.driver.generator.RandomDataGeneratorFactory;
import org.ldbcouncil.finbench.driver.log.LoggingService;
import org.ldbcouncil.finbench.driver.runtime.ConcurrentErrorReporter;
import org.ldbcouncil.finbench.driver.runtime.DefaultQueues;
import org.ldbcouncil.finbench.driver.runtime.SimpleResultsLogWriter;
import org.ldbcouncil.finbench.driver.runtime.WorkloadRunner;
import org.ldbcouncil.finbench.driver.runtime.coordination.CompletionTimeException;
import org.ldbcouncil.finbench.driver.runtime.coordination.CompletionTimeService;
import org.ldbcouncil.finbench.driver.runtime.coordination.CompletionTimeServiceAssistant;
import org.ldbcouncil.finbench.driver.runtime.coordination.CompletionTimeWriter;
import org.ldbcouncil.finbench.driver.runtime.metrics.DisruptorSbeMetricsService;
import org.ldbcouncil.finbench.driver.runtime.metrics.JsonWorkloadMetricsFormatter;
import org.ldbcouncil.finbench.driver.runtime.metrics.MetricsCollectionException;
import org.ldbcouncil.finbench.driver.runtime.metrics.MetricsManager;
import org.ldbcouncil.finbench.driver.runtime.metrics.MetricsService;
import org.ldbcouncil.finbench.driver.runtime.metrics.NullResultsLogWriter;
import org.ldbcouncil.finbench.driver.runtime.metrics.ResultsLogWriter;
import org.ldbcouncil.finbench.driver.runtime.metrics.WorkloadResultsSnapshot;
import org.ldbcouncil.finbench.driver.runtime.metrics.WorkloadStatusSnapshot;
import org.ldbcouncil.finbench.driver.temporal.TemporalUtil;
import org.ldbcouncil.finbench.driver.temporal.TimeSource;
import org.ldbcouncil.finbench.driver.util.ClassLoaderHelper;
import org.ldbcouncil.finbench.driver.util.Tuple3;
import org.ldbcouncil.finbench.driver.validation.ResultsLogValidationResult;
import org.ldbcouncil.finbench.driver.validation.ResultsLogValidationSummary;
import org.ldbcouncil.finbench.driver.validation.ResultsLogValidationTolerances;
import org.ldbcouncil.finbench.driver.validation.ResultsLogValidator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;



/**
 * 描述：自动测试模式，采用二分方法来测试出适合当前机器的配置参数。
 * 备注：
 */
public class AutomaticTestMode implements DriverMode<Object> {
    private final ControlService controlService;
    private final TimeSource timeSource;
    private final LoggingService loggingService;
    private final long randomSeed;
    private final TemporalUtil temporalUtil;
    private final ResultsDirectory resultsDirectory;

    private Workload workload = null;
    private Db database = null;
    private MetricsService metricsService = null;
    private CompletionTimeService completionTimeService = null;
    private WorkloadRunner workloadRunner = null;
    private ResultsLogWriter resultsLogWriter = null;

    public static double throughput;

    public AutomaticTestMode(
            ControlService controlService,
            TimeSource timeSource,
            long randomSeed) throws DriverException {
        this.controlService = controlService;
        this.timeSource = timeSource;
        this.loggingService = controlService.loggingServiceFactory().loggingServiceFor(getClass().getSimpleName());
        this.randomSeed = randomSeed;
        this.temporalUtil = new TemporalUtil();
        this.resultsDirectory = new ResultsDirectory(controlService.configuration());
    }

    /*
    TODO clientMode.init()
    TODO clientMode
     */
    public WorkloadStatusSnapshot status() throws MetricsCollectionException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void init() throws DriverException {
        loggingService.info("Driver Configuration");
        loggingService.info(controlService.toString());
    }

    @Override
    public Object startExecutionAndAwaitCompletion() throws DriverException {
        double l = 1E-9;
        double r = 1;
        // 记录最后成功的一次的吞吐量
        double throughput = 0;
        // 快速预估阶段
        loggingService.info("-----------------------------------快速预估阶段-------------------------------------------");
        while (r - l >= 1E-5) {
            // 第一次先尝试使用配置指定的tcr运行
            double tcr = controlService.configuration().timeCompressionRatio();
            loggingService.info("新的一轮：时间压缩比：" + tcr);
            if (validationTest(5 * 60 * 1000)) {
                r = tcr;
                throughput = AutomaticTestMode.throughput;
            } else {
                l = tcr;
            }
            tcr = l + (r - l) / 2;
            ((ConsoleAndFileDriverConfiguration) controlService.configuration()).timeCompressionRatio = tcr;
        }

        // 精确调参阶段，以防止产时间运行时机器损耗而导致的性能下降
        loggingService.info("-----------------------------------精确调参阶段-------------------------------------------");
        l = throughput;
        r = Math.min(l * 5, 1);
        boolean succeed = false;
        while (r - l >= 1E-5) {
            double tcr = l + (r - l) / 2;
            loggingService.info("新的一轮：时间压缩比：" + tcr);
            ((ConsoleAndFileDriverConfiguration) controlService.configuration()).timeCompressionRatio = tcr;
            if (validationTest(-1)) {
                r = tcr;
                succeed = true;
                throughput = AutomaticTestMode.throughput;
            } else {
                l = tcr;
                succeed = false;
            }
        }
        // 如果找到最后的l没有成功，则更换成最后成功的那一次
        if (!succeed) {
            ((ConsoleAndFileDriverConfiguration) controlService.configuration()).timeCompressionRatio = r;
            // 就不必再执行了，因为此时机器状态已经下滑
            // validationTest(-1);
        }
        return throughput;
    }

    /**
     * 执行一次配置参数测试
     *
     * @return 一个boolean值，代表此次测试是否校验通过（延迟数小于阈值）
     */
    private boolean validationTest(int milli) throws DriverException {
        List<ResultsLogValidationResult.ValidationError> errors = executionAndAwaitCompletion(milli);
        if (errors.isEmpty()) {
            loggingService.info("\n----------------------------------此次测试校验通过----------------------------\n"
                    + "----------------------------throughput: " + throughput + "-----------------------------\n"
                    + "----------------------------------tcr: " + controlService.configuration().timeCompressionRatio()
                    + "-----------------------------");
            return true;
        } else {
            loggingService.info("\n----------------------------------此次测试校验失败----------------------------\n"
                    + "-----------------------------throughput: " + throughput + "-----------------------------\n"
                    + "----------------------------------tcr: " + controlService.configuration().timeCompressionRatio()
                    + "----------------------------");
            return false;
        }
    }

    public List<ResultsLogValidationResult.ValidationError> executionAndAwaitCompletion(int milli)
            throws DriverException {
        List<ResultsLogValidationResult.ValidationError> error = null;
        if (controlService.configuration().warmupCount() > 0) {
            loggingService.info("\n"
                    + " --------------------\n"
                    + " --- Warmup Phase ---\n"
                    + " --------------------");
            doInit(true);
            error = doExecute(true, milli);
            try {
                // TODO remove in future
                // This is necessary to clear the runnable context pool
                // As objects in the pool would otherwise hold references to services used during warmup
                loggingService.info("reInit database...");
                database.reInitTest();
            } catch (DbException e) {
                throw new DriverException(format("Error reinitializing DB: %s", database.getClass().getName()), e);
            }
        } else {
            loggingService.info("\n"
                    + " ---------------------------------\n"
                    + " --- No Warmup Phase Requested ---\n"
                    + " ---------------------------------");
        }
        if (milli == -1) {
            loggingService.info("\n"
                    + " -----------------\n"
                    + " --- Run Phase ---\n"
                    + " -----------------");
            doInit(false);
            error = doExecute(false, -1);
            try {
                // TODO remove in future
                // This is necessary to clear the runnable context pool
                // As objects in the pool would otherwise hold references to services used during warmup
                loggingService.info("reInit database...");
                database.reInitTest();
            } catch (DbException e) {
                throw new DriverException(format("Error reinitializing DB: %s", database.getClass().getName()), e);
            }

//            try {
//                loggingService.info("Shutting down database connector...");
//                Instant dbShutdownStart = Instant.now();
//                database.close();
//                Duration shutdownDuration = Duration.between(dbShutdownStart, Instant.now());
//                loggingService.info("Database connector shutdown successfully in: " + shutdownDuration);
//            } catch (IOException e) {
//                throw new DriverException("Error shutting down database", e);
//            }
//            loggingService.info("Workload completed successfully");
        }
        return error;
    }

    private void doInit(boolean warmup) throws DriverException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(randomSeed));

        //  ================================
        //  ===  Results Log CSV Writer  ===
        //  ================================
        File resultsLog = resultsDirectory.getOrCreateResultsLogFile(warmup);
        try {
            // 如果 resultLog 不为空，则创建文件、写入HEADER列标题
            resultsLogWriter = (null == resultsLog)
                    ? new NullResultsLogWriter()
                    : new SimpleResultsLogWriter(
                    resultsLog,
                    controlService.configuration().timeUnit(),
                    controlService.configuration().flushLog());
        } catch (IOException e) {
            throw new DriverException(
                    format("Error creating results log writer for: %s", resultsLog.getAbsolutePath()), e);
        }

        //  ------------------
        //  ---  Workload  ---
        //  ------------------
        loggingService.info("Scanning workload streams to calculate their limits...");

        // 指定需要跳过的操作数，可通过skip配置指定
        // 正式运行阶段会跳过预热执行的操作数
        long offset = (warmup)
                ? controlService.configuration().skipCount()
                : controlService.configuration().skipCount() + controlService.configuration().warmupCount();
        // 本次执行操作的界限。（并不是总数，而是执行到第limit个操作数停止）
        long limit = (warmup)
                ? controlService.configuration().warmupCount()
                : controlService.configuration().operationCount();

        // 初始化workload，并获取异步操作流（Writes、ComplexRead、SimpleRead）
        WorkloadStreams workloadStreams;
        long minimumTimeStamp;
        try {
            boolean returnStreamsWithDbConnector = true;
            Tuple3<WorkloadStreams, Workload, Long> streamsAndWorkloadAndMinimumTimeStamp =
                    WorkloadStreams.createNewWorkloadWithOffsetAndLimitedWorkloadStreams(
                            controlService.configuration(),
                            gf,
                            returnStreamsWithDbConnector,
                            offset,
                            limit,
                            controlService.loggingServiceFactory()
                    );
            workloadStreams = streamsAndWorkloadAndMinimumTimeStamp._1();
            workload = streamsAndWorkloadAndMinimumTimeStamp._2();
            minimumTimeStamp = streamsAndWorkloadAndMinimumTimeStamp._3();
        } catch (Exception e) {
            throw new DriverException(format("Error loading workload class: %s",
                    controlService.configuration().workloadClassName()), e);
        }
        loggingService.info(format("Loaded workload: %s", workload.getClass().getName()));

        loggingService.info(format("Retrieving workload stream: %s", workload.getClass().getSimpleName()));
        controlService.setWorkloadStartTimeAsMilli(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5));
        WorkloadStreams timeMappedWorkloadStreams;
        try {
            timeMappedWorkloadStreams = WorkloadStreams.timeOffsetAndCompressWorkloadStreams(
                    workloadStreams,
                    controlService.workloadStartTimeAsMilli(),
                    controlService.configuration().timeCompressionRatio(),
                    gf
            );
        } catch (WorkloadException e) {
            throw new DriverException("Error while retrieving operation stream for workload", e);
        }

        //  ---------------=
        //  ---==  DB  ---==
        //  ---------------=
        if (null == database) {
            try {
                database = ClassLoaderHelper.loadDb(controlService.configuration().dbClassName());
                database.init(
                        controlService.configuration().asMap(),
                        controlService.loggingServiceFactory().loggingServiceFor(database.getClass().getSimpleName()),
                        workload.operationTypeToClassMapping()
                );
            } catch (DbException e) {
                throw new DriverException(
                        format("Error initializing DB: %s", controlService.configuration().dbClassName()), e);
            }
            loggingService.info(format("Loaded DB: %s", database.getClass().getName()));
        }

        //  ------------------------
        //  ---  Metrics Service  ==
        //  ------------------------
        try {
            // TODO create metrics service factory so different ones can be easily created
            // Disruptor是用于JVM中多个线程之间的高效内存消息队列
            metricsService = new DisruptorSbeMetricsService(
                    timeSource,
                    errorReporter,
                    controlService.configuration().timeUnit(),
                    DisruptorSbeMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO,
                    resultsLogWriter,
                    workload.operationTypeToClassMapping(),
                    controlService.loggingServiceFactory()
            );
        } catch (MetricsCollectionException e) {
            throw new DriverException("Error creating metrics service", e);
        }

        //  ---------------------------------
        //  ---  Completion Time Service  ---
        //  ---------------------------------
        CompletionTimeServiceAssistant completionTimeServiceAssistant = new CompletionTimeServiceAssistant();
        try {
            completionTimeService =
                    completionTimeServiceAssistant.newThreadedQueuedCompletionTimeService(
                            timeSource,
                            errorReporter
                    );
        } catch (CompletionTimeException e) {
            throw new DriverException("Error instantiating Completion Time Service", e);
        }

        //  ------------------------
        //  ---  Workload Runner  ==
        //  ------------------------
        loggingService.info(format("Instantiating %s", WorkloadRunner.class.getSimpleName()));
        try {
            int operationHandlerExecutorsBoundedQueueSize = DefaultQueues.DEFAULT_BOUND_1000;
            workloadRunner = new WorkloadRunner(
                    timeSource,
                    database,
                    timeMappedWorkloadStreams,
                    metricsService,
                    errorReporter,
                    completionTimeService,
                    controlService.loggingServiceFactory(),
                    controlService.configuration().threadCount(),
                    controlService.configuration().statusDisplayIntervalAsSeconds(),
                    controlService.configuration().spinnerSleepDurationAsMilli(),
                    controlService.configuration().ignoreScheduledStartTimes(),
                    operationHandlerExecutorsBoundedQueueSize);
        } catch (Exception e) {
            throw new DriverException(format("Error instantiating %s", WorkloadRunner.class.getSimpleName()), e);
        }

        //  ------------------------------------------=
        //  ---  Initialize Completion Time Service  ==
        //  ------------------------------------------=
        // TODO note, this MUST be done after creation of Workload Runner because Workload Runner creates the
        // TODO "writers" for completion time service (refactor this mess at some stage)
        try {
            if (completionTimeService.getAllWriters().isEmpty()) {
                // There are no completion time writers, CT would never advance or be non-null,
                // set to max so nothing ever waits on it
                long nearlyMaxPossibleTimeAsMilli = Long.MAX_VALUE - 1;
                long maxPossibleTimeAsMilli = Long.MAX_VALUE;
                // Create a writer to use for advancing CT
                CompletionTimeWriter completionTimeWriter = completionTimeService.newCompletionTimeWriter();
                completionTimeWriter.submitInitiatedTime(nearlyMaxPossibleTimeAsMilli);
                completionTimeWriter.submitCompletedTime(nearlyMaxPossibleTimeAsMilli);
                completionTimeWriter.submitInitiatedTime(maxPossibleTimeAsMilli);
                completionTimeWriter.submitCompletedTime(maxPossibleTimeAsMilli);
            } else {
                // There are some completion time writers, initialize them to lowest time stamp in workload
                completionTimeServiceAssistant
                        .writeInitiatedAndCompletedTimesToAllWriters(completionTimeService, minimumTimeStamp - 1);
                completionTimeServiceAssistant
                        .writeInitiatedAndCompletedTimesToAllWriters(completionTimeService, minimumTimeStamp);
                boolean completionTimeAdvancedToDesiredTime =
                        completionTimeServiceAssistant.waitForCompletionTime(
                                timeSource,
                                minimumTimeStamp - 1,
                                TimeUnit.SECONDS.toMillis(5),
                                completionTimeService,
                                errorReporter
                        );
                long completionTimeWaitTimeoutDurationAsMilli = TimeUnit.SECONDS.toMillis(5);
                if (!completionTimeAdvancedToDesiredTime) {
                    throw new DriverException(
                            format(
                                    "Timed out [%s] while waiting for completion time to advance to workload "
                                            + "start time\nCurrent CT: %s\nWaiting For CT: %s",
                                    completionTimeWaitTimeoutDurationAsMilli,
                                    completionTimeService.completionTimeAsMilli(),
                                    controlService.workloadStartTimeAsMilli())
                    );
                }
                loggingService.info("CT: " + temporalUtil
                        .milliTimeToDateTimeString(completionTimeService.completionTimeAsMilli()) + " / "
                        + completionTimeService.completionTimeAsMilli());
            }
        } catch (CompletionTimeException e) {
            throw new DriverException(
                    "Error while writing initial initiated and completed times to Completion Time Service", e);
        }
    }

    private List<ResultsLogValidationResult.ValidationError> doExecute(boolean warmup, int milli) throws DriverException {
        // 关闭 workload、完成时间服务、指标服务
        try {
            ConcurrentErrorReporter errorReporter = null;
            if (milli == -1) {
                // 正常执行，走EXECUTE_BENCHMARK的流程
                errorReporter = workloadRunner.getFuture().get();
            } else {
                // 启动workloadRunner线程，主线程睡眠milli的时间
                errorReporter = workloadRunner.getFuture(milli);
            }
            loggingService.info("Shutting down workload...");
            workload.close();
            if (errorReporter.errorEncountered()) {
                throw new DriverException("Error running workload\n" + errorReporter.toString());
            }
        } catch (Exception e) {
            throw new DriverException("Error running workload", e);
        }

        loggingService.info("Shutting down completion time service...");
        try {
            completionTimeService.shutdown();
        } catch (CompletionTimeException e) {
            throw new DriverException("Error during shutdown of completion time service", e);
        }

        loggingService.info("Shutting down metrics collection service...");
        WorkloadResultsSnapshot workloadResults;
        try {
            workloadResults = metricsService.getWriter().results();
            metricsService.shutdown();
        } catch (MetricsCollectionException e) {
            throw new DriverException("Error during shutdown of metrics collection service", e);
        }

        try {
            // 将度量指标结果输出到日志
            if (warmup) {
                loggingService.summaryResult(workloadResults);
            } else {
                loggingService.detailedResult(workloadResults);
            }
            // 输出指标服务结果到 XX-results.json文件
            if (resultsDirectory.exists()) {
                File resultsSummaryFile = resultsDirectory.getOrCreateResultsSummaryFile(warmup);
                loggingService.info(
                        format("Exporting workload metrics to %s...", resultsSummaryFile.getAbsolutePath())
                );
                MetricsManager.export(workloadResults,
                        new JsonWorkloadMetricsFormatter(),
                        new FileOutputStream(resultsSummaryFile),
                        Charsets.UTF_8
                );
                // 输出配置信息到 XX-configuration.properties文件
                File configurationFile = resultsDirectory.getOrCreateConfigurationFile(warmup);
                Files.write(
                        configurationFile.toPath(),
                        controlService.configuration().toPropertiesString().getBytes(StandardCharsets.UTF_8)
                );
                resultsLogWriter.close();
                if (!controlService.configuration().ignoreScheduledStartTimes()) {
                    loggingService.info("Validating workload results...");
                    // TODO make this feature accessible directly
                    ResultsLogValidator resultsLogValidator = new ResultsLogValidator();
                    // 计算可容忍延迟操作的总数
                    ResultsLogValidationTolerances resultsLogValidationTolerances =
                            workload.resultsLogValidationTolerances(controlService.configuration(), warmup);

                    // 统计延迟操作数，生成一个直方图简易快照，返回每种操作类型的最小、最大、平均延迟
                    ResultsLogValidationSummary resultsLogValidationSummary = resultsLogValidator.compute(
                            resultsDirectory.getOrCreateResultsLogFile(warmup),
                            resultsLogValidationTolerances.excessiveDelayThresholdAsMilli()
                    );
                    // 输出统计延迟操作数结果到XX-validation.json文件
                    File resultsValidationFile = resultsDirectory.getOrCreateResultsValidationFile(warmup);
                    loggingService.info(
                            format("Exporting workload results validation to: %s",
                                    resultsValidationFile.getAbsolutePath())
                    );
                    Files.write(
                            resultsValidationFile.toPath(),
                            resultsLogValidationSummary.toJson().getBytes(StandardCharsets.UTF_8)
                    );
                    // TODO export result
                    // 验证基准测试的结果
                    ResultsLogValidationResult validationResult = resultsLogValidator.validate(
                            resultsLogValidationSummary,
                            resultsLogValidationTolerances,
                            controlService.configuration().recordDelayedOperations(),
                            workloadResults
                    );
                    loggingService.info(validationResult.getScheduleAuditResult(
                            controlService.configuration().recordDelayedOperations()
                    ));
//                    Files.write(
//                        resultsValidationFile.toPath(),
//                        resultsLogValidationSummary.toJson().getBytes(StandardCharsets.UTF_8)
//                    );
                    return validationResult.errors();
                }
            }
        } catch (Exception e) {
            throw new DriverException("Could not export workload metrics", e);
        }
        return null;
    }
}


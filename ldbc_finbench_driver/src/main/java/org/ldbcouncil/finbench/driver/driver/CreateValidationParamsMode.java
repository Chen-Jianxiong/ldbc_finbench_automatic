package org.ldbcouncil.finbench.driver.driver;

import com.google.common.collect.ImmutableList;
import org.ldbcouncil.finbench.driver.Db;
import org.ldbcouncil.finbench.driver.Operation;
import org.ldbcouncil.finbench.driver.Workload;
import org.ldbcouncil.finbench.driver.WorkloadStreams;
import org.ldbcouncil.finbench.driver.control.ControlService;
import org.ldbcouncil.finbench.driver.generator.GeneratorFactory;
import org.ldbcouncil.finbench.driver.generator.RandomDataGeneratorFactory;
import org.ldbcouncil.finbench.driver.log.LoggingService;
import org.ldbcouncil.finbench.driver.util.ClassLoaderHelper;
import org.ldbcouncil.finbench.driver.util.Tuple3;
import org.ldbcouncil.finbench.driver.validation.ValidationParam;
import org.ldbcouncil.finbench.driver.validation.ValidationParamsGenerator;
import org.ldbcouncil.finbench.driver.validation.ValidationParamsToJson;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import static java.lang.String.format;

public class CreateValidationParamsMode implements DriverMode<Object> {
    private final ControlService controlService;
    private final LoggingService loggingService;
    private final long randomSeed;

    private Workload workload = null;
    private Db database = null;
    private Iterator<Operation> timeMappedOperations = null;

    /**
     * Create class to generate validation queries.
     *
     * @param controlService Object with functions to time, log and stored init configuration
     * @param randomSeed     The random seed used for the data generator
     * @throws DriverException
     */
    public CreateValidationParamsMode(ControlService controlService, long randomSeed) throws DriverException {
        this.controlService = controlService;
        this.loggingService = controlService.loggingServiceFactory().loggingServiceFor(getClass().getSimpleName());
        this.randomSeed = randomSeed;
    }

    /**
     * Initializes the validation parameter class. This loads the configuration given through
     * validation.properties and the database to use to generate the validation parameters.
     */
    @Override
    public void init() throws DriverException {
        // 创建并初始化 配置项workload 指定的实现类（LdbcFinBenchTransactionWorkload）
        try {
            workload = ClassLoaderHelper.loadWorkload(controlService.configuration().workloadClassName());
            workload.init(controlService.configuration());
        } catch (Exception e) {
            throw new DriverException(format("Error loading Workload class: %s",
                controlService.configuration().workloadClassName()), e);
        }
        loggingService.info(format("Loaded Workload: %s", workload.getClass().getName()));

        //  ================
        //  =====  DB  =====
        //  ================
        try {
            database = ClassLoaderHelper.loadDb(controlService.configuration().dbClassName());
            database.init(
                controlService.configuration().asMap(),
                controlService.loggingServiceFactory().loggingServiceFor(database.getClass().getSimpleName()),
                workload.operationTypeToClassMapping()
            );
        } catch (Exception e) {
            throw new DriverException(
                format("Error loading DB class: %s", controlService.configuration().dbClassName()), e);
        }
        loggingService.info(format("Loaded DB: %s", database.getClass().getName()));

        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(randomSeed));

        loggingService.info(
            format("Retrieving operation stream for workload: %s", workload.getClass().getSimpleName()));
        try {
            boolean returnStreamsWithDbConnector = false;
            Tuple3<WorkloadStreams, Workload, Long> streamsAndWorkload =
                WorkloadStreams.createNewWorkloadWithOffsetAndLimitedWorkloadStreams(
                    controlService.configuration(),
                    gf,
                    returnStreamsWithDbConnector,
                    0,
                    controlService.configuration().operationCount(),
                    controlService.loggingServiceFactory()
                );
            workload = streamsAndWorkload._2();
            WorkloadStreams workloadStreams = streamsAndWorkload._1();
            timeMappedOperations =
                WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators(gf, workloadStreams);
        } catch (Exception e) {
            throw new DriverException("Error while retrieving operation stream for workload", e);
        }

        loggingService.info("Driver Configuration");
        loggingService.info(controlService.toString());
    }

    /**
     * Create validation parameters.
     */
    @Override
    public Object startExecutionAndAwaitCompletion() throws DriverException {
        try (Workload w = workload; Db db = database) {
            // 创建要生成的验证文件， validate_database参数项指定（validation_params.csv）
            File validationFileToGenerate =
                new File(controlService.configuration().databaseValidationFilePath());

            int validationSetSize = controlService.configuration().validationParametersSize();

            loggingService.info(
                format("Generating database validation file: %s", validationFileToGenerate.getAbsolutePath()));

            // 验证参数迭代生成器
            Iterator<ValidationParam> validationParamsGenerator = new ValidationParamsGenerator(
                db,
                w.dbValidationParametersFilter(validationSetSize),
                timeMappedOperations,
                validationSetSize
            );
            // 将验证参数迭代生成器转换为Json
            List<ValidationParam> validationParams = ImmutableList.copyOf(validationParamsGenerator);
            ValidationParamsToJson validationParamsAsJson = new ValidationParamsToJson(
                validationParams,
                workload,
                controlService.configuration().validationSerializationCheck()
            );
            // 将json序列化到文件，并再读取一遍，判断序列化前后是否会不一样
            validationParamsAsJson.serializeValidationParameters(validationFileToGenerate);

            loggingService.info(format("Successfully generated %s database validation parameters",
                validationParams.size()));
        } catch (Exception e) {
            throw new DriverException("Error encountered duration validation parameter creation", e);
        }
        return null;
    }
}

package org.ldbcouncil.finbench.driver.driver;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.ldbcouncil.finbench.driver.Db;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.Workload;
import org.ldbcouncil.finbench.driver.WorkloadException;
import org.ldbcouncil.finbench.driver.control.ControlService;
import org.ldbcouncil.finbench.driver.log.LoggingService;
import org.ldbcouncil.finbench.driver.util.ClassLoaderHelper;
import org.ldbcouncil.finbench.driver.validation.DbValidationResult;
import org.ldbcouncil.finbench.driver.validation.DbValidator;
import org.ldbcouncil.finbench.driver.validation.ValidationParam;
import org.ldbcouncil.finbench.driver.validation.ValidationParamsFromJson;

import java.io.*;
import java.util.List;

import static java.lang.String.format;

public class ValidateDatabaseMode implements DriverMode<DbValidationResult> {
    private final ControlService controlService;
    private final LoggingService loggingService;

    private Workload workload = null;
    private Db database = null;

    public ValidateDatabaseMode(ControlService controlService) {
        this.controlService = controlService;
        this.loggingService = controlService.loggingServiceFactory().loggingServiceFor(getClass().getSimpleName());
    }

    @Override
    public void init() throws DriverException {
        // 创建并初始化配置项workload指定的实现类（LdbcFinBenchTransactionWorkload）
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
        } catch (DbException e) {
            throw new DriverException(
                format("Error loading DB class: %s", controlService.configuration().dbClassName()), e);
        }
        loggingService.info(format("Loaded DB: %s", database.getClass().getName()));

        loggingService.info("Driver Configuration");
        loggingService.info(controlService.toString());
    }

    @Override
    public DbValidationResult startExecutionAndAwaitCompletion() throws DriverException {
        try (Workload w = workload; Db db = database) {
            // 创建要生成的验证文件， validate_database参数项指定（validation_params.csv）
            File validationParamsFile = new File(controlService.configuration().databaseValidationFilePath());

            loggingService.info(
                format("Validating database against expected results\n * Db: %s\n * Validation Params File: %s",
                    db.getClass().getName(), validationParamsFile.getAbsolutePath()));

            // 反序列化 json 文件
            ValidationParamsFromJson
                validationParamsFromJson = new ValidationParamsFromJson(validationParamsFile, workload);
            List<ValidationParam> validationParamList = validationParamsFromJson.deserialize();

            DbValidationResult databaseValidationResult;
            try {
                DbValidator dbValidator = new DbValidator();
                // 使用生成的验证参数验证数据
                databaseValidationResult = dbValidator.validate(
                    validationParamList.iterator(),
                    db,
                    validationParamList.size(),
                    w
                );
            } catch (WorkloadException e) {
                throw new DriverException(format("Error while validating workload using file: %s",
                    validationParamsFile.getAbsolutePath()), e);
            }

            // 验证失败操作的实际结果文件
            File failedValidationOperationsFile = new File(validationParamsFile.getParentFile(),
                removeExtension(validationParamsFile.getName()) + "-failed-actual.json");
            if (failedValidationOperationsFile.exists()) {
                FileUtils.forceDelete(failedValidationOperationsFile);
            }
            failedValidationOperationsFile.createNewFile();
            try (Writer writer = new OutputStreamWriter(new FileOutputStream(failedValidationOperationsFile),
                Charsets.UTF_8)) {
                writer.write(databaseValidationResult.actualResultsForFailedOperationsAsJsonString(w));
                writer.flush();
            } catch (Exception e) {
                throw new DriverException(
                    format("Encountered error while writing to file\nFile: %s",
                        failedValidationOperationsFile.getAbsolutePath()),
                    e
                );
            }

            // 验证失败操作的预期结果文件
            File expectedResultsForFailedValidationOperationsFile = new File(validationParamsFile.getParentFile(),
                removeExtension(validationParamsFile.getName()) + "-failed-expected.json");
            if (expectedResultsForFailedValidationOperationsFile.exists()) {
                FileUtils.forceDelete(expectedResultsForFailedValidationOperationsFile);
            }
            expectedResultsForFailedValidationOperationsFile.createNewFile();
            try (Writer writer = new OutputStreamWriter(
                new FileOutputStream(expectedResultsForFailedValidationOperationsFile), Charsets.UTF_8)) {
                writer.write(databaseValidationResult.expectedResultsForFailedOperationsAsJsonString(w));
                writer.flush();
            } catch (Exception e) {
                throw new DriverException(
                    format("Encountered error while writing to file\nFile: %s",
                        failedValidationOperationsFile.getAbsolutePath()),
                    e
                );
            }

            loggingService.info(databaseValidationResult.resultMessage());
            loggingService.info(format(
                "For details see the following files:\n * %s\n * %s",
                failedValidationOperationsFile.getAbsolutePath(),
                expectedResultsForFailedValidationOperationsFile.getAbsolutePath()
            ));
            return databaseValidationResult;
        } catch (IOException e) {
            throw new DriverException("Error occurred during database validation", e);
        }
    }

    String removeExtension(String filename) {
        return (filename.indexOf(".") == -1) ? filename : filename.substring(0, filename.lastIndexOf("."));
    }
}

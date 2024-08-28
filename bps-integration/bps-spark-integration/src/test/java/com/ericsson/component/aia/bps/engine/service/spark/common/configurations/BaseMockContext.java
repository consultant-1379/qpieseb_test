package com.ericsson.component.aia.bps.engine.service.spark.common.configurations;

import static com.ericsson.component.aia.bps.core.common.Constants.SEPARATOR;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.EXPECTED_FILE_OUTPUT;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.INPUT_DATA_SET;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.ROOT_BASE_FOLDER;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileDeleteStrategy;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BaseMockContext is the parent base class for all mock context objects and it holds common code useful while running testcase.
 */
public abstract class BaseMockContext {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseMockContext.class);

    /** The tmp dir. */
    protected Path tmpDir;

    /** The input data format. */
    protected String inputDataFormat;

    /** The output data format. */
    protected String outputDataFormat;

    /** The fields all string. */
    boolean fieldsAllString = true;

    /**
     * Instantiates a new base mock context.
     */
    public BaseMockContext() {

    }

    /**
     * Instantiates a new base mock context.
     *
     * @param className
     *            the class name
     */
    public BaseMockContext(final String className) {

        /*
         * This configuration should be set incase of windows System.setProperty("hadoop.home.dir", "C:\\aia\\components\\hadoop-bin");
         */
        final String dir = ROOT_BASE_FOLDER + className;
        final File fileDir = new File(dir);

        if (fileDir.exists()) {
            fileDir.delete();
        }

        fileDir.mkdir();
        tmpDir = fileDir.toPath();
    }

    /**
     * Input configurations for a input source as defined in flow xml.
     *
     * @return the map
     */
    public abstract Map<String, String> inputConfigurations();

    /**
     * OutputConfigurations for a output source as defined in flow xml.
     *
     * @return the map
     */
    public abstract Map<String, String> outputConfigurations();

    /**
     * Validates test case scenario.
     */
    public abstract void validate();

    /**
     * StepConfigurations for a Job as defined in flow xml.
     *
     * @return the map
     */
    public Map<String, String> stepConfigurations() {
        final Map<String, String> stepMap = new HashMap<String, String>();
        stepMap.put("uri", "spark-batch://sales-analysis");
        stepMap.put("sql", "SELECT * FROM sales");
        final String tmpFolder = tmpDir.toAbsolutePath() + SEPARATOR;
        stepMap.put("spark.local.dir", tmpFolder + "spark_local_dir");
        stepMap.put("hive.exec.dynamic.partition.mode", "nonstrict");
        stepMap.put("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        stepMap.put("spark.externalBlockStore.url", tmpFolder + "spark.externalBlockStore.url");
        stepMap.put("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        stepMap.put("hive.metastore.warehouse.dir", tmpFolder + "hive");
        stepMap.put("spark.externalBlockStore.baseDir", tmpFolder + "spark.externalBlockStore.baseDir");
        stepMap.put("hive.exec.scratchdir", tmpFolder + "hive.exec.scratchdir");
        stepMap.put("hive.downloaded.resources.dir", tmpDir + SEPARATOR + "hive.resources.dir_" + System.currentTimeMillis());
        stepMap.put("hive.querylog.location", tmpFolder + "querylog.location");
        stepMap.put("hive.exec.local.scratchdir", tmpFolder + "hive.exec.local.scratchdir");
        stepMap.put("hive.downloaded.resources.dir", tmpFolder + "hive.downloaded.resources.dir");
        stepMap.put("hive.metadata.export.location", tmpFolder + "hive.metadata.export.location");
        stepMap.put("hive.metastore.metadb.dir", tmpFolder + "hive.metastore.metadb.dir");

        for (final Map.Entry<String, String> entry : stepMap.entrySet()) {
            System.setProperty(entry.getKey(), entry.getValue());
        }

        return stepMap;
    }

    /**
     * Gets the temporary directory.
     *
     * @return the tmpDir
     */
    public Path getTmpDir() {
        return tmpDir;
    }

    /**
     * Clean up operation for junit test cases.
     */
    public void cleanUp() {

        try {
            FileDeleteStrategy.FORCE.delete(tmpDir.toFile());
        } catch (final IOException e) {
            LOGGER.info("CleanUp, IOException", e);
        }

    }

    /**
     * Validates expected & actual file output data.
     *
     * @param outputDirectory
     *            the outputDirectory
     */
    public void validateFileOutput(final Path outputDirectory) {
        validateFileOutput(outputDirectory, EXPECTED_FILE_OUTPUT + SEPARATOR + "simple_text");
    }

    /**
     * Validates expected & actual file output data.
     *
     * @param outputDirectory
     *            the outputDirectory
     * @param expectedOpPath
     *            the expected output path
     */
    public void validateFileOutput(final Path outputDirectory, final String expectedOpPath) {

        LOGGER.info("******************************validateFileOutput**************************************");

        LOGGER.info("outputDirectory-------" + outputDirectory);
        final java.io.File[] actual = outputDirectory.toFile().listFiles();
        LOGGER.info("Actual--FileOutput-------" + Arrays.toString(actual));

        final File expectedFile = new File(expectedOpPath);

        boolean isCompareCheckDone = false;

        for (final java.io.File actualFile : actual) {

            LOGGER.info("actualFile.getAbsolutePath()------" + actualFile.getAbsolutePath());

            if (!actualFile.getName().toString().endsWith(".crc") && !actualFile.getName().toString().endsWith("_SUCCESS")) {

                LOGGER.info("expectedFile----" + expectedFile);
                LOGGER.info("actualFile------" + actualFile);

                isCompareCheckDone = true;

                try {

                    if (FileUtils.readLines(actualFile).isEmpty()) {
                        LOGGER.error("FileUtils.readLines(actualFile)--.isEmpty()--" + FileUtils.readLines(actualFile));
                        Assert.fail("File name not equal");
                    }

                    Assert.assertEquals(FileUtils.readLines(expectedFile), FileUtils.readLines(actualFile));

                } catch (final IOException e) {
                    LOGGER.error("validateFileOutput------", e);
                    Assert.fail(e.getMessage());
                }
            }
        }

        if (!isCompareCheckDone) {
            Assert.fail("File names not equal");
        }

    }

    /**
     * Validate Validates expected & actual DB output data.
     *
     * @param conn
     *            the connection
     * @param dbName
     *            the database name
     * @param query
     *            the query
     */
    public void validateDBOutput(final Connection conn, final String dbName, String query) {

        final String JDBC_OP = tmpDir.toAbsolutePath() + SEPARATOR + dbName + "_JDBC_OP.csv";
        query = query.replace("JDBC_OP", JDBC_OP);
        Statement stmt = null;

        try {
            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            conn.close();
        } catch (final Exception e) {
            Assert.fail(e.getMessage());
        }

        final java.io.File expectedFile = new java.io.File(INPUT_DATA_SET);
        final java.io.File actualFile = new java.io.File(JDBC_OP);

        try {

            LOGGER.info("FileUtils.readLines(actualFile)----" + FileUtils.readLines(actualFile));
            LOGGER.info("FileUtils.readLines(expectedFile)----" + FileUtils.readLines(expectedFile));
            containsExactText(actualFile, expectedFile);
        } catch (final IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    void containsExactText(final File expectedFile, final File actualFile) throws IOException {

        final List<String> actual = FileUtils.readLines(actualFile);
        final List<String> expected = FileUtils.readLines(expectedFile);

        Collections.sort(actual);
        Collections.sort(expected);

        Assert.assertEquals(actual, expected);

    }

    /**
     * Checks if is fields all string.
     *
     * @return true, if is fields all string
     */
    public boolean isFieldsAllString() {
        return fieldsAllString;
    }

    /**
     * Sets the fields all string.
     *
     * @param fieldsAllString
     *            the new fields all string
     */
    public void setFieldsAllString(final boolean fieldsAllString) {
        this.fieldsAllString = fieldsAllString;
    }

    /**
     * @return the inputDataFormat
     */
    public String getInputDataFormat() {
        return inputDataFormat;
    }

    /**
     * @param inputDataFormat
     *            the inputDataFormat to set
     */
    public void setInputDataFormat(final String inputDataFormat) {
        this.inputDataFormat = inputDataFormat;
    }

    /**
     * @return the outputDataFormat
     */
    public String getOutputDataFormat() {
        return outputDataFormat;
    }

    /**
     * @param outputDataFormat
     *            the outputDataFormat to set
     */
    public void setOutputDataFormat(final String outputDataFormat) {
        this.outputDataFormat = outputDataFormat;
    }

}

/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2016
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.ericsson.component.aia.bps.engine.service.spark;

import static com.ericsson.component.aia.bps.core.common.Constants.SEPARATOR;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.BASE_IT_FOLDER;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.FLOW;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.FLOW_XML;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.ROOT_BASE_FOLDER;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileDeleteStrategy;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.engine.service.BPSPipeLineExecuter;
import com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.DATA_FORMAT;
import com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.TestType;
import com.ericsson.component.aia.bps.engine.service.spark.common.TestUtil;
import com.ericsson.component.aia.bps.engine.service.spark.common.configurations.BaseMockContext;
import com.ericsson.component.aia.bps.engine.service.spark.common.configurations.MockJdbcContext;

/**
 * Integration test suite for BPSPipeLineExecuter Spark Batch.
 */
@RunWith(value = Parameterized.class)
public class BPSPipeLineExecuterBatchTest {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(BPSPipeLineExecuterBatchTest.class);

    /** The data formats map. */
    private static Map<TestType, EnumSet<DATA_FORMAT>> dataFormatsMap;

    /** The Constant emptyArray. */
    private static final String[] emptyArray = { "" };

    /** The name. */
    private final String name;

    /** The input type. */
    private final Class<? extends BaseMockContext> inputType;

    /** The output type. */
    private final Class<? extends BaseMockContext> outputType;

    /** The tmp dir. */
    private Path tmpDir;

    /** The input context. */
    private BaseMockContext inputContext;

    /** The output context. */
    private BaseMockContext outputContext;

    /** The input data format. */
    private String inputDataFormat;

    /** The output data format. */
    private String outputDataFormat;

    static {
        dataFormatsMap = new HashMap<>();
        //dataFormatsMap.put(TestType.HDFS, CSV_JSON_PARQUET);
        //dataFormatsMap.put(TestType.FILE, CSV_JSON_PARQUET);
    }

    /**
     * Instantiates a new BPS pipe line executer batch test.
     *
     * @param name
     *            the name
     * @param inputType
     *            the input type
     * @param outputType
     *            the output type
     * @param inputDataFormat
     *            the input data format
     * @param outputDataFormat
     *            the output data format
     */
    public BPSPipeLineExecuterBatchTest(final String name, final Class<? extends BaseMockContext> inputType,
                                        final Class<? extends BaseMockContext> outputType, final String inputDataFormat,
                                        final String outputDataFormat) {
        this.inputType = inputType;
        this.outputType = outputType;
        this.name = name;
        this.inputDataFormat = inputDataFormat;
        this.outputDataFormat = outputDataFormat;
    }

    /**
     * Generates Scenarios matrix set.
     *
     * @return the collection
     * @throws Exception
     *             the exception
     */
    @Parameters(name = "{index}: Validating Scenario [ {0} ]")
    public static Collection<Object[]> data() throws Exception {

        LOGGER.info("Creating integration test scenarios" + BASE_IT_FOLDER);

        final List<Object[]> scenarios = new ArrayList<>();

        for (final TestType fromType : TestType.values()) {

            for (final String inputDataFormat : getScenarios(fromType.name())) {

                String inputAppendStr = inputDataFormat;

                if (StringUtils.isNotBlank(inputDataFormat)) {
                    inputAppendStr = "_" + inputDataFormat;
                }

                for (final TestType toType : TestType.values()) {

                    for (final String outputDataFormat : getScenarios(fromType.name())) {

                        String outputAppendStr = outputDataFormat;

                        if (StringUtils.isNotBlank(outputDataFormat)) {
                            outputAppendStr = "_" + outputDataFormat;
                        }

                        final Object[] scenario = new Object[5];
                        final Class<? extends BaseMockContext> inputContext = fromType.ref;
                        final Class<? extends BaseMockContext> outputContext = toType.ref;
                        scenario[0] = fromType.name() + inputAppendStr + " TO " + toType.name() + outputAppendStr;
                        scenario[1] = inputContext;
                        scenario[2] = outputContext;
                        scenario[3] = inputDataFormat;
                        scenario[4] = outputDataFormat;

                        scenarios.add(scenario);
                    }
                }
            }
        }

        LOGGER.info("Finished created integration test scenarios");

        return scenarios;
    }

    /**
     * Gets test scenarios for data formats.
     *
     * @param contextType
     *            the context type
     * @return the scenarios
     */
    public static String[] getScenarios(final String contextType) {
        if (dataFormatsMap.containsKey(contextType)) {

            final List<String> list = new ArrayList<>();

            for (final DATA_FORMAT dataFormat : dataFormatsMap.get(contextType)) {
                list.add(dataFormat.getDataFormat());
            }

            return (String[]) list.toArray();
        } else {
            return emptyArray;
        }
    }

    /**
     * Creates the base folder.
     *
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void createBaseFolder() throws Exception {

        if (new File(BASE_IT_FOLDER).exists()) {
            FileUtils.forceDelete(new File(BASE_IT_FOLDER));
        }

        FileUtils.forceMkdir(new File(BASE_IT_FOLDER));
    }

    /**
     * Delete base folder.
     *
     * @throws Exception
     *             the exception
     */
    @AfterClass
    public static void deleteBaseFolder() throws Exception {

        if (new File(BASE_IT_FOLDER).exists()) {
            FileUtils.forceDelete(new File(BASE_IT_FOLDER));
        }
    }

    /**
     * Creates and initializes folders and different context required for the unit test case.
     *
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception {

        final String dir = ROOT_BASE_FOLDER + this.getClass().getSimpleName();
        final File fileDir = new File(dir);
        fileDir.mkdir();
        tmpDir = fileDir.toPath();
        LOGGER.info("Created Temp Directory--" + tmpDir.toAbsolutePath());

        System.setProperty("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=" + tmpDir + SEPARATOR + "junit_metastore_db;create=true");

        System.setProperty("hive.downloaded.resources.dir", tmpDir + SEPARATOR + "hive.resources.dir_" + System.currentTimeMillis());

        if (!inputType.getName().equals(outputType.getName())) {
            inputContext = inputType.newInstance();
            outputContext = outputType.newInstance();
        } else {
            outputContext = inputContext = inputType.newInstance();
        }

        inputContext.setInputDataFormat(inputDataFormat);
        outputContext.setOutputDataFormat(outputDataFormat);
    }

    /**
     * Executes Test scenario {0} based on Collection<Object[]>.
     */
    @Test
    public void testScenario() {

        LOGGER.info("Started test scenario");

        /*
         * System.setProperty("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=" + tmpDir + SEPARATOR + "junit_metastore_db;create=true");
         */
        final Map<String, Map<String, String>> context = new HashMap<>();
        context.put("attributeIPMap", inputContext.inputConfigurations());
        context.put("attributeOPMap", outputContext.outputConfigurations());
        context.put("attributeStepMap", outputContext.stepConfigurations());

        LOGGER.info("About to execute pipeline");

        executePipeLine(context, tmpDir, name);

        LOGGER.info("successfully executed pipeline");

        LOGGER.info("inputType: " + inputType.getName());
        LOGGER.info("outputType: " + outputType.getName());

        if (!(inputContext instanceof MockJdbcContext || outputContext instanceof MockJdbcContext)) {
            outputContext.setFieldsAllString(false);
        }

        outputContext.validate();

        LOGGER.info("successfully executed validate");

        LOGGER.info("Finished test scenario");
    }

    /**
     * Creates the flow xml based on the scenario.
     *
     * @param target_op
     *            the target op
     * @param context
     *            the context
     */
    public static void createFlowXml(final Path target_op, final Map<String, Map<String, String>> context) {

        try {
            TestUtil.createFolder(target_op);
            TestUtil.createXml(FLOW_XML, target_op.toFile().toString(), context, FLOW);

            LOGGER.info("Initialized Pipe-line successfully, executing pipe-line now!!!");
        } catch (final IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Executes test case scenario pipeline based on flow xml.
     *
     * @param context
     *            the context
     * @param target_op
     *            the target op
     * @param scenarioType
     *            the scenario type
     */
    public static void executePipeLine(final Map<String, Map<String, String>> context, final Path target_op, final String scenarioType) {

        LOGGER.info("Creating Flow xml for the test scenario");

        createFlowXml(target_op, context);

        LOGGER.info("Created Flow xml for the test scenario");

        LOGGER.info("Started running pipeline");

        final String flowXML = target_op.toFile().toString() + SEPARATOR + FLOW;

        BPSPipeLineExecuter.main(new String[] { flowXML });

        LOGGER.info("Started running pipeline");
    }

    /**
     * Clean up operation for junit test cases.
     */
    @After
    public void tearDown() {

        LOGGER.info("Cleaning up of junit started");

        if (null != tmpDir && tmpDir.toFile().exists()) {
            try {
                FileDeleteStrategy.FORCE.delete(tmpDir.toFile());
            } catch (final IOException e) {
                LOGGER.info("CleanUp, IOException ", e);
            }
        }

        try {
            inputContext.cleanUp();
            outputContext.cleanUp();
        } catch (final Exception e) {
            LOGGER.info("CleanUp, Exception", e);
        }

        LOGGER.info("Cleanup of junit is successful");
    }
}

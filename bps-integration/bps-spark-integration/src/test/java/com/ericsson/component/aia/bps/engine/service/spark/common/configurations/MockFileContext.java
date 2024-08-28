package com.ericsson.component.aia.bps.engine.service.spark.common.configurations;

import static com.ericsson.component.aia.bps.core.common.Constants.SEPARATOR;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.EXPECTED_FILE_OUTPUT;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.INPUT_FILE;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.TRUE;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileDeleteStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.common.uri.IOURIS;

/**
 * MockFileContext is one of the implementation for BaseMockContext and it is useful in creating and validating FILE related test cases.
 */
public class MockFileContext extends BaseMockContext {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(MockFileContext.class);

    /**
     * Instantiates a new mock file context.
     */
    public MockFileContext() {
        super(MockFileContext.class.getSimpleName() + System.currentTimeMillis());
        LOGGER.info("Created Temp Directory--" + tmpDir.toAbsolutePath());
    }

    /**
     * Input configurations for a input source as defined in flow xml.
     *
     * @return the map
     */
    @Override
    public Map<String, String> inputConfigurations() {
        final Map<String, String> input = new HashMap<String, String>();
        input.put("uri", INPUT_FILE);
        input.put("header", TRUE);
        input.put("inferSchema", TRUE);
        input.put("drop-malformed", TRUE);
        input.put("dateFormat", "SimpleDateFormat");
        input.put("data.format", "text");
        input.put("skip-comments", TRUE);
        input.put("quote", "&quot;");
        input.put("table-name", "sales");
        input.put("quoteMode", "ALL");
        return input;
    };

    /**
     * OutputConfigurations for a output source as defined in flow xml.
     *
     * @return the map
     */
    @Override
    public Map<String, String> outputConfigurations() {
        final Map<String, String> output = new HashMap<String, String>();
        final String uri = tmpDir.toAbsolutePath().toString().replace("\\", "/");
        output.put("uri", IOURIS.FILE.getUri() + uri);
        output.put("data.format", "text");
        output.put("quote", "&quot;");
        output.put("quoteMode", "NON_NUMERIC");
        return output;
    };

    /**
     * StepConfigurations for a Job as defined in flow xml.
     *
     * @return the map
     */
    @Override
    public Map<String, String> stepConfigurations() {
        final Map<String, String> stepMap = super.stepConfigurations();
        stepMap.put("master.url", "local[*]");
        return stepMap;
    }

    /**
     * Clean up operation for junit test cases.
     */
    @Override
    public void cleanUp() {

        try {
            FileDeleteStrategy.FORCE.delete(tmpDir.toFile());
        } catch (final IOException e) {
            LOGGER.info("CleanUp, IOException", e);
        }
    }

    /**
     * Validates expected & actual output data.
     */
    @Override
    public void validate() {

        final java.nio.file.Path opDir = tmpDir.toAbsolutePath();

        if (fieldsAllString) {

            validateFileOutput(tmpDir.toAbsolutePath());
        } else {
            validateFileOutput(opDir, EXPECTED_FILE_OUTPUT + SEPARATOR + "simple_text");
        }
    }
}

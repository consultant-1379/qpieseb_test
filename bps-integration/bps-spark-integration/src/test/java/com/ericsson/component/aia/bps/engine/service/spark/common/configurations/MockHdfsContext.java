package com.ericsson.component.aia.bps.engine.service.spark.common.configurations;

import static com.ericsson.component.aia.bps.core.common.Constants.SEPARATOR;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.EXPECTED_FILE_OUTPUT;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.INPUT_DATA_FILE;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.INPUT_FILE;
import static com.ericsson.component.aia.bps.engine.service.spark.common.TestConstants.TRUE;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.common.Constants;
import com.ericsson.component.aia.bps.core.common.uri.IOURIS;

/**
 * MockHdfsContext is one of the implementation for BaseMockContext and it is useful in creating and validating HDFS related test cases.
 */
public class MockHdfsContext extends BaseMockContext {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(MockHdfsContext.class);

    /** The hdfs cluster. */
    private MiniDFSCluster hdfsCluster;

    /** The dst. */
    private String dst = "/test/" + INPUT_DATA_FILE;

    /** The dst op. */
    private String dst_op = "/hadoop-op";

    /** The hdfs uri output. */
    private String hdfsUriOutput;

    /** The fs. */
    private FileSystem fs;

    /**
     * Instantiates a new mock hdfs context.
     */
    public MockHdfsContext() {
        super(MockHdfsContext.class.getSimpleName() + System.currentTimeMillis());

        System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);
        final Path myPath = new Path("/test/");
        final File testDataPath = tmpDir.toFile();
        final Configuration conf = new HdfsConfiguration();
        final File testDataCluster1 = new File(testDataPath, "cluster1");
        final String c1PathStr = testDataCluster1.getAbsolutePath();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, c1PathStr);
        final FileSystem fileSys;
        // Source file in the local file system
        final String localSrc = INPUT_FILE.replace(IOURIS.FILE.getUri(), "");
        try {
            hdfsCluster = new MiniDFSCluster.Builder(conf).build();
            fileSys = hdfsCluster.getFileSystem();

            // dfsCluster = new MiniDFSCluster(conf, 1, true, null);
            assertNotNull("Cluster has a file system", hdfsCluster.getFileSystem());

            fileSys.mkdirs(myPath);
            // Input stream for the file in local file system to be written to
            // HDFS
            final InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

            // Destination file in HDFS
            fs = FileSystem.get(URI.create(dst), conf);
            final OutputStream out = fs.create(new Path(dst));

            // Copy file from local to HDFS
            IOUtils.copyBytes(in, out, 4096, true);
            LOGGER.info(dst + " copied to HDFS");
        } catch (final IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Input configurations for a input source as defined in flow xml.
     *
     * @return the map
     */
    @Override
    public Map<String, String> inputConfigurations() {
        final String hdfsURI = "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/" + dst;
        final Map<String, String> input = new HashMap<String, String>();
        input.put("uri", hdfsURI);
        input.put("header", TRUE);
        input.put("inferSchema", TRUE);
        input.put("drop-malformed", TRUE);
        input.put("dateFormat", "SimpleDateFormat");
        input.put("data.format", "text");
        input.put("table-name", "sales");
        input.put("skip-comments", TRUE);
        input.put("quote", "&quot;");
        return input;
    }

    /**
     * OutputConfigurations for a output source as defined in flow xml.
     *
     * @return the map
     */
    @Override
    public Map<String, String> outputConfigurations() {
        final Map<String, String> output = new HashMap<String, String>();
        hdfsUriOutput = "hdfs://hdfs://localhost:" + hdfsCluster.getNameNodePort() + dst_op;
        output.put("uri", hdfsUriOutput);
        output.put("data.format", "text");
        return output;
    }

    /**
     * Clean up operation for junit test cases.
     */
    @Override
    public void cleanUp() {

        if (null != fs) {
            try {
                fs.close();
            } catch (final IOException e) {
                LOGGER.info("CleanUp, IOException", e);
            }
        }

        if (hdfsCluster != null) {
            hdfsCluster.shutdown();
        }

        super.cleanUp();
    }

    /**
     * Validates expected & actual output data.
     */
    @Override
    public void validate() {

        // Get the filesystem - HDFS
        final FSDataInputStream in = null;
        final File file;

        try {

            file = new File(tmpDir.toAbsolutePath() + Constants.SEPARATOR + "hdfs-op");
            file.mkdirs();

            final FileStatus[] status = fs.listStatus(new Path("/hadoop-op"));

            for (int i = 0; i < status.length; i++) {
                fs.copyToLocalFile(status[i].getPath(),
                        new Path("file:///" + file.getAbsoluteFile().getPath() + Constants.SEPARATOR + status[i].getPath().getName()));
            }

            LOGGER.info("End Of file: HDFS file read complete");
        } catch (final Exception e) {
            fail(e.getMessage());
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeStream(in);
        }

        final java.nio.file.Path opDir = Paths.get(file.getAbsoluteFile().getPath());

        if (fieldsAllString) {

            validateFileOutput(opDir);

        } else {
            validateFileOutput(opDir, EXPECTED_FILE_OUTPUT + SEPARATOR + "simple_text");
        }
    }
}

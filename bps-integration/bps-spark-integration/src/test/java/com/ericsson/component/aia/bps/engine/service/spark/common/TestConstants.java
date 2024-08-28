package com.ericsson.component.aia.bps.engine.service.spark.common;

import static com.ericsson.component.aia.bps.core.common.Constants.SEPARATOR;

import java.util.EnumSet;

import com.ericsson.component.aia.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.bps.engine.service.spark.common.configurations.BaseMockContext;
import com.ericsson.component.aia.bps.engine.service.spark.common.configurations.MockFileContext;
import com.ericsson.component.aia.bps.engine.service.spark.common.configurations.MockHdfsContext;
import com.ericsson.component.aia.bps.engine.service.spark.common.configurations.MockJdbcContext;

/**
 * Various constants used across Spark test module.
 */
public interface TestConstants {

    /** The working dir. */
    String WORKING_DIR = System.getProperty("user.dir");

    /** The input data file. */
    String INPUT_DATA_FILE = "SalesJan2009.csv";

    /** The input data set. */
    String INPUT_DATA_SET = "src/test/data/expected_output/expected_output.csv".replace("/", SEPARATOR);

    /** The expected file output. */
    String EXPECTED_FILE_OUTPUT = "src/test/data/expected_output/file_output".replace("/", SEPARATOR);

    /** The input file. */
    String INPUT_FILE = IOURIS.FILE.getUri() + WORKING_DIR + "/src/test/data/input_data_set/SalesJan2009.csv".replace("/", SEPARATOR);

    /** The flow xml. */
    String FLOW_XML = "src" + SEPARATOR + "test" + SEPARATOR + "data" + SEPARATOR + "flow.vm";

    /** The newline. */
    String NEWLINE = "\n";

    /** The hive url. */
    String HIVE_URL = "org.apache.hive.jdbc.HiveDriver";

    /** The dfs replication interval. */
    int DFS_REPLICATION_INTERVAL = 1;

    /** The flow. */
    String FLOW = "flow.xml";

    /** The base it folder. */
    String BASE_IT_FOLDER = System.getProperty("java.io.tmpdir") + SEPARATOR + "bps_it";

    /** The root base folder. */
    String ROOT_BASE_FOLDER = BASE_IT_FOLDER + SEPARATOR + "junit_testing_";

    /** The h2 driver. */
    String H2_DRIVER = "org.h2.Driver";

    /** The true. */
    String TRUE = "true";

    /**
     * The Enum TestType.
     */
    enum TestType {

        // HIVE(MockHiveContext.class);
        /** The jdbc. */
        JDBC(MockJdbcContext.class),
        /** The file. */
        FILE(MockFileContext.class),
        /** The hdfs. */
        HDFS(MockHdfsContext.class);

        /** The ref. */
        public Class<? extends BaseMockContext> ref;

        // FILE(MockFileContext.class);

        /**
         * Instantiates a new test type.
         *
         * @param ref
         *            the ref
         */
        TestType(final Class<? extends BaseMockContext> ref) {
            this.ref = ref;
        }
    }

    /**
     * The Enum DATA_FORMATS.
     */
    enum DATA_FORMAT {

        /** The json. */
        JSON("json"),
        /** The csv. */
        CSV("csv"),
        /** The text. */
        TEXT("text"),
        /** The sequencefile. */
        SEQUENCEFILE("SEQUENCEFILE"),
        /** The rcfile. */
        RCFILE("RCFILE"),
        /** The parquet. */
        PARQUET("parquet"),
        /** The avro. */
        AVRO("avro"),
        /** The orc. */
        ORC("ORC"),
        /** The textfile. */
        TEXTFILE("TEXTFILE");

        /** Data Format. */
        public String dataFormat;

        /**
         * Instantiates a new data formats.
         *
         * @param dataFormat
         *            the data format
         */
        DATA_FORMAT(final String dataFormat) {
            this.dataFormat = dataFormat;
        }

        /**
         * @return the dataFormat
         */
        public String getDataFormat() {
            return dataFormat;
        }
    }

    EnumSet<DATA_FORMAT> HDFS_DATA_FORMATS = EnumSet.of(DATA_FORMAT.CSV, DATA_FORMAT.JSON);
    EnumSet<DATA_FORMAT> FILE_DATA_FORMATS = EnumSet.of(DATA_FORMAT.CSV, DATA_FORMAT.JSON);

    /** The csv json parquet. */
    EnumSet<DATA_FORMAT> CSV_JSON_PARQUET = EnumSet.of(DATA_FORMAT.CSV, DATA_FORMAT.JSON, DATA_FORMAT.PARQUET);
}

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
package com.ericsson.component.aia.bps.spark.datasourceservice;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.common.Constants;
import com.ericsson.component.aia.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.bps.core.datasourceservice.AbstractBpsDataSourceService;
import com.google.common.base.Preconditions;

/**
 * The <code>BpsSparkHdfsDataSourceService</code> is responsible for reading data from hdfs system and return respective {@link DataFrame } .<br>
 *
 * The <code>BpsSparkHdfsDataSourceService</code> implements <code>BpsDataSourceService&lt;HiveContext, DataFrame&gt;</code> which is specific to
 * HiveContext & DataFrame. <br>
 * <br>
 */
public class BpsSparkHdfsDataSourceService extends AbstractBpsDataSourceService<HiveContext, DataFrame> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BpsSparkHdfsDataSourceService.class);

    /*
     * (non-Javadoc)
     *
     * @see com.ericsson.aia.common.service.GenericService#getServiceContextName()
     */
    @Override
    public String getServiceContextName() {
        return IOURIS.HDFS.getUri();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.ericsson.aia.bps.core.service.BpsDataService#getDataStream(java.lang.Object, java.util.Properties)
     */
    @Override
    public DataFrame getDataStream() {
        LOGGER.trace("Entering the getHDFSFileContexts method ");
        Preconditions.checkState(context != null, "Spark context is not initialized.");
        final String file = properties.getProperty(Constants.URI);
        final String format = properties.getProperty(Constants.DATA_FORMAT);
        final String header = properties.getProperty(Constants.HEADER_ENABLED);
        final String inferedSchema = properties.getProperty(Constants.INFER_SCHEMA);
        Preconditions.checkArgument(format != null && format.trim().length() != 0,
                "Please specify a data format. The data.format supported values are text,json,parquet");
        final String tableName = properties.getProperty("table-name");

        if (("text").equalsIgnoreCase(format)) {
            final DataFrame frame = context.read().format("com.databricks.spark.csv").option("inferSchema", inferedSchema).option("header", header)
                    .load(file);
            frame.registerTempTable(tableName);
            return frame;
        } else if (("json").equalsIgnoreCase(format)) {
            final DataFrame frame = context.read().json(file);
            frame.registerTempTable(tableName);
            return frame;
        } else if (("parquet").equalsIgnoreCase(format)) {
            final DataFrame frame = context.read().load(file);
            frame.registerTempTable(tableName);
            return frame;
        }
        LOGGER.trace("Returning the getHDFSFileContexts method ");
        throw new IllegalArgumentException("invalid data format");
    }
}

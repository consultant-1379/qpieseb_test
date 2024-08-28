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

import java.util.Properties;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.common.Constants;
import com.ericsson.component.aia.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.bps.core.datasourceservice.AbstractBpsDataSourceService;
import com.google.common.base.Preconditions;

/**
 * The <code>BpsSparkFileDataSourceService</code> is responsible for reading data from file system and return respective {@link DataFrame } .<br>
 *
 * The <code>BpsSparkFileDataSourceService</code> implements <code>BpsDataSourceService&lt;HiveContext, DataFrame&gt;</code> which is specific to
 * HiveContext & DataFrame. <br>
 * <br>
 * Example of simple configuration of FileDatasource.
 *
 * <pre>
 *  &lt;input name="file_DataSource_Name"&gt;
 *       &lt;attribute name="URI" value="file:///Absolute_Path" /&gt;    &lt;!--  Representing data source is File type  --&gt;
 *       &lt;attribute name="header" value="true|false" /&gt; &lt;!--  if file type is csv and has first row representing header   --&gt;
 *       &lt;attribute name="inferSchema" value="true|false" /&gt;  &lt;!-- Can infer Schema ?   --&gt;
 *       &lt;attribute name="drop-malformed" value="true" /&gt;  &lt;!--  can drop malformed row ?   --&gt;
 *       &lt;attribute name="dateFormat" value="SimpleDateFormat" /&gt; &lt;!--  how to interpret date   --&gt;
 *       &lt;attribute name="data.format" value="text" /&gt; &lt;!--  date format type   --&gt;
 *       &lt;attribute name="skip-comments" value="true|false" /&gt;  &lt;!--  skip comment part of the file?   --&gt;
 *       &lt;attribute name="quote" value="&amp;quot;" /&gt;
 *       &lt;attribute name="persist" value="false" /&gt;
 *       &lt;!-- If this enabled the data will be materialized , otherwise will dropped after finishing job --&gt;
 *       &lt;attribute name="table-name" value="Table_Name" /&gt; &lt;!--by which provided content can be further utilized in processing phase--&gt;
 * &lt;/input"&gt;
 * </pre>
 *
 */
public class BpsSparkFileDataSourceService extends AbstractBpsDataSourceService<HiveContext, DataFrame> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BpsSparkFileDataSourceService.class);

    @Override
    public String getServiceContextName() {
        return IOURIS.FILE.getUri();
    }

    /**
     * The {@link BpsSparkFileDataSourceService#getDataStream(HiveContext, Properties)} will return Dataframe based on input configuration.
     */
    @Override
    public DataFrame getDataStream() {
        //LOGGER.trace(String.format("Initiating getDataStream for %s. ", getS));
        Preconditions.checkState(context != null, "Spark context is not initialized.");
        final String inferedSchema = properties.getProperty(Constants.INFER_SCHEMA);
        final String header = properties.getProperty(Constants.HEADER_ENABLED);
        final String tableName = properties.getProperty("table-name");
        final boolean persist = Boolean.getBoolean(properties.getProperty("persist"));
        final String partition = properties.getProperty("partition.columns");
        final String format = properties.getProperty(Constants.DATA_FORMAT);
        if (persist && !"text".equalsIgnoreCase(format)) {
            Preconditions.checkArgument(format != null && format.trim().length() != 0,
                    "Please specify a data format. The data.format supported values are text,parquet");
        }
        final DataFrame dataFrame = context.read().format("com.databricks.spark.csv").option("inferSchema", inferedSchema).option("header", header)
                .load(properties.getProperty(Constants.URI));
        if (persist && !"text".equalsIgnoreCase(format)) {
            Preconditions.checkArgument(partition != null && partition.trim().length() > 0,
                    "If perisistance enabled for input, please provide one ore more partition colums comma seperated.");
            dataFrame.write().mode(SaveMode.Append).partitionBy(partition.split(",")).saveAsTable(tableName);
        } else {
            dataFrame.registerTempTable(tableName);
        }
        LOGGER.trace("Returning the createFileContext method ");
        return dataFrame;
    }
}

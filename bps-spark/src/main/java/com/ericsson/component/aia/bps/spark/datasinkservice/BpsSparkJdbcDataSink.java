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
package com.ericsson.component.aia.bps.spark.datasinkservice;

import java.util.Properties;

import org.apache.spark.sql.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.component.aia.bps.core.common.Constants;
import com.ericsson.component.aia.bps.core.common.uri.IOURIS;
import com.ericsson.component.aia.bps.core.service.streams.BpsAbstractDataSink;
import com.ericsson.component.aia.bps.spark.configuration.partition.SparkDefaultPartition;
import com.ericsson.component.aia.bps.spark.utils.JDBCUtilities;

/**
 * The <code>BpsSparkJdbcDataSink</code> class is responsible for writing {@link DataFrame } to a Jdbc.<br>
 * The default Partition strategy is {@link SparkDefaultPartition}.
 *
 * @param <C>
 *            the generic type representing the context like HiveContext etc.
 */
public class BpsSparkJdbcDataSink<C> extends BpsAbstractDataSink<C, DataFrame> {

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(BpsSparkJdbcDataSink.class);

    /** The connection props. */
    private Properties connectionProps;

    /** The table name. */
    private String tableName;

    /**
     * Configured instance of {@link BpsSparkJdbcDataSink} for the specified sinkContextName.<br>
     * sinkContextName is the name of the output sink which was configured in flow.xml through tag &lt;output name="sinkContextName"&gt; ....
     * &lt;/output&gt;
     *
     * @param context
     *            the context for which Sink needs to be configured.
     * @param properties
     *            the configuration associated with underlying output sink.
     * @param sinkContextName
     *            Unique name associated with each of the output sink.
     */
    @Override
    public void configureDataSink(final C context, final Properties properties, final String sinkContextName) {
        super.configureDataSink(context, properties, sinkContextName);
        LOGGER.trace(String.format("Initiating configureDataSink for %s. ", getDataSinkContextName()));
        tableName = properties.getProperty(Constants.TABLE);
        // connectionProps = JDBCUtilities.getJDBCProperties(configuration);
        connectionProps = properties;
        JDBCUtilities.cleanTarget(getWritingContext(), connectionProps, tableName);
        strategy = new SparkDefaultPartition();
        LOGGER.info(String.format("Configuring %s for the output  %s with partition strategy %s", this.getClass().getName(), sinkContextName,
                strategy.getClass().getName()));
        strategy.init(properties);
        LOGGER.trace(String.format("Finished configureDataSink method for the sink context name %s ", sinkContextName));
    }

    /**
     * Writes DataFrame to JDBC data source.
     */
    @Override
    public void write(final DataFrame frame) {
        LOGGER.trace(String.format("Initiating Write for %s. ", getDataSinkContextName()));
        frame.write().jdbc(getWritingContext(), tableName, connectionProps);
        LOGGER.trace(String.format("Finished Write for %s. ", getDataSinkContextName()));
    }

    @Override
    public void cleanUp() {
        LOGGER.trace(String.format("Cleaning resources allocated for %s ", getDataSinkContextName()));
        strategy = null;
        LOGGER.trace(String.format("Cleaned resources allocated for %s ", getDataSinkContextName()));
    }

    @Override
    public String getServiceContextName() {
        return IOURIS.JDBC.getUri();
    }
}

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
package com.ericsson.component.aia.bps.spark.configuration;

import com.ericsson.component.aia.bps.core.validation.BpsConfigurationValidation;

/**
 * The Class SparkConfiguration.
 */
public class SparkConfiguration extends BpsConfigurationValidation {

    /** The uri. */
    protected String uri = null;

    /** The name. */
    private String name;

    /**
     * Gets the name.
     *
     * @return the name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Gets the uri.
     *
     * @return the uri
     */
    public String getUri() {
        return uri;
    }
}

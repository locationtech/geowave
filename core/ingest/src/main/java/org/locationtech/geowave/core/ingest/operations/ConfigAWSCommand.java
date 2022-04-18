/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.ingest.operations;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.ConfigSection;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "aws", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Create a local configuration for AWS S3")
public class ConfigAWSCommand extends DefaultOperation implements Command {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigAWSCommand.class);
  public static final String AWS_S3_ENDPOINT_PREFIX = "s3.endpoint";
  public static final String AWS_S3_ENDPOINT_URL = AWS_S3_ENDPOINT_PREFIX + ".url";

  @Parameter(description = "<AWS S3 endpoint URL> (for example s3.amazonaws.com)")
  private List<String> parameters = new ArrayList<>();

  private String url = null;

  @Override
  public boolean prepare(final OperationParams params) {
    boolean retval = true;
    retval |= super.prepare(params);

    return retval;
  }

  @Override
  public void execute(final OperationParams params) throws Exception {
    if (parameters.size() != 1) {
      throw new ParameterException("Requires argument: <AWS S3 endpoint URL>");
    }
    url = parameters.get(0);
    final Properties existingProps = getGeoWaveConfigProperties(params);

    // all switches are optional
    if (url != null) {
      existingProps.setProperty(AWS_S3_ENDPOINT_URL, url);
    }

    // Write properties file
    ConfigOptions.writeProperties(
        getGeoWaveConfigFile(params),
        existingProps,
        this.getClass(),
        AWS_S3_ENDPOINT_PREFIX,
        params.getConsole());
  }

  public static String getS3Url(final Properties configProperties) {

    String s3EndpointUrl = configProperties.getProperty(ConfigAWSCommand.AWS_S3_ENDPOINT_URL);
    if (s3EndpointUrl == null) {
      LOGGER.warn(
          "S3 endpoint URL is empty. Config using \"geowave config aws <s3 endpoint url>\"");

      s3EndpointUrl = "s3.amazonaws.com";
    }

    if (!s3EndpointUrl.contains("://")) {
      s3EndpointUrl = "s3://" + s3EndpointUrl;
    }

    return s3EndpointUrl;
  }

  public void setS3UrlParameter(final String s3EndpointUrl) {
    parameters = new ArrayList<>();
    parameters.add(s3EndpointUrl);
  }
}

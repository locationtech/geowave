/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.ingest;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import org.locationtech.geowave.core.ingest.URLIngestUtils.URLTYPE;
import org.locationtech.geowave.core.ingest.operations.ConfigAWSCommand;
import org.locationtech.geowave.core.store.ingest.IngestUrlHandlerSpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3IngestHandler implements IngestUrlHandlerSpi {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3IngestHandler.class);

  public S3IngestHandler() {}

  @Override
  public Path handlePath(final String inputPath, final Properties configProperties)
      throws IOException {
    // If input path is S3
    if (inputPath.startsWith("s3://")) {
      try {
        URLIngestUtils.setURLStreamHandlerFactory(URLTYPE.S3);
      } catch (NoSuchFieldException | SecurityException | IllegalArgumentException
          | IllegalAccessException e1) {
        LOGGER.error("Error in setting up S3URLStreamHandler Factory", e1);
        return null;
      }

      if (configProperties == null) {
        LOGGER.error("Unable to load config properties");
        return null;
      }
      String s3EndpointUrl = configProperties.getProperty(ConfigAWSCommand.AWS_S3_ENDPOINT_URL);
      if (s3EndpointUrl == null) {
        LOGGER.warn(
            "S3 endpoint URL is empty. Config using \"geowave config aws <s3 endpoint url>\"");
        s3EndpointUrl = "s3.amazonaws.com";
      }

      if (!s3EndpointUrl.contains("://")) {
        s3EndpointUrl = "s3://" + s3EndpointUrl;
      }

      return URLIngestUtils.setupS3FileSystem(inputPath, s3EndpointUrl);
    }

    return null;
  }

}

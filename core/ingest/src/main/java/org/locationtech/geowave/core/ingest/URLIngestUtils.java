/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.ingest;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import org.locationtech.geowave.mapreduce.s3.GeoWaveAmazonS3Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class URLIngestUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(URLIngestUtils.class);

  public static Path setupS3FileSystem(final String basePath, final String s3EndpointUrl)
      throws IOException {
    final FileSystem fs;
    try {
      fs = GeoWaveAmazonS3Factory.getFileSystem(new URI(s3EndpointUrl));
      // HP Fortify "Path Traversal" false positive
      // What Fortify considers "user input" comes only
      // from users with OS-level access anyway

    } catch (final URISyntaxException e) {
      LOGGER.error("Unable to ingest data, Inavlid S3 path");
      return null;
    }

    final String s3InputPath = basePath.replaceFirst("s3://", "/");
    try {
      return fs.getPath(s3InputPath);
    } catch (final InvalidPathException e) {
      LOGGER.error("Input valid input path " + s3InputPath);
      return null;
    }
  }
}

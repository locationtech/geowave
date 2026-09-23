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
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Collections;
import org.locationtech.geowave.mapreduce.s3.GeoWaveAmazonS3Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.upplication.s3fs.S3FileSystemProvider;

public class URLIngestUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(URLIngestUtils.class);

  public static Path setupS3FileSystem(final String basePath, final String s3EndpointUrl)
      throws IOException {
    Path path = null;
    FileSystem fs = null;
    try {
      fs =
          FileSystems.newFileSystem(
              new URI(s3EndpointUrl + "/"),
              Collections.singletonMap(
                  S3FileSystemProvider.AMAZON_S3_FACTORY_CLASS,
                  GeoWaveAmazonS3Factory.class.getName()),
              Thread.currentThread().getContextClassLoader());
      // HP Fortify "Path Traversal" false positive
      // What Fortify considers "user input" comes only
      // from users with OS-level access anyway

    } catch (final URISyntaxException e) {
      LOGGER.error("Unable to ingest data, Inavlid S3 path");
      return null;
    } catch (final FileSystemAlreadyExistsException e) {
      LOGGER.info("File system " + s3EndpointUrl + "already exists");
      try {
        fs = FileSystems.getFileSystem(new URI(s3EndpointUrl + "/"));
      } catch (final URISyntaxException e1) {
        LOGGER.error("Unable to ingest data, Inavlid S3 path");
        return null;
      }
    }

    final String s3InputPath = basePath.replaceFirst("s3://", "/");
    try {
      path = fs.getPath(s3InputPath);
    } catch (final InvalidPathException e) {
      LOGGER.error("Input valid input path " + s3InputPath);
      return null;
    }

    return path;
  }
}

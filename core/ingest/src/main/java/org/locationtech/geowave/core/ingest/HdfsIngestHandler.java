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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import org.locationtech.geowave.core.ingest.URLIngestUtils.URLTYPE;
import org.locationtech.geowave.core.store.ingest.IngestUrlHandlerSpi;
import org.locationtech.geowave.mapreduce.operations.ConfigHDFSCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsIngestHandler implements IngestUrlHandlerSpi {
  private static final Logger LOGGER = LoggerFactory.getLogger(HdfsIngestHandler.class);

  public HdfsIngestHandler() {}

  @Override
  public Path handlePath(final String inputPath, final Properties configProperties)
      throws IOException {
    // If input path is HDFS
    if (inputPath.startsWith("hdfs://")) {
      try {
        URLIngestUtils.setURLStreamHandlerFactory(URLTYPE.HDFS);
      } catch (final Error | NoSuchFieldException | SecurityException | IllegalArgumentException
          | IllegalAccessException e) {
        LOGGER.error("Error in setStreamHandlerFactory for HDFS", e);
        return null;
      }

      final String hdfsFSUrl = ConfigHDFSCommand.getHdfsUrl(configProperties);

      final String hdfsInputPath = inputPath.replaceFirst("hdfs://", "/");

      try {

        final URI uri = new URI(hdfsFSUrl + hdfsInputPath);
        // HP Fortify "Path Traversal" false positive
        // What Fortify considers "user input" comes only
        // from users with OS-level access anyway
        final Path path = Paths.get(uri);
        if (!Files.exists(path)) {
          LOGGER.error("Input path " + inputPath + " does not exist");
          return null;
        }
        return path;
      } catch (final URISyntaxException e) {
        LOGGER.error("Unable to ingest data, Inavlid HDFS Path", e);
        return null;
      }
    }
    return null;
  }

}

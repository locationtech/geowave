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
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLStreamHandlerFactory;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Collections;
import org.locationtech.geowave.mapreduce.hdfs.HdfsUrlStreamHandlerFactory;
import org.locationtech.geowave.mapreduce.s3.GeoWaveAmazonS3Factory;
import org.locationtech.geowave.mapreduce.s3.S3URLStreamHandlerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.upplication.s3fs.S3FileSystemProvider;

public class URLIngestUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(URLIngestUtils.class);

  public static enum URLTYPE {
    S3, HDFS
  }

  private static boolean hasS3Handler = false;
  private static boolean hasHdfsHandler = false;

  public static void setURLStreamHandlerFactory(final URLTYPE urlType) throws NoSuchFieldException,
      SecurityException, IllegalArgumentException, IllegalAccessException {
    // One-time init for each type
    if ((urlType == URLTYPE.S3) && hasS3Handler) {
      return;
    } else if ((urlType == URLTYPE.HDFS) && hasHdfsHandler) {
      return;
    }

    final Field lockField = URL.class.getDeclaredField("streamHandlerLock");
    // HP Fortify "Access Control" false positive
    // The need to change the accessibility here is
    // necessary, has been review and judged to be safe
    lockField.setAccessible(true);
    synchronized (lockField.get(null)) {

      // check again synchronized
      if ((urlType == URLTYPE.S3) && hasS3Handler) {
        return;
      } else if ((urlType == URLTYPE.HDFS) && hasHdfsHandler) {
        return;
      }

      final Field factoryField = URL.class.getDeclaredField("factory");
      // HP Fortify "Access Control" false positive
      // The need to change the accessibility here is
      // necessary, has been review and judged to be safe
      factoryField.setAccessible(true);

      final URLStreamHandlerFactory urlStreamHandlerFactory =
          (URLStreamHandlerFactory) factoryField.get(null);

      if (urlStreamHandlerFactory == null) {
        if (urlType == URLTYPE.S3) {
          URL.setURLStreamHandlerFactory(new S3URLStreamHandlerFactory());
          hasS3Handler = true;
        } else { // HDFS
          URL.setURLStreamHandlerFactory(new HdfsUrlStreamHandlerFactory());
          hasHdfsHandler = true;
        }

      } else {
        factoryField.set(null, null);

        if (urlType == URLTYPE.S3) {
          URL.setURLStreamHandlerFactory(new S3URLStreamHandlerFactory(urlStreamHandlerFactory));
          hasS3Handler = true;
        } else { // HDFS
          URL.setURLStreamHandlerFactory(new HdfsUrlStreamHandlerFactory(urlStreamHandlerFactory));
          hasHdfsHandler = true;
        }
      }
    }
  }

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

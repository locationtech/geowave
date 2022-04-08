/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.landsat8;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownloadRunner extends AnalyzeRunner {
  private static final Logger LOGGER = LoggerFactory.getLogger(DownloadRunner.class);
  private static final int DOWNLOAD_RETRY = 5;
  private static final String DOWNLOAD_DIRECTORY = "images";
  protected Landsat8DownloadCommandLineOptions downloadOptions;

  public DownloadRunner(
      final Landsat8BasicCommandLineOptions analyzeOptions,
      final Landsat8DownloadCommandLineOptions downloadOptions) {
    super(analyzeOptions);
    this.downloadOptions = downloadOptions;
  }

  @Override
  protected void nextBand(final SimpleFeature band, final AnalysisInfo analysisInfo) {
    super.nextBand(band, analysisInfo);
    final String downloadUrl =
        (String) band.getAttribute(BandFeatureIterator.BAND_DOWNLOAD_ATTRIBUTE_NAME);
    final File localPath = getDownloadFile(band, landsatOptions.getWorkspaceDir());
    if (localPath.exists()) {
      if (downloadOptions.isOverwriteIfExists()) {
        if (!localPath.delete()) {
          LOGGER.warn("Unable to delete file '" + localPath.getAbsolutePath() + "'");
        }
      } else {
        return;
      }
    }
    final File localTempPath = getDownloadTempFile(band, landsatOptions.getWorkspaceDir());
    if (localTempPath.exists()) {
      if (!localTempPath.delete()) {
        LOGGER.error("Unable to delete file '" + localTempPath.getAbsolutePath() + "'");
      }
    }
    if (!localPath.getParentFile().exists() && !localPath.getParentFile().mkdirs()) {
      LOGGER.warn(
          "Unable to create directory '" + localPath.getParentFile().getAbsolutePath() + "'");
    }
    InputStream in = null;
    // first download the gzipped file
    int retry = 0;
    boolean success = false;
    while (!success && (retry < DOWNLOAD_RETRY)) {
      try {
        if (retry > 0) {
          // wait for a second
          Thread.sleep(1000L);
        }
        final URLConnection connection = new URL(downloadUrl).openConnection();
        connection.setConnectTimeout(360_000);
        connection.setReadTimeout(360_000);
        in = connection.getInputStream();
        success = true;

        final FileOutputStream outStream = new FileOutputStream(localTempPath);
        IOUtils.copyLarge(in, outStream);
        outStream.close();
        FileUtils.moveFile(localTempPath, localPath);
      } catch (final IOException | InterruptedException e) {
        LOGGER.error(
            "Unable to read image from public S3 '" + downloadUrl + "'; retry round " + ++retry,
            e);
      } finally {
        if (in != null) {
          IOUtils.closeQuietly(in);
        }
      }
    }
  }

  protected static File getDownloadTempFile(
      final SimpleFeature band,
      final String workspaceDirectory) {
    final File file = getDownloadFile(band, workspaceDirectory);
    return new File(file.getParentFile(), file.getName() + ".download");
  }

  protected static File getDownloadFile(final SimpleFeature band, final String workspaceDirectory) {
    final int path = (int) band.getAttribute(SceneFeatureIterator.PATH_ATTRIBUTE_NAME);
    final int row = (int) band.getAttribute(SceneFeatureIterator.ROW_ATTRIBUTE_NAME);
    final String product =
        (String) band.getAttribute(SceneFeatureIterator.PRODUCT_ID_ATTRIBUTE_NAME);
    return new File(
        workspaceDirectory
            + File.separator
            + DOWNLOAD_DIRECTORY
            + File.separator
            + path
            + File.separator
            + row
            + File.separator
            + product
            + File.separator
            + band.getID()
            + ".TIF");
  }
}

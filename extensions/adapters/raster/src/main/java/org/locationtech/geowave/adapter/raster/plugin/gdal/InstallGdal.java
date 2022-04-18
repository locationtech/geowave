/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.plugin.gdal;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.util.Locale;
import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.archiver.tar.TarGZipUnArchiver;
import org.codehaus.plexus.logging.console.ConsoleLogger;
import org.locationtech.geowave.adapter.raster.util.ZipUtils;
import org.slf4j.LoggerFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class InstallGdal {
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(InstallGdal.class);

  public static final File DEFAULT_TEMP_DIR = new File("./target/temp");
  private static final String GDAL_ENV = "baseGdalDownload";
  // this has some of the content from
  // http://demo.geo-solutions.it/share/github/imageio-ext/releases/1.1.X/1.1.7/native/gdal

  // rehosted, with all supplemental files to retain the credit (just to
  // lessen the burden of additional network traffic imposed on this external
  // server)
  private static final String DEFAULT_BASE =
      "https://s3.amazonaws.com/geowave/third-party-downloads/gdal";

  public static void main(final String[] args) throws IOException {
    File gdalDir = null;
    if ((args != null) && (args.length > 0) && (args[0] != null) && !args[0].trim().isEmpty()) {
      gdalDir = new File(args[0]);
      // HP Fortify "Path Traversal" false positive
      // What Fortify considers "user input" comes only
      // from users with OS-level access anyway
    } else {
      gdalDir = new File(DEFAULT_TEMP_DIR, "gdal");
    }

    if (gdalDir.exists() && gdalDir.isDirectory()) {
      final File[] files = gdalDir.listFiles();
      if ((files != null) && (files.length > 1)) {
        System.out.println("GDAL already exists");
        return;
      } else {
        LOGGER.error(
            "Directory "
                + gdalDir.getAbsolutePath()
                + " exists but does not contain GDAL, consider deleting directory or choosing a different one.");
      }
    }

    if (!gdalDir.mkdirs()) {
      LOGGER.warn("unable to create directory " + gdalDir.getAbsolutePath());
    }

    install(gdalDir);
  }

  @SuppressFBWarnings(value = "REC_CATCH_EXCEPTION")
  private static void install(final File gdalDir) throws IOException {
    URL url;
    String file;
    String gdalEnv = System.getProperty(GDAL_ENV);
    if ((gdalEnv == null) || gdalEnv.trim().isEmpty()) {
      gdalEnv = DEFAULT_BASE;
    }
    if (isWindows()) {
      file = "win-x64-gdal204.zip";
      url = new URL(gdalEnv + "/windows/MSVC2017/" + file);
    } else if (isMac()) {
      file = "gdal-1.9.2_macOSX.zip";
      url = new URL(gdalEnv + "/mac/" + file);
    } else {
      file = "linux-libgdal26.tar.gz";
      url = new URL(gdalEnv + "/linux/" + file);
    }
    final File downloadFile = new File(gdalDir, file);
    if (downloadFile.exists() && (downloadFile.length() < 1)) {
      // its corrupt, delete it
      if (!downloadFile.delete()) {
        LOGGER.warn(
            "File '" + downloadFile.getAbsolutePath() + "' is corrupt and cannot be deleted");
      }
    }
    System.out.println("Downloading GDAL native libraries...");
    if (!downloadFile.exists()) {
      boolean success = false;
      for (int i = 0; i < 3; i++) {
        try (FileOutputStream fos = new FileOutputStream(downloadFile)) {
          final URLConnection connection = url.openConnection();
          connection.setConnectTimeout(360_000);
          connection.setReadTimeout(360_000);
          IOUtils.copyLarge(connection.getInputStream(), fos);
          fos.flush();
          success = true;
          break;
        } catch (final Exception e) {
          LOGGER.warn("Unable to download url '" + url + "'. Retry attempt #" + i);
        }
      }
      if (!success) {
        LOGGER.error("Unable to download url '" + url + "' after 3 attempts.");
        System.exit(-1);
      }
    }
    if (file.endsWith("zip")) {
      ZipUtils.unZipFile(downloadFile, gdalDir.getAbsolutePath(), false);
    } else {
      final TarGZipUnArchiver unarchiver = new TarGZipUnArchiver();
      unarchiver.enableLogging(
          new ConsoleLogger(org.codehaus.plexus.logging.Logger.LEVEL_WARN, "GDAL Unarchive"));
      unarchiver.setSourceFile(downloadFile);
      unarchiver.setDestDirectory(gdalDir);
      unarchiver.extract();
      // the symbolic links are not working, programmatically re-create
      // them
      final File[] links = gdalDir.listFiles(new FileFilter() {
        @Override
        public boolean accept(final File pathname) {
          return pathname.length() <= 0;
        }
      });
      if (links != null) {
        final File[] actualLibs = gdalDir.listFiles(new FileFilter() {
          @Override
          public boolean accept(final File pathname) {
            return pathname.length() > 0;
          }
        });
        for (final File link : links) {
          // find an actual lib that matches
          for (final File lib : actualLibs) {
            if (lib.getName().startsWith(link.getName())) {
              if (link.delete()) {
                Files.createSymbolicLink(
                    link.getAbsoluteFile().toPath(),
                    lib.getAbsoluteFile().toPath());
              }
              break;
            }
          }
        }
      }
    }
    if (!downloadFile.delete()) {
      LOGGER.warn("cannot delete " + downloadFile.getAbsolutePath());
    }
    System.out.println("GDAL installed in directory " + gdalDir.getAbsolutePath());
  }

  private static boolean isWindows() {
    final String OS = System.getProperty("os.name", "generic").toLowerCase(Locale.ENGLISH);
    return (OS.indexOf("win") > -1);
  }

  private static boolean isMac() {
    final String OS = System.getProperty("os.name", "generic").toLowerCase(Locale.ENGLISH);
    return (OS.indexOf("mac") >= 0);

  }

}

/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.dynamodb.cli;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.archiver.tar.TarGZipUnArchiver;
import org.codehaus.plexus.logging.console.ConsoleLogger;
import org.slf4j.LoggerFactory;
import com.jcraft.jsch.Logger;

public class DynamoDBLocal {
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(DynamoDBLocal.class);

  // AWS's documented download. Despite the path it serves the current release, 3.x, which is built
  // on SDK v2, needs Java 17 and has native SQLite for Apple silicon. The old S3 location stopped
  // at 1.25.
  private static final String DYNDB_URL = "https://d1ni2b6xgvw0s0.cloudfront.net/v2.x/";
  private static final String DYNDB_TAR = "dynamodb_local_latest.tar.gz";
  public static final int DEFAULT_PORT = 8000;

  private static final long EMULATOR_SPINUP_DELAY_MS = 30000L;
  private static final long EMULATOR_SHUTDOWN_TIMEOUT_MS = 30000L;
  public static final File DEFAULT_DIR = new File("./temp");

  private final File dynLocalDir;
  private final int port;
  private ExecuteWatchdog watchdog;
  private DefaultExecuteResultHandler resultHandler;

  public DynamoDBLocal() {
    this(null, null);
  }

  public DynamoDBLocal(final String localDir) {
    this(localDir, null);
  }

  public DynamoDBLocal(final int port) {
    this(null, port);
  }

  public DynamoDBLocal(final String localDir, final Integer port) {
    if ((localDir != null) && !localDir.isEmpty()) {
      dynLocalDir = new File(localDir);
    } else {
      dynLocalDir = new File(DEFAULT_DIR, "dynamodb");
    }
    if (port != null) {
      this.port = port;
    } else {
      this.port = DEFAULT_PORT;
    }
    if (!dynLocalDir.exists() && !dynLocalDir.mkdirs()) {
      LOGGER.warn("unable to create directory " + dynLocalDir.getAbsolutePath());
    }
  }

  public boolean start() {
    if (!isInstalled()) {
      try {
        if (!install()) {
          return false;
        }
      } catch (final IOException e) {
        LOGGER.error(e.getMessage());
        return false;
      }
    }

    try {
      startDynamoLocal();
    } catch (IOException | InterruptedException e) {
      LOGGER.error(e.getMessage());
      return false;
    }
    if (resultHandler.hasResult()) {
      // such as when the port is still taken
      LOGGER.error(
          "DynamoDB Local exited on startup with exit value " + resultHandler.getExitValue(),
          resultHandler.getException());
      return false;
    }

    return true;
  }

  public boolean isRunning() {
    return ((watchdog != null) && watchdog.isWatching());
  }

  public void stop() {
    watchdog.destroyProcess();
    // Destroying only signals the process. A start straight after would race it for the port,
    // lose, and exit, leaving nothing listening.
    try {
      resultHandler.waitFor(EMULATOR_SHUTDOWN_TIMEOUT_MS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    if (!resultHandler.hasResult()) {
      LOGGER.warn(
          "DynamoDB Local did not exit within " + EMULATOR_SHUTDOWN_TIMEOUT_MS + " ms of stopping");
    }
  }

  private boolean isInstalled() {
    final File dynLocalJar = new File(dynLocalDir, "DynamoDBLocal.jar");

    return (dynLocalJar.canRead());
  }

  protected boolean install() throws IOException {
    HttpURLConnection.setFollowRedirects(true);
    final URL url = new URL(DYNDB_URL + DYNDB_TAR);

    final File downloadFile = new File(dynLocalDir, DYNDB_TAR);
    if (!downloadFile.exists()) {
      try (FileOutputStream fos = new FileOutputStream(downloadFile)) {
        IOUtils.copyLarge(url.openStream(), fos);
        fos.flush();
      }
    }

    final TarGZipUnArchiver unarchiver = new TarGZipUnArchiver();
    unarchiver.enableLogging(new ConsoleLogger(Logger.WARN, "DynamoDB Local Unarchive"));
    unarchiver.setSourceFile(downloadFile);
    unarchiver.setDestDirectory(dynLocalDir);
    unarchiver.extract();

    if (!downloadFile.delete()) {
      LOGGER.warn("cannot delete " + downloadFile.getAbsolutePath());
    }

    // Check the install
    if (!isInstalled()) {
      LOGGER.error("DynamoDB Local install failed");
      return false;
    }

    return true;
  }

  /**
   * Using apache commons exec for cmd line execution
   *
   * @param command
   * @return exitCode
   * @throws ExecuteException
   * @throws IOException
   * @throws InterruptedException
   */
  private void startDynamoLocal() throws ExecuteException, IOException, InterruptedException {
    // java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar
    // -sharedDb
    // this JVM's own java, since the one on the path may be older than DynamoDB Local's minimum
    final CommandLine cmdLine =
        new CommandLine(new File(System.getProperty("java.home"), "bin/java").getPath());

    cmdLine.addArgument("-Djava.library.path=" + dynLocalDir + "/DynamoDBLocal_lib");
    cmdLine.addArgument("-jar");
    cmdLine.addArgument(dynLocalDir + "/DynamoDBLocal.jar");
    cmdLine.addArgument("-sharedDb");
    cmdLine.addArgument("-inMemory");
    // otherwise it reports usage to AWS and leaves a file in the working directory
    cmdLine.addArgument("-disableTelemetry");
    cmdLine.addArgument("-port");
    cmdLine.addArgument(Integer.toString(port));
    // DynamoDB Local accepts any credentials, but a client needs some to sign with
    System.setProperty("aws.accessKeyId", "dummy");
    System.setProperty("aws.secretAccessKey", "dummy");

    // Using a result handler makes the emulator run async
    resultHandler = new DefaultExecuteResultHandler();

    // watchdog shuts down the emulator, later
    watchdog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
    final Executor executor = new DefaultExecutor();
    executor.setWatchdog(watchdog);
    executor.execute(cmdLine, resultHandler);

    // we need to wait here for a bit, in case the emulator needs to update
    // itself
    Thread.sleep(EMULATOR_SPINUP_DELAY_MS);
  }
}

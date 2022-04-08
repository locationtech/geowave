/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.bigtable.cli;

import java.io.File;
import java.io.IOException;
import com.beust.jcommander.Parameter;

public class RunBigtableEmulatorOptions {
  @Parameter(names = {"--directory", "-d"}, description = "The directory to use for Bigtable")
  private String directory = BigtableEmulator.DEFAULT_DIR.getPath();


  @Parameter(names = {"--url", "-u"}, description = "The url location to download Bigtable")
  private String url = "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads";

  @Parameter(names = {"--sdk", "-s"}, description = "The name of the Bigtable SDK")
  private String sdk = "google-cloud-sdk-183.0.0-linux-x86_64.tar.gz";

  @Parameter(names = {"--port", "-p"}, description = "The port the emulator will run on")
  private String port = "127.0.0.1:8086";

  public String getDirectory() {
    return directory;
  }

  public String getUrl() {
    return url;
  }

  public String getSdk() {
    return sdk;
  }

  public String getPort() {
    return port;
  }

  public void setDirectory(String directory) {
    this.directory = directory;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public void setSdk(String sdk) {
    this.sdk = sdk;
  }

  public void setPort(String port) {
    this.port = port;
  }

  public BigtableEmulator getServer() throws IOException {
    return new BigtableEmulator(this);
  }
}

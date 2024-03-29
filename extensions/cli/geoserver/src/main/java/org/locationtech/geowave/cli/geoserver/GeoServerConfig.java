/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver;

import static org.locationtech.geowave.cli.geoserver.constants.GeoServerConstants.GEOSERVER_PASS;
import static org.locationtech.geowave.cli.geoserver.constants.GeoServerConstants.GEOSERVER_URL;
import static org.locationtech.geowave.cli.geoserver.constants.GeoServerConstants.GEOSERVER_USER;
import static org.locationtech.geowave.cli.geoserver.constants.GeoServerConstants.GEOSERVER_WORKSPACE;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Properties;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.operations.config.security.utils.SecurityUtils;
import org.locationtech.geowave.core.cli.utils.URLUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.internal.Console;

public class GeoServerConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoServerConfig.class);

  public static final String DEFAULT_URL = "localhost:8080";
  public static final String DEFAULT_USER = "admin";
  public static final String DEFAULT_PASS = "geoserver";
  public static final String DEFAULT_WORKSPACE = "geowave";
  public static final String DEFAULT_CS = "-raster";
  public static final String DEFAULT_DS = "-vector";

  public static final String DISPLAY_NAME_PREFIX = "GeoWave Datastore - ";
  public static final String QUERY_INDEX_STRATEGY_KEY = "Query Index Strategy";

  private String url = null;
  private String user = null;
  private String pass = null;
  private String workspace = null;

  private final File propFile;
  private final Properties gsConfigProperties;

  /**
   * Properties File holds defaults; updates config if empty.
   *
   * @param propFile
   */
  public GeoServerConfig(final File propFile, final Console console) {
    this.propFile = propFile;

    if ((propFile != null) && propFile.exists()) {
      gsConfigProperties = ConfigOptions.loadProperties(propFile);
    } else {
      gsConfigProperties = new Properties();
    }
    boolean update = false;

    url = gsConfigProperties.getProperty(GEOSERVER_URL);
    if (url == null) {
      url = DEFAULT_URL;
      gsConfigProperties.setProperty(GEOSERVER_URL, url);
      update = true;
    }

    user = gsConfigProperties.getProperty(GEOSERVER_USER);
    if (user == null) {
      user = DEFAULT_USER;
      gsConfigProperties.setProperty(GEOSERVER_USER, user);
      update = true;
    }

    pass = gsConfigProperties.getProperty(GEOSERVER_PASS);
    if (pass == null) {
      pass = DEFAULT_PASS;
      gsConfigProperties.setProperty(GEOSERVER_PASS, pass);
      update = true;
    } else {
      try {
        final File resourceTokenFile = SecurityUtils.getFormattedTokenKeyFileForConfig(propFile);
        // if password in config props is encrypted, need to decrypt it
        pass =
            SecurityUtils.decryptHexEncodedValue(
                pass,
                resourceTokenFile.getCanonicalPath(),
                console);
      } catch (final Exception e) {
        LOGGER.error("An error occurred decrypting password: " + e.getLocalizedMessage(), e);
      }
    }

    workspace = gsConfigProperties.getProperty(GEOSERVER_WORKSPACE);
    if (workspace == null) {
      workspace = DEFAULT_WORKSPACE;
      gsConfigProperties.setProperty(GEOSERVER_WORKSPACE, workspace);
      update = true;
    }

    if (update) {
      ConfigOptions.writeProperties(propFile, gsConfigProperties, console);

      LOGGER.info("GeoServer Config Saved");
    }
  }

  /** Secondary no-arg constructor for direct-access testing */
  public GeoServerConfig(final Console console) {
    this(ConfigOptions.getDefaultPropertyFile(console), console);
  }

  public String getUrl() {
    String internalUrl;
    if (!url.contains("//")) {
      internalUrl = url + "/geoserver";
    } else {
      internalUrl = url;
    }
    try {
      return URLUtils.getUrl(internalUrl);
    } catch (MalformedURLException | URISyntaxException e) {
      LOGGER.error("Error discovered in validating specified url: " + e.getLocalizedMessage(), e);
      return internalUrl;
    }
  }

  public void setUrl(final String url) {
    this.url = url;
  }

  public String getUser() {
    return user;
  }

  public void setUser(final String user) {
    this.user = user;
  }

  public String getPass() {
    return pass;
  }

  public void setPass(final String pass) {
    this.pass = pass;
  }

  public String getWorkspace() {
    return workspace;
  }

  public void setWorkspace(final String workspace) {
    this.workspace = workspace;
  }

  public File getPropFile() {
    return propFile;
  }

  public Properties getGsConfigProperties() {
    return gsConfigProperties;
  }
}

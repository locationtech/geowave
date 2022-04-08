/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
/** */
package org.locationtech.geowave.core.cli.converters;

import com.beust.jcommander.converters.BaseConverter;
import com.beust.jcommander.internal.Console;
import com.beust.jcommander.internal.DefaultConsole;
import com.beust.jcommander.internal.JDK6Console;
import org.locationtech.geowave.core.cli.Constants;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.utils.PropertiesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.lang.reflect.Method;
import java.util.Properties;

/**
 * Base value converter for handling field conversions of varying types
 *
 * @param <T>
 */
public abstract class GeoWaveBaseConverter<T> extends BaseConverter<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveBaseConverter.class);

  private String propertyKey;
  private static Console console;
  private static Properties properties;

  public GeoWaveBaseConverter() {
    super("");
    init();
  }

  public GeoWaveBaseConverter(final String optionName) {
    super(optionName);
    init();
  }

  private void init() {
    File propertyFile = null;
    if (new ConfigOptions().getConfigFile() != null) {
      propertyFile = new File(new ConfigOptions().getConfigFile());
    } else {
      propertyFile = ConfigOptions.getDefaultPropertyFile(getConsole());
    }
    if ((propertyFile != null) && propertyFile.exists()) {
      setProperties(ConfigOptions.loadProperties(propertyFile));
    }
  }

  protected static Console getConsole() {
    if (console == null) {
      try {
        Method consoleMethod = System.class.getDeclaredMethod("console");
        Object systemConsole = consoleMethod.invoke(null);
        if (systemConsole == null) {
          console = new DefaultConsole();
        } else {
          console = new JDK6Console(systemConsole);
        }
      } catch (Throwable t) {
        console = new DefaultConsole();
      }
    }
    return console;
  }

  /**
   * Prompt a user for a standard value and return the input.
   *
   * @param promptMessage the prompt message
   * @return the value that was read
   */
  public static String promptAndReadValue(final String promptMessage) {
    LOGGER.trace("ENTER :: promptAndReadValue()");
    final PropertiesUtils propsUtils = new PropertiesUtils(getProperties());
    final boolean defaultEchoEnabled =
        propsUtils.getBoolean(Constants.CONSOLE_DEFAULT_ECHO_ENABLED_KEY, false);
    LOGGER.debug(
        "Default console echo is {}",
        new Object[] {defaultEchoEnabled ? "enabled" : "disabled"});
    getConsole().print(promptMessage);
    char[] responseChars = getConsole().readPassword(defaultEchoEnabled);
    final String response = new String(responseChars);
    responseChars = null;

    return response;
  }

  /**
   * Prompt a user for a password and return the input.
   *
   * @param promptMessage the prompt message
   * @return the value that was read
   */
  public static String promptAndReadPassword(final String promptMessage) {
    LOGGER.trace("ENTER :: promptAndReadPassword()");
    final PropertiesUtils propsUtils = new PropertiesUtils(getProperties());
    final boolean defaultEchoEnabled =
        propsUtils.getBoolean(Constants.CONSOLE_DEFAULT_ECHO_ENABLED_KEY, false);
    final boolean passwordEchoEnabled =
        propsUtils.getBoolean(Constants.CONSOLE_PASSWORD_ECHO_ENABLED_KEY, defaultEchoEnabled);
    LOGGER.debug(
        "Password console echo is {}",
        new Object[] {passwordEchoEnabled ? "enabled" : "disabled"});
    getConsole().print(promptMessage);
    char[] passwordChars = getConsole().readPassword(passwordEchoEnabled);
    final String strPassword = new String(passwordChars);
    passwordChars = null;

    return strPassword;
  }

  /** @return the propertyKey */
  public String getPropertyKey() {
    return propertyKey;
  }

  /** @param propertyKey the propertyKey to set */
  public void setPropertyKey(final String propertyKey) {
    this.propertyKey = propertyKey;
  }

  /**
   * Specify if a converter is for a password field. This allows a password field to be specified,
   * though side-stepping most of the default jcommander password functionality.
   *
   * @return {@code true} if the converter is for a password field
   */
  public boolean isPassword() {
    return false;
  }

  /**
   * Specify if a field is required.
   *
   * @return {@code true} if the field is required
   */
  public boolean isRequired() {
    return false;
  }

  /** @return the properties */
  private static Properties getProperties() {
    return properties;
  }

  /** @param properties the properties to set */
  private void setProperties(final Properties props) {
    properties = props;
  }
}

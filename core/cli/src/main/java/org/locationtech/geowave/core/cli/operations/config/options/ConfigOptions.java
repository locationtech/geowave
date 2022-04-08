/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.operations.config.options;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import org.locationtech.geowave.core.cli.Constants;
import org.locationtech.geowave.core.cli.VersionUtils;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.security.utils.SecurityUtils;
import org.locationtech.geowave.core.cli.utils.JCommanderParameterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Console;

/**
 * Config options allows the user to override the default location for configuration options, and
 * also allows commands to load the properties needed for running the program.
 */
public class ConfigOptions {
  public static final String CHARSET = "ISO-8859-1";

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigOptions.class);

  public static final String PROPERTIES_FILE_CONTEXT = "properties-file";
  public static final String GEOWAVE_CACHE_PATH = ".geowave";
  public static final String GEOWAVE_CACHE_FILE = "config.properties";

  /** Allow the user to override the config file location */
  @Parameter(
      names = {"-cf", "--config-file"},
      description = "Override configuration file (default is <home>/.geowave/config.properties)")
  private String configFile;

  public ConfigOptions() {}

  public String getConfigFile() {
    return configFile;
  }

  public void setConfigFile(final String configFilePath) {
    configFile = configFilePath;
  }

  /**
   * The default property file is in the user's home directory, in the .geowave folder.
   *
   * @return a property file in the user's home directory
   */
  public static File getDefaultPropertyPath() {
    // File location
    // HP Fortify "Path Manipulation" false positive
    // What Fortify considers "user input" comes only
    // from users with OS-level access anyway
    final String cachePath =
        String.format(
            "%s%s%s",
            System.getProperty("user.home"),
            File.separator,
            GEOWAVE_CACHE_PATH);
    return new File(cachePath);
  }

  /**
   * The default property file is in the user's home directory, in the .geowave folder. If the
   * version can not be found the first available property file in the folder is used.
   *
   * @return the default property file
   */
  public static File getDefaultPropertyFile() {
    return getDefaultPropertyFile(null);
  }

  /**
   * The default property file is in the user's home directory, in the .geowave folder. If the
   * version can not be found the first available property file in the folder is used.
   *
   * @param console console to print output to
   *
   * @return the default property file
   */
  public static File getDefaultPropertyFile(final Console console) {
    // HP Fortify "Path Manipulation" false positive
    // What Fortify considers "user input" comes only
    // from users with OS-level access anyway
    final File defaultPath = getDefaultPropertyPath();
    final String version = VersionUtils.getVersion(console);
    if (version != null) {
      return formatConfigFile(version, defaultPath);
    } else {
      final String[] configFiles = defaultPath.list(new FilenameFilter() {

        @Override
        public boolean accept(final File dir, final String name) {
          return name.endsWith("-config.properties");
        }
      });
      if ((configFiles != null) && (configFiles.length > 0)) {
        final String backupVersion = configFiles[0].substring(0, configFiles[0].length() - 18);
        return formatConfigFile(backupVersion, defaultPath);
      } else {
        return formatConfigFile("unknownversion", defaultPath);
      }
    }
  }

  /**
   * Configures a File based on a given path name and version
   *
   * @param version
   * @param defaultPath
   * @return Configured File
   */
  public static File formatConfigFile(final String version, final File defaultPath) {
    // HP Fortify "Path Manipulation" false positive
    // What Fortify considers "user input" comes only
    // from users with OS-level access anyway
    final String configFile =
        String.format(
            "%s%s%s%s%s",
            defaultPath.getAbsolutePath(),
            File.separator,
            version,
            "-",
            GEOWAVE_CACHE_FILE);
    return new File(configFile);
  }

  public static boolean writeProperties(
      final File configFile,
      final Properties properties,
      final Class<?> clazz,
      final String namespacePrefix,
      final Console console) {
    try {
      final Properties tmp = new Properties() {
        private static final long serialVersionUID = 1L;

        @Override
        public Set<Object> keySet() {
          return Collections.unmodifiableSet(new TreeSet<>(super.keySet()));
        }

        @Override
        public synchronized Enumeration<Object> keys() {
          return Collections.enumeration(new TreeSet<>(super.keySet()));
        }
      };

      // check if encryption is enabled - it is by default and would need
      // to be explicitly disabled
      if (Boolean.parseBoolean(
          properties.getProperty(
              Constants.ENCRYPTION_ENABLED_KEY,
              Constants.ENCRYPTION_ENABLED_DEFAULT))) {
        // check if any values exist that need to be encrypted before
        // written to properties
        if (clazz != null) {
          final Field[] fields = clazz.getDeclaredFields();
          for (final Field field : fields) {
            for (final Annotation annotation : field.getAnnotations()) {
              if (annotation.annotationType() == Parameter.class) {
                final Parameter parameter = (Parameter) annotation;

                if (JCommanderParameterUtils.isPassword(parameter)) {
                  final String storeFieldName =
                      ((namespacePrefix != null) && !"".equals(namespacePrefix.trim()))
                          ? namespacePrefix + "." + field.getName()
                          : field.getName();
                  if (properties.containsKey(storeFieldName)) {
                    final String value = properties.getProperty(storeFieldName);
                    String encryptedValue = value;
                    try {
                      final File tokenFile =
                          SecurityUtils.getFormattedTokenKeyFileForConfig(configFile);
                      encryptedValue =
                          SecurityUtils.encryptAndHexEncodeValue(
                              value,
                              tokenFile.getAbsolutePath(),
                              console);
                    } catch (final Exception e) {
                      LOGGER.error(
                          "An error occurred encrypting specified password value: "
                              + e.getLocalizedMessage(),
                          e);
                      encryptedValue = value;
                    }
                    properties.setProperty(storeFieldName, encryptedValue);
                  }
                }
              }
            }
          }
        }
      }

      tmp.putAll(properties);
      try (FileOutputStream str = new FileOutputStream(configFile)) {
        tmp.store(
            // HPFortify FP: passwords are stored encrypted
            str,
            null);
      }
    } catch (final FileNotFoundException e) {
      LOGGER.error("Could not find the property file.", e);
      return false;
    } catch (final IOException e) {
      LOGGER.error("Exception writing property file.", e);
      return false;
    }
    return true;
  }

  /**
   * Write the given properties to the file, and log an error if an exception occurs.
   *
   * @return true if success, false if failure
   */
  public static boolean writeProperties(
      final File configFile,
      final Properties properties,
      final Console console) {
    return writeProperties(configFile, properties, null, null, console);
  }

  /**
   * This helper function will load the properties file, or return null if it can't. It's designed
   * to be used by other commands.
   */
  public static Properties loadProperties(final File configFile) {
    return loadProperties(configFile, null);
  }

  /**
   * This helper function will load the properties file, or return null if it can't. It's designed
   * to be used by other commands.
   */
  public static Properties loadProperties(final File configFile, final String pattern) {

    // Load the properties file.
    final Properties properties = new Properties();
    if (configFile.exists()) {
      Pattern p = null;
      if (pattern != null) {
        p = Pattern.compile(pattern);
      }
      InputStream is = null;
      try {
        if (p != null) {
          try (FileInputStream input = new FileInputStream(configFile);
              Scanner s = new Scanner(input, CHARSET)) {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final PrintWriter writer = new PrintWriter(new OutputStreamWriter(out, CHARSET));
            while (s.hasNext()) {
              final String line = s.nextLine();
              if (p.matcher(line).find()) {
                writer.println(line);
              }
            }
            writer.flush();
            is = new ByteArrayInputStream(out.toByteArray());
          }
        } else {
          is = new FileInputStream(configFile);
        }

        properties.load(is);
      } catch (final IOException e) {
        LOGGER.error("Could not find property cache file: " + configFile, e);

        return null;
      } finally {
        if (is != null) {
          try {
            is.close();
          } catch (final IOException e) {
            LOGGER.error(e.getMessage(), e);
          }
        }
      }
    }
    return properties;
  }

  /**
   * Load the properties file into the input params.
   *
   * @param inputParams
   */
  public void prepare(final OperationParams inputParams) {
    File propertyFile = null;
    if (getConfigFile() != null) {
      propertyFile = new File(getConfigFile());
    } else {
      propertyFile = getDefaultPropertyFile(inputParams.getConsole());
    }

    // Set the properties on the context.
    inputParams.getContext().put(PROPERTIES_FILE_CONTEXT, propertyFile);
  }
}

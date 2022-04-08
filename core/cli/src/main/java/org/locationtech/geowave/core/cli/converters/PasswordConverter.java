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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.utils.FileUtils;
import org.locationtech.geowave.core.cli.utils.PropertiesUtils;
import com.beust.jcommander.ParameterException;

/**
 * This class will allow support for user's passing in passwords through a variety of ways. Current
 * supported options for passwords include standard password input (pass), an environment variable
 * (env), a file containing the password text (file), a properties file containing the password
 * associated with a specific key (propfile), and the user being prompted to enter the password at
 * command line (stdin). <br> <br> Required notation for specifying varying inputs are:
 *
 * <ul> <li><b>pass</b>:&lt;password&gt; <li><b>env</b>:&lt;variable containing the password&gt;
 * <li><b>file</b>:&lt;local file containing the password&gt; <li><b>propfile</b>:&lt;local
 * properties file containing the password&gt;<b>:</b>&lt;property file key&gt; <li><b>stdin</b>
 * </ul>
 */
public class PasswordConverter extends GeoWaveBaseConverter<String> {
  public PasswordConverter(final String optionName) {
    super(optionName);
  }

  /*
   * HP Fortify "Use of Hard-coded Password - Password Management: Hardcoded Password" false
   * positive This is not a hard-coded password, just a description telling users options they have
   * for entering a password
   */
  public static final String DEFAULT_PASSWORD_DESCRIPTION =
      "Can be specified as 'pass:<password>', 'file:<local file containing the password>', "
          + "'propfile:<local properties file containing the password>:<property file key>', 'env:<variable containing the pass>', or stdin";
  public static final String STDIN = "stdin";
  private static final String SEPARATOR = ":";

  private enum KeyType {
    PASS("pass" + SEPARATOR) {
      @Override
      String process(final String password) {
        return password;
      }
    },
    ENV("env" + SEPARATOR) {
      @Override
      String process(final String envVariable) {
        return System.getenv(envVariable);
      }
    },
    FILE("file" + SEPARATOR) {
      @Override
      String process(final String value) {
        try {
          final String password = FileUtils.readFileContent(new File(value));
          if ((password != null) && !"".equals(password.trim())) {
            return password;
          }
        } catch (final Exception ex) {
          throw new ParameterException(ex);
        }
        return null;
      }
    },
    PROPFILE("propfile" + SEPARATOR) {
      @Override
      String process(final String value) {
        if ((value != null) && !"".equals(value.trim())) {
          if (value.indexOf(SEPARATOR) != -1) {
            String propertyFilePath = value.split(SEPARATOR)[0];
            String propertyKey = value.split(SEPARATOR)[1];
            if ((propertyFilePath != null) && !"".equals(propertyFilePath.trim())) {
              propertyFilePath = propertyFilePath.trim();
              final File propsFile = new File(propertyFilePath);
              if ((propsFile != null) && propsFile.exists()) {
                final Properties properties = PropertiesUtils.fromFile(propsFile);
                if ((propertyKey != null) && !"".equals(propertyKey.trim())) {
                  propertyKey = propertyKey.trim();
                }
                if ((properties != null) && properties.containsKey(propertyKey)) {
                  return properties.getProperty(propertyKey);
                }
              } else {
                try {
                  throw new ParameterException(
                      new FileNotFoundException(
                          propsFile != null
                              ? "Properties file not found at path: " + propsFile.getCanonicalPath()
                              : "No properties file specified"));
                } catch (final IOException e) {
                  throw new ParameterException(e);
                }
              }
            } else {
              throw new ParameterException("No properties file path specified");
            }
          } else {
            throw new ParameterException(
                "Property File values are expected in input format <property file path>::<property key>");
          }
        } else {
          throw new ParameterException(new Exception("No properties file specified"));
        }
        return value;
      }
    },
    STDIN(PasswordConverter.STDIN) {
      private String input = null;

      @Override
      public boolean matches(final String value) {
        return prefix.equals(value);
      }

      @Override
      String process(final String value) {
        if (input == null) {
          input = promptAndReadPassword("Enter password: ");
        }
        return input;
      }
    },
    DEFAULT("") {
      @Override
      String process(final String password) {
        return password;
      }
    };

    String prefix;

    private KeyType(final String prefix) {
      this.prefix = prefix;
    }

    public boolean matches(final String value) {
      return value.startsWith(prefix);
    }

    public String convert(final String value) {
      return process(value.substring(prefix.length()));
    }

    String process(final String value) {
      return value;
    }
  }

  @Override
  public String convert(final String value) {
    for (final KeyType keyType : KeyType.values()) {
      if (keyType.matches(value)) {
        return keyType.convert(value);
      }
    }
    return value;
  }

  @Override
  public boolean isPassword() {
    return true;
  }

  @Override
  public boolean isRequired() {
    return true;
  }

  protected Properties getGeoWaveConfigProperties() {
    final File geowaveConfigPropsFile = getGeoWaveConfigFile();
    return ConfigOptions.loadProperties(geowaveConfigPropsFile);
  }

  protected File getGeoWaveConfigFile() {
    return ConfigOptions.getDefaultPropertyFile(getConsole());
  }
}

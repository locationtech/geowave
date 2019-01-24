/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Properties;
import org.locationtech.geowave.core.cli.Constants;
import org.locationtech.geowave.core.cli.utils.JCommanderParameterUtils;
import org.locationtech.geowave.core.cli.utils.PropertiesUtils;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This interface doesn't actually do anything, is just used for tracking during development. */
public abstract class StoreFactoryOptions {
  private static final Logger LOGGER = LoggerFactory.getLogger(StoreFactoryOptions.class);

  public static final String GEOWAVE_NAMESPACE_OPTION = "gwNamespace";
  public static final String GEOWAVE_NAMESPACE_DESCRIPTION =
      "The geowave namespace (optional; default is no namespace)";

  @Parameter(names = "--" + GEOWAVE_NAMESPACE_OPTION, description = GEOWAVE_NAMESPACE_DESCRIPTION)
  protected String geowaveNamespace;

  public StoreFactoryOptions() {}

  public StoreFactoryOptions(String geowaveNamespace) {
    this.geowaveNamespace = geowaveNamespace;
  }

  public String getGeoWaveNamespace() {
    if ("null".equalsIgnoreCase(geowaveNamespace)) {
      return null;
    }
    return geowaveNamespace;
  }

  public void setGeoWaveNamespace(final String geowaveNamespace) {
    this.geowaveNamespace = geowaveNamespace;
  }

  public abstract StoreFactoryFamilySpi getStoreFactory();

  public DataStorePluginOptions createPluginOptions() {
    return new DataStorePluginOptions(this);
  }

  public abstract DataStoreOptions getStoreOptions();

  public void validatePluginOptions() throws ParameterException {
    validatePluginOptions(new Properties());
  }

  /**
   * Method to perform global validation for all plugin options
   *
   * @throws Exception
   */
  public void validatePluginOptions(Properties properties) throws ParameterException {
    LOGGER.trace("ENTER :: validatePluginOptions()");
    PropertiesUtils propsUtils = new PropertiesUtils(properties);
    boolean defaultEchoEnabled =
        propsUtils.getBoolean(Constants.CONSOLE_DEFAULT_ECHO_ENABLED_KEY, false);
    boolean passwordEchoEnabled =
        propsUtils.getBoolean(Constants.CONSOLE_PASSWORD_ECHO_ENABLED_KEY, defaultEchoEnabled);
    LOGGER.debug(
        "Default console echo is {}, Password console echo is {}",
        new Object[] {
            defaultEchoEnabled ? "enabled" : "disabled",
            passwordEchoEnabled ? "enabled" : "disabled"});
    for (Field field : this.getClass().getDeclaredFields()) {
      for (Annotation annotation : field.getAnnotations()) {
        if (annotation.annotationType() == Parameter.class) {
          Parameter parameter = (Parameter) annotation;
          if (JCommanderParameterUtils.isRequired(parameter)) {
            field.setAccessible(true); // HPFortify
            // "Access Specifier Manipulation"
            // False Positive: These
            // fields are being modified
            // by trusted code,
            // in a way that is not
            // influenced by user input
            Object value = null;
            try {
              value = field.get(this);
              if (value == null) {
                JCommander.getConsole().println(
                    "Field ["
                        + field.getName()
                        + "] is required: "
                        + Arrays.toString(parameter.names())
                        + ": "
                        + parameter.description());
                JCommander.getConsole().print("Enter value for [" + field.getName() + "]: ");
                boolean echoEnabled =
                    JCommanderParameterUtils.isPassword(parameter) ? passwordEchoEnabled
                        : defaultEchoEnabled;
                char[] password = JCommander.getConsole().readPassword(echoEnabled);
                String strPassword = new String(password);
                password = null;

                if (!"".equals(strPassword.trim())) {
                  value =
                      (strPassword != null && !"".equals(strPassword.trim())) ? strPassword.trim()
                          : null;
                }
                if (value == null) {
                  throw new ParameterException(
                      "Value for [" + field.getName() + "] cannot be null");
                } else {
                  field.set(this, value);
                }
              }
            } catch (Exception ex) {
              LOGGER.error(
                  "An error occurred validating plugin options for ["
                      + this.getClass().getName()
                      + "]: "
                      + ex.getLocalizedMessage(),
                  ex);
            }
          }
        }
      }
    }
  }
}

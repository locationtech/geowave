/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.operations.config;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.locationtech.geowave.core.cli.Constants;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.cli.converters.PasswordConverter;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.operations.config.security.utils.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "set", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Set GeoWave configuration property directly")
public class SetCommand extends ServiceEnabledCommand<Object> {
  /** Return "200 OK" for the set command. */
  @Override
  public Boolean successStatusIs200() {
    return true;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SetCommand.class);

  @Parameter(description = "<name> <value>")
  private List<String> parameters = new ArrayList<>();

  @Parameter(
      names = {"--password"},
      description = "Specify if the value being set is a password and should be encrypted in the configurations")
  private Boolean password = false;

  private boolean isRestCall = true;

  @Override
  public void execute(final OperationParams params) {
    isRestCall = false;
    computeResults(params);
  }

  /**
   * Add rest endpoint for the set command. Looks for GET params with keys 'key' and 'value' to set.
   *
   * @return string containing json with details of success or failure of the set
   */
  @Override
  public Object computeResults(final OperationParams params) {
    return setKeyValue(params);
  }

  /** Set the key value pair in the config. Store the previous value of the key in prevValue */
  private Object setKeyValue(final OperationParams params) {

    final File f = getGeoWaveConfigFile(params);
    final Properties p = ConfigOptions.loadProperties(f);

    String key = null;
    String value = null;
    final PasswordConverter converter = new PasswordConverter(null);
    if ((parameters.size() == 1) && (parameters.get(0).indexOf("=") != -1)) {
      final String[] parts = StringUtils.split(parameters.get(0), "=");
      key = parts[0];
      if (!isRestCall && password) {
        value = converter.convert(parts[1]);
      } else {
        value = parts[1];
      }
    } else if (parameters.size() == 2) {
      key = parameters.get(0);
      if (!isRestCall && password) {
        value = converter.convert(parameters.get(1));

      } else {
        value = parameters.get(1);
      }
    } else {
      throw new ParameterException("Requires: <name> <value>");
    }

    if (password) {
      // check if encryption is enabled in configuration
      if (Boolean.parseBoolean(
          p.getProperty(Constants.ENCRYPTION_ENABLED_KEY, Constants.ENCRYPTION_ENABLED_DEFAULT))) {
        try {
          final File tokenFile =
              SecurityUtils.getFormattedTokenKeyFileForConfig(getGeoWaveConfigFile());
          value =
              SecurityUtils.encryptAndHexEncodeValue(
                  value,
                  tokenFile.getAbsolutePath(),
                  params.getConsole());
          LOGGER.debug("Value was successfully encrypted");
        } catch (final Exception e) {
          LOGGER.error(
              "An error occurred encrypting the specified value: " + e.getLocalizedMessage(),
              e);
        }
      } else {
        LOGGER.debug(
            "Value was set as a password, though encryption is currently disabled, so value was not encrypted. "
                + "Please enable encryption and re-try.\n"
                + "Note: To enable encryption, run the following command: geowave config set {}=true",
            Constants.ENCRYPTION_ENABLED_KEY);
      }
    }

    final Object previousValue = p.setProperty(key, value);
    if (!ConfigOptions.writeProperties(f, p, params.getConsole())) {
      throw new WritePropertiesException("Write failure");
    } else {
      return previousValue;
    }
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(final String key, final String value) {
    parameters = new ArrayList<>();
    parameters.add(key);
    parameters.add(value);
  }

  private static class WritePropertiesException extends RuntimeException {

    /** */
    private static final long serialVersionUID = 1L;

    private WritePropertiesException(final String string) {
      super(string);
    }
  }
}

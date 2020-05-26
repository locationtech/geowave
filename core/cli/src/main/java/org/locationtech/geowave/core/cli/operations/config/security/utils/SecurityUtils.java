/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
/** */
package org.locationtech.geowave.core.cli.operations.config.security.utils;

import java.io.File;
import org.locationtech.geowave.core.cli.operations.config.security.crypto.BaseEncryption;
import org.locationtech.geowave.core.cli.operations.config.security.crypto.GeoWaveEncryption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.internal.Console;

/** Security utility class for simpler interfacing with */
public class SecurityUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(SecurityUtils.class);

  private static BaseEncryption encService;
  private static final String WRAPPER = BaseEncryption.WRAPPER;

  /**
   * Method to decrypt a value
   *
   * @param value Value to decrypt. Should be wrapped with ENC{}
   * @param resourceLocation Optional value to specify the location of the encryption service
   *        resource location
   * @return decrypted value
   */
  public static String decryptHexEncodedValue(
      final String value,
      final String resourceLocation,
      Console console) throws Exception {
    LOGGER.trace("Decrypting hex-encoded value");
    if ((value != null) && !"".equals(value.trim())) {
      if (BaseEncryption.isProperlyWrapped(value.trim())) {
        try {
          return getEncryptionService(resourceLocation, console).decryptHexEncoded(value);
        } catch (final Throwable t) {
          LOGGER.error(
              "Encountered exception during content decryption: " + t.getLocalizedMessage(),
              t);
        }
      } else {
        LOGGER.debug(
            "WARNING: Value to decrypt was not propertly encoded and wrapped with "
                + WRAPPER
                + ". Not decrypting value.");
        return value;
      }
    } else {
      LOGGER.debug("WARNING: No value specified to decrypt.");
    }
    return "";
  }

  /**
   * Method to encrypt and hex-encode a string value
   *
   * @param value value to encrypt and hex-encode
   * @param resourceLocation resource token to use for encrypting the value
   * @return If encryption is successful, encrypted and hex-encoded string value is returned wrapped
   *         with ENC{}
   */
  public static String encryptAndHexEncodeValue(
      final String value,
      final String resourceLocation,
      Console console) throws Exception {
    LOGGER.debug("Encrypting and hex-encoding value");
    if ((value != null) && !"".equals(value.trim())) {
      if (!BaseEncryption.isProperlyWrapped(value)) {
        try {
          return getEncryptionService(resourceLocation, console).encryptAndHexEncode(value);
        } catch (final Throwable t) {
          LOGGER.error(
              "Encountered exception during content encryption: " + t.getLocalizedMessage(),
              t);
        }
      } else {
        LOGGER.debug(
            "WARNING: Value to encrypt already appears to be encrypted and already wrapped with "
                + WRAPPER
                + ". Not encrypting value.");
        return value;
      }
    } else {
      LOGGER.debug("WARNING: No value specified to encrypt.");
      return value;
    }
    return value;
  }

  /**
   * Returns an instance of the encryption service, initialized with the token at the provided
   * resource location
   *
   * @param resourceLocation location of the resource token to initialize the encryption service
   *        with
   * @return An initialized instance of the encryption service
   * @throws Exception
   */
  private static synchronized BaseEncryption getEncryptionService(
      final String resourceLocation,
      Console console) throws Throwable {
    if (encService == null) {
      if ((resourceLocation != null) && !"".equals(resourceLocation.trim())) {
        LOGGER.trace(
            "Setting resource location for encryption service: [" + resourceLocation + "]");
        encService = new GeoWaveEncryption(resourceLocation, console);
      } else {
        encService = new GeoWaveEncryption(console);
      }
    } else {
      if (!resourceLocation.equals(encService.getResourceLocation())) {
        encService = new GeoWaveEncryption(resourceLocation, console);
      }
    }
    return encService;
  }

  /**
   * Utilty method to format the file path for the token key file associated with a config file
   *
   * @param configFile Location of config file that token key file is associated with
   * @return File for given config file
   */
  public static File getFormattedTokenKeyFileForConfig(final File configFile) {
    return new File(
        // get the resource location
        configFile.getParentFile(),
        // get the formatted token file name with version
        BaseEncryption.getFormattedTokenFileName(configFile.getName()));
  }
}

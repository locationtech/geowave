/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.operations.config.security;

import java.io.File;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.ConfigSection;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.operations.config.security.crypto.BaseEncryption;
import org.locationtech.geowave.core.cli.operations.config.security.utils.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "newcryptokey", parentOperation = ConfigSection.class)
@Parameters(
    commandDescription = "Generate a new security cryptography key for use with configuration properties")
public class NewTokenCommand extends DefaultOperation implements Command {
  private static final Logger sLog = LoggerFactory.getLogger(NewTokenCommand.class);

  @Override
  public void execute(final OperationParams params) {
    sLog.trace("ENTER :: execute");

    final File geowaveDir = getGeoWaveDirectory();
    if ((geowaveDir != null) && geowaveDir.exists()) {
      final File tokenFile = getSecurityTokenFile();
      // if token already exists, iterate through config props file and
      // re-encrypt any encrypted values against the new token
      if ((tokenFile != null) && tokenFile.exists()) {
        try {
          sLog.info(
              "Existing encryption token file exists already at path ["
                  + tokenFile.getCanonicalPath());
          sLog.info(
              "Creating new encryption token and migrating all passwords in [{}] to be encrypted with new token",
              ConfigOptions.getDefaultPropertyFile(params.getConsole()).getCanonicalPath());

          File backupFile = null;
          boolean tokenBackedUp = false;
          try {
            backupFile = new File(tokenFile.getCanonicalPath() + ".bak");
            tokenBackedUp = tokenFile.renameTo(backupFile);
            generateNewEncryptionToken(tokenFile);
          } catch (final Exception ex) {
            sLog.error(
                "An error occurred backing up existing token file. Please check directory and permissions and try again.",
                ex);
          }
          if (tokenBackedUp) {
            final Properties configProps = getGeoWaveConfigProperties(params);
            if (configProps != null) {
              boolean updated = false;
              final Set<Object> keySet = configProps.keySet();
              final Iterator<Object> keyIter = keySet.iterator();
              if (keyIter != null) {
                String configKey = null;
                while (keyIter.hasNext()) {
                  configKey = (String) keyIter.next();
                  final String configValue = configProps.getProperty(configKey);
                  if ((configValue != null)
                      && !"".equals(configValue.trim())
                      && BaseEncryption.isProperlyWrapped(configValue)) {
                    // HP Fortify "NULL Pointer Dereference"
                    // false positive
                    // Exception handling will catch if
                    // backupFile is null
                    final String decryptedValue =
                        SecurityUtils.decryptHexEncodedValue(
                            configValue,
                            backupFile.getCanonicalPath(),
                            params.getConsole());
                    final String encryptedValue =
                        SecurityUtils.encryptAndHexEncodeValue(
                            decryptedValue,
                            tokenFile.getCanonicalPath(),
                            params.getConsole());
                    configProps.put(configKey, encryptedValue);
                    updated = true;
                  }
                }
              }
              if (updated) {
                ConfigOptions.writeProperties(
                    getGeoWaveConfigFile(params),
                    configProps,
                    params.getConsole());
              }
            }
            // HP Fortify "NULL Pointer Dereference" false positive
            // Exception handling will catch if backupFile is null
            backupFile.deleteOnExit();
          }
        } catch (final Exception ex) {
          sLog.error(
              "An error occurred creating a new encryption token: " + ex.getLocalizedMessage(),
              ex);
        }
      } else {
        generateNewEncryptionToken(tokenFile);
      }
    }
    sLog.trace("EXIT :: execute");
  }
}

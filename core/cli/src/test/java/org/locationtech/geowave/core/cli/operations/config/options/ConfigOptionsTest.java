/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.operations.config.options;

import static org.junit.Assert.assertEquals;
import java.io.File;
import java.util.Properties;
import org.junit.Test;
import com.beust.jcommander.JCommander;

public class ConfigOptionsTest {
  @Test
  public void testWriteProperty() {
    final String parent = String.format("%s", System.getProperty("user.home"));
    final File path = new File(parent);
    final File configfile = ConfigOptions.formatConfigFile("0", path);
    final Properties prop = new Properties();
    final String key = "key";
    final String value = "value";
    prop.setProperty(key, value);
    final boolean success =
        ConfigOptions.writeProperties(configfile, prop, new JCommander().getConsole());
    if (success) {
      final Properties loadprop = ConfigOptions.loadProperties(configfile);
      assertEquals(value, loadprop.getProperty(key));
    }
  }
}

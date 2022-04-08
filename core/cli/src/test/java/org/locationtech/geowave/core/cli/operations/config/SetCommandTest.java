/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.operations.config;

import static org.junit.Assert.assertEquals;
import java.io.File;
import java.util.Properties;
import org.junit.Test;
import org.locationtech.geowave.core.cli.operations.GeoWaveTopLevelSection;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.CommandLineOperationParams;
import org.locationtech.geowave.core.cli.parser.OperationParser;
import org.locationtech.geowave.core.cli.spi.OperationRegistry;

public class SetCommandTest {

  @Test
  public void testExecute() {
    final String[] args = {"config", "set", "name", "value"};
    final OperationRegistry registry = OperationRegistry.getInstance();
    final OperationParser parser = new OperationParser(registry);
    final CommandLineOperationParams params = parser.parse(GeoWaveTopLevelSection.class, args);

    final SetCommand setcommand = new SetCommand();
    final String name = "name";
    final String value = "value";
    setcommand.setParameters(name, value);
    setcommand.prepare(params);
    setcommand.execute(params);

    final File f = (File) params.getContext().get(ConfigOptions.PROPERTIES_FILE_CONTEXT);
    final Properties p = ConfigOptions.loadProperties(f);
    assertEquals(value, p.getProperty(name));
  }
}

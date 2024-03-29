/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.operations;

import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.locationtech.geowave.core.cli.parser.CommandLineOperationParams;
import org.locationtech.geowave.core.cli.parser.OperationParser;
import org.locationtech.geowave.core.cli.spi.OperationRegistry;

public class ExplainCommandTest {

  @Test
  public void testPrepare() {
    final String[] args = {"explain"};
    final OperationRegistry registry = OperationRegistry.getInstance();
    final OperationParser parser = new OperationParser(registry);
    final CommandLineOperationParams params = parser.parse(GeoWaveTopLevelSection.class, args);

    final ExplainCommand expcommand = new ExplainCommand();
    expcommand.prepare(params);
    assertEquals(false, params.isValidate());
    assertEquals(true, params.isAllowUnknown());
  }
}

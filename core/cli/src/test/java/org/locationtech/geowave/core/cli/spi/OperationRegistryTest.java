/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.spi;

import static org.junit.Assert.assertEquals;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.locationtech.geowave.core.cli.operations.ExplainCommand;

public class OperationRegistryTest {

  @Test
  public void testGetOperation() {
    final OperationEntry optentry = new OperationEntry(ExplainCommand.class);
    final List<OperationEntry> entries = new ArrayList<>();
    entries.add(optentry);
    final OperationRegistry optreg = new OperationRegistry(entries);

    assertEquals("explain", optreg.getOperation(ExplainCommand.class).getOperationNames()[0]);
    assertEquals(true, optreg.getAllOperations().contains(optentry));
  }
}

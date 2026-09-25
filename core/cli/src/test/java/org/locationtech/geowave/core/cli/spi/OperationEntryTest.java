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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.operations.ExplainCommand;

public class OperationEntryTest {

  @GeowaveOperation(name = "failing")
  public static class FailingConstructorOperation extends DefaultOperation {
    public FailingConstructorOperation() {
      throw new IllegalStateException("constructor failed");
    }
  }

  @GeowaveOperation(name = "no-default-constructor")
  public static class NoDefaultConstructorOperation extends DefaultOperation {
    public NoDefaultConstructorOperation(final String unused) {}
  }

  @Test
  public void testCreateInstance() {
    assertTrue(new OperationEntry(ExplainCommand.class).createInstance() instanceof ExplainCommand);
  }

  @Test
  public void testConstructorExceptionIsNotWrapped() {
    final OperationEntry entry = new OperationEntry(FailingConstructorOperation.class);
    final IllegalStateException e =
        assertThrows(IllegalStateException.class, entry::createInstance);
    assertEquals("constructor failed", e.getMessage());
  }

  @Test
  public void testMissingNoArgConstructorReturnsNull() {
    assertNull(new OperationEntry(NoDefaultConstructorOperation.class).createInstance());
  }
}

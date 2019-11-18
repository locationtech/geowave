/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query.gwql;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.locationtech.geowave.adapter.vector.query.gwql.parse.GWQLParser;

public abstract class AbstractStatementTest {
  protected void assertInvalidStatement(String statement, String expectedMessage) {
    try {
      GWQLParser.parseStatement(statement);
      fail();
    } catch (GWQLParseException e) {
      // expected
      assertTrue(
          e.getMessage() + " does not contain " + expectedMessage,
          e.getMessage().contains(expectedMessage));
    }
  }
}

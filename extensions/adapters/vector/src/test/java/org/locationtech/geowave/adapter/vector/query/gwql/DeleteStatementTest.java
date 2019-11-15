/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query.gwql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.text.ParseException;
import org.junit.Test;
import org.locationtech.geowave.adapter.vector.query.gwql.parse.GWQLParser;
import org.locationtech.geowave.adapter.vector.query.gwql.statement.DeleteStatement;
import org.locationtech.geowave.adapter.vector.query.gwql.statement.Statement;
import org.opengis.filter.And;
import org.opengis.filter.Filter;
import org.opengis.filter.spatial.BBOX;
import org.opengis.filter.temporal.During;

public class DeleteStatementTest extends AbstractStatementTest {

  @Test
  public void testInvalidStatements() {
    // Missing from
    assertInvalidStatement("DELETE", "expecting FROM");
    // Missing store and type name
    assertInvalidStatement("DELETE FROM", "expecting IDENTIFIER");
    // Missing from
    assertInvalidStatement("DELETE store.type", "missing FROM");
    // Missing type
    assertInvalidStatement("DELETE FROM store", "expecting '.'");
    // Missing filter
    assertInvalidStatement("DELETE FROM store.type WHERE", "expecting CQL");
  }

  @Test
  public void testValidStatements() {
    GWQLParser.parseStatement("DELETE FROM store.type");
    GWQLParser.parseStatement("DELETE FROM store.type WHERE CQL(a < 1)");
  }


  @Test
  public void testDelete() throws ParseException, IOException {
    final String statement = "DELETE FROM store.type";
    final Statement gwStatement = GWQLParser.parseStatement(statement);
    assertTrue(gwStatement instanceof DeleteStatement);
    final DeleteStatement deleteStatement = (DeleteStatement) gwStatement;
    assertNotNull(deleteStatement.typeName());
    assertEquals("store", deleteStatement.typeName().storeName());
    assertEquals("type", deleteStatement.typeName().typeName());
    assertNull(deleteStatement.filter());
  }

  @Test
  public void testDeleteWithFilter() throws ParseException, IOException {
    final String statement =
        "DELETE FROM store.type WHERE CQL(BBOX(geometry,27.20,41.30,27.30,41.20) and start during 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z)";
    final Statement gwStatement = GWQLParser.parseStatement(statement);
    assertTrue(gwStatement instanceof DeleteStatement);
    final DeleteStatement deleteStatement = (DeleteStatement) gwStatement;
    assertNotNull(deleteStatement.typeName());
    assertEquals("store", deleteStatement.typeName().storeName());
    assertEquals("type", deleteStatement.typeName().typeName());
    assertNotNull(deleteStatement.filter());
    final Filter filter = deleteStatement.filter();
    assertTrue(filter instanceof And);
    final And andFilter = (And) filter;
    assertTrue(andFilter.getChildren().size() == 2);
    assertTrue(andFilter.getChildren().get(0) instanceof BBOX);
    assertTrue(andFilter.getChildren().get(1) instanceof During);
  }
}

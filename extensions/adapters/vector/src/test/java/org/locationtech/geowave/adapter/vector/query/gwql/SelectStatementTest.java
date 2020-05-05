/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query.gwql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.text.ParseException;
import org.junit.Test;
import org.locationtech.geowave.adapter.vector.query.gwql.AggregationSelector;
import org.locationtech.geowave.adapter.vector.query.gwql.ColumnSelector;
import org.locationtech.geowave.adapter.vector.query.gwql.parse.GWQLParser;
import org.locationtech.geowave.adapter.vector.query.gwql.statement.SelectStatement;
import org.locationtech.geowave.adapter.vector.query.gwql.statement.Statement;
import org.opengis.filter.And;
import org.opengis.filter.Filter;
import org.opengis.filter.spatial.BBOX;
import org.opengis.filter.temporal.During;

public class SelectStatementTest extends AbstractStatementTest {

  @Test
  public void testInvalidStatements() {
    // Missing from
    assertInvalidStatement("SELECT *", "expecting FROM");
    // Missing store and type name
    assertInvalidStatement("SELECT * FROM", "expecting IDENTIFIER");
    // Missing everything
    assertInvalidStatement("SELECT", "expecting {'*', IDENTIFIER}");
    // All columns and single selector
    assertInvalidStatement("SELECT *, column FROM store.type", "expecting FROM");
    // All columns and aggregation selector
    assertInvalidStatement("SELECT *, agg(column) FROM store.type", "expecting FROM");
    // No type name
    assertInvalidStatement("SELECT * FROM store", "expecting '.'");
    // No selectors
    assertInvalidStatement("SELECT FROM store.type", "expecting {'*', IDENTIFIER}");
    // Aggregation and non aggregation selectors
    assertInvalidStatement("SELECT agg(*), column FROM store.type", "expecting '('");
    // Non-CQL where filter
    assertInvalidStatement("SELECT * FROM store.type WHERE a < 1", "expecting CQL");
    // No where filter
    assertInvalidStatement("SELECT * FROM store.type WHERE", "expecting CQL");
    // No limit count
    assertInvalidStatement("SELECT * FROM store.type LIMIT", "missing INTEGER");
    // Non-integer limit count
    assertInvalidStatement("SELECT * FROM store.type LIMIT 1.5", "expecting INTEGER");
    // Missing column alias
    assertInvalidStatement("SELECT a AS FROM store.type", "expecting IDENTIFIER");
  }

  @Test
  public void testValidStatements() {
    GWQLParser.parseStatement("SELECT * FROM store.type");
    GWQLParser.parseStatement("SELECT * FROM store.type LIMIT 1");
    GWQLParser.parseStatement("SELECT * FROM store.type WHERE CQL(a < 1)");
    GWQLParser.parseStatement("SELECT * FROM store.type WHERE CQL(a > 1) LIMIT 1");
    GWQLParser.parseStatement("SELECT a, b FROM store.type");
    GWQLParser.parseStatement("SELECT a, b FROM store.type LIMIT 1");
    GWQLParser.parseStatement("SELECT a, b FROM store.type WHERE CQL(a < 1)");
    GWQLParser.parseStatement("SELECT a, b FROM store.type WHERE CQL(a > 1) LIMIT 2");
    GWQLParser.parseStatement("SELECT a AS a_alt, b FROM store.type");
    GWQLParser.parseStatement("SELECT a AS a_alt, b FROM store.type LIMIT 1");
    GWQLParser.parseStatement("SELECT a AS a_alt, b FROM store.type WHERE CQL(a < 1)");
    GWQLParser.parseStatement("SELECT a AS a_alt, b FROM store.type WHERE CQL(a > 1) LIMIT 2");
    GWQLParser.parseStatement("SELECT SUM(a) FROM store.type");
    GWQLParser.parseStatement("SELECT SUM(a) FROM store.type LIMIT 1");
    GWQLParser.parseStatement("SELECT SUM(a) FROM store.type WHERE CQL(a < 1)");
    GWQLParser.parseStatement("SELECT SUM(a) FROM store.type WHERE CQL(a > 1) LIMIT 3");
    GWQLParser.parseStatement("SELECT SUM(a) AS sum FROM store.type");
    GWQLParser.parseStatement("SELECT SUM(a) AS sum FROM store.type LIMIT 1");
    GWQLParser.parseStatement("SELECT SUM(a) AS sum FROM store.type WHERE CQL(a < 1)");
    GWQLParser.parseStatement("SELECT SUM(a) AS sum FROM store.type WHERE CQL(a > 1) LIMIT 3");
    GWQLParser.parseStatement("SELECT COUNT(*) FROM store.type");
    GWQLParser.parseStatement("SELECT COUNT(*) FROM store.type LIMIT 1");
    GWQLParser.parseStatement("SELECT COUNT(*) FROM store.type WHERE CQL(a < 1)");
    GWQLParser.parseStatement("SELECT COUNT(*) FROM store.type WHERE CQL(a > 1) LIMIT 4");
    GWQLParser.parseStatement("SELECT SUM(a), COUNT(*) FROM store.type");
    GWQLParser.parseStatement("SELECT SUM(a), COUNT(*) FROM store.type LIMIT 1");
    GWQLParser.parseStatement("SELECT SUM(a), COUNT(*) FROM store.type WHERE CQL(a < 1)");
    GWQLParser.parseStatement("SELECT SUM(a), COUNT(*) FROM store.type WHERE CQL(a > 1) LIMIT 4");
  }


  @Test
  public void testAllColumns() throws ParseException, IOException {
    final String statement = "SELECT * FROM store.type";
    final Statement gwStatement = GWQLParser.parseStatement(statement);
    assertTrue(gwStatement instanceof SelectStatement);
    final SelectStatement selectStatement = (SelectStatement) gwStatement;
    assertFalse(selectStatement.isAggregation());
    assertNotNull(selectStatement.typeName());
    assertEquals("store", selectStatement.typeName().storeName());
    assertEquals("type", selectStatement.typeName().typeName());
    assertNull(selectStatement.filter());
  }

  @Test
  public void testAllColumnsWithFilter() throws ParseException, IOException {
    final String statement =
        "SELECT * FROM store.type WHERE CQL(BBOX(geometry,27.20,41.30,27.30,41.20) and start during 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z)";
    final Statement gwStatement = GWQLParser.parseStatement(statement);
    assertTrue(gwStatement instanceof SelectStatement);
    final SelectStatement selectStatement = (SelectStatement) gwStatement;
    assertFalse(selectStatement.isAggregation());
    assertNotNull(selectStatement.typeName());
    assertEquals("store", selectStatement.typeName().storeName());
    assertEquals("type", selectStatement.typeName().typeName());
    assertNotNull(selectStatement.filter());
    Filter filter = selectStatement.filter();
    assertTrue(filter instanceof And);
    And andFilter = (And) filter;
    assertTrue(andFilter.getChildren().size() == 2);
    assertTrue(andFilter.getChildren().get(0) instanceof BBOX);
    assertTrue(andFilter.getChildren().get(1) instanceof During);
    assertNull(selectStatement.limit());
  }

  @Test
  public void testAllColumnsWithFilterAndLimit() throws ParseException, IOException {
    final String statement =
        "SELECT * FROM store.type WHERE CQL(BBOX(geometry,27.20,41.30,27.30,41.20) and start during 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z) LIMIT 1";
    final Statement gwStatement = GWQLParser.parseStatement(statement);
    assertTrue(gwStatement instanceof SelectStatement);
    final SelectStatement selectStatement = (SelectStatement) gwStatement;
    assertFalse(selectStatement.isAggregation());
    assertNotNull(selectStatement.typeName());
    assertEquals("store", selectStatement.typeName().storeName());
    assertEquals("type", selectStatement.typeName().typeName());
    assertNotNull(selectStatement.filter());
    Filter filter = selectStatement.filter();
    assertTrue(filter instanceof And);
    And andFilter = (And) filter;
    assertTrue(andFilter.getChildren().size() == 2);
    assertTrue(andFilter.getChildren().get(0) instanceof BBOX);
    assertTrue(andFilter.getChildren().get(1) instanceof During);
    assertNotNull(selectStatement.limit());
    assertEquals(1, selectStatement.limit().intValue());
  }

  @Test
  public void testAggregation() {
    final String statement = "SELECT sum(a) FROM store.type";
    final Statement gwStatement = GWQLParser.parseStatement(statement);
    assertTrue(gwStatement instanceof SelectStatement);
    final SelectStatement selectStatement = (SelectStatement) gwStatement;
    assertTrue(selectStatement.isAggregation());
    assertNotNull(selectStatement.typeName());
    assertEquals("store", selectStatement.typeName().storeName());
    assertEquals("type", selectStatement.typeName().typeName());
    assertNotNull(selectStatement.selectors());
    assertTrue(selectStatement.selectors().size() == 1);
    assertTrue(selectStatement.selectors().get(0) instanceof AggregationSelector);
    AggregationSelector selector = (AggregationSelector) selectStatement.selectors().get(0);
    assertNull(selector.alias());
    assertEquals("sum", selector.functionName());
    assertEquals(1, selector.functionArgs().length);
    assertEquals("a", selector.functionArgs()[0]);
    assertNull(selectStatement.filter());
  }

  @Test
  public void testAggregationAlias() {
    final String statement = "SELECT sum(a) AS total FROM store.type";
    final Statement gwStatement = GWQLParser.parseStatement(statement);
    assertTrue(gwStatement instanceof SelectStatement);
    final SelectStatement selectStatement = (SelectStatement) gwStatement;
    assertTrue(selectStatement.isAggregation());
    assertNotNull(selectStatement.typeName());
    assertEquals("store", selectStatement.typeName().storeName());
    assertEquals("type", selectStatement.typeName().typeName());
    assertNotNull(selectStatement.selectors());
    assertTrue(selectStatement.selectors().size() == 1);
    assertTrue(selectStatement.selectors().get(0) instanceof AggregationSelector);
    AggregationSelector selector = (AggregationSelector) selectStatement.selectors().get(0);
    assertEquals("total", selector.alias());
    assertEquals("sum", selector.functionName());
    assertEquals(1, selector.functionArgs().length);
    assertEquals("a", selector.functionArgs()[0]);
    assertNull(selectStatement.filter());
  }

  @Test
  public void testColumnSubset() {
    final String statement = "SELECT a, b, c FROM store.type";
    final Statement gwStatement = GWQLParser.parseStatement(statement);
    assertTrue(gwStatement instanceof SelectStatement);
    final SelectStatement selectStatement = (SelectStatement) gwStatement;
    assertFalse(selectStatement.isAggregation());
    assertNotNull(selectStatement.typeName());
    assertEquals("store", selectStatement.typeName().storeName());
    assertEquals("type", selectStatement.typeName().typeName());
    assertNotNull(selectStatement.selectors());
    assertTrue(selectStatement.selectors().size() == 3);
    assertTrue(selectStatement.selectors().get(0) instanceof ColumnSelector);
    ColumnSelector selector = (ColumnSelector) selectStatement.selectors().get(0);
    assertNull(selector.alias());
    assertEquals("a", selector.columnName());
    assertTrue(selectStatement.selectors().get(1) instanceof ColumnSelector);
    selector = (ColumnSelector) selectStatement.selectors().get(1);
    assertNull(selector.alias());
    assertEquals("b", selector.columnName());
    assertTrue(selectStatement.selectors().get(2) instanceof ColumnSelector);
    selector = (ColumnSelector) selectStatement.selectors().get(2);
    assertNull(selector.alias());
    assertEquals("c", selector.columnName());
    assertNull(selectStatement.filter());
  }

  @Test
  public void testColumnSubsetWithAliases() {
    final String statement = "SELECT a AS a_alt, b, c AS c_alt FROM store.type";
    final Statement gwStatement = GWQLParser.parseStatement(statement);
    assertTrue(gwStatement instanceof SelectStatement);
    final SelectStatement selectStatement = (SelectStatement) gwStatement;
    assertFalse(selectStatement.isAggregation());
    assertNotNull(selectStatement.typeName());
    assertEquals("store", selectStatement.typeName().storeName());
    assertEquals("type", selectStatement.typeName().typeName());
    assertNotNull(selectStatement.selectors());
    assertTrue(selectStatement.selectors().size() == 3);
    assertTrue(selectStatement.selectors().get(0) instanceof ColumnSelector);
    ColumnSelector selector = (ColumnSelector) selectStatement.selectors().get(0);
    assertEquals("a_alt", selector.alias());
    assertEquals("a", selector.columnName());
    assertTrue(selectStatement.selectors().get(1) instanceof ColumnSelector);
    selector = (ColumnSelector) selectStatement.selectors().get(1);
    assertNull(selector.alias());
    assertEquals("b", selector.columnName());
    assertTrue(selectStatement.selectors().get(2) instanceof ColumnSelector);
    selector = (ColumnSelector) selectStatement.selectors().get(2);
    assertEquals("c_alt", selector.alias());
    assertEquals("c", selector.columnName());
    assertNull(selectStatement.filter());
  }

  @Test
  public void testUnconventionalNaming() {
    final String statement = "SELECT [a-1], `b-2`, \"c-3\" FROM store.[ty-p3]";
    final Statement gwStatement = GWQLParser.parseStatement(statement);
    assertTrue(gwStatement instanceof SelectStatement);
    final SelectStatement selectStatement = (SelectStatement) gwStatement;
    assertFalse(selectStatement.isAggregation());
    assertNotNull(selectStatement.typeName());
    assertEquals("store", selectStatement.typeName().storeName());
    assertEquals("ty-p3", selectStatement.typeName().typeName());
    assertNotNull(selectStatement.selectors());
    assertTrue(selectStatement.selectors().size() == 3);
    assertTrue(selectStatement.selectors().get(0) instanceof ColumnSelector);
    ColumnSelector selector = (ColumnSelector) selectStatement.selectors().get(0);
    assertNull(selector.alias());
    assertEquals("a-1", selector.columnName());
    assertTrue(selectStatement.selectors().get(1) instanceof ColumnSelector);
    selector = (ColumnSelector) selectStatement.selectors().get(1);
    assertNull(selector.alias());
    assertEquals("b-2", selector.columnName());
    assertTrue(selectStatement.selectors().get(2) instanceof ColumnSelector);
    selector = (ColumnSelector) selectStatement.selectors().get(2);
    assertNull(selector.alias());
    assertEquals("c-3", selector.columnName());
    assertNull(selectStatement.filter());
  }
}

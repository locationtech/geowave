/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.gwql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.text.ParseException;
import org.junit.Test;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.adapter.annotation.GeoWaveDataType;
import org.locationtech.geowave.core.store.adapter.annotation.GeoWaveField;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.query.filter.expression.And;
import org.locationtech.geowave.core.store.query.filter.expression.Filter;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericBetween;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextComparisonOperator;
import org.locationtech.geowave.core.store.query.gwql.parse.GWQLParser;
import org.locationtech.geowave.core.store.query.gwql.statement.SelectStatement;
import org.locationtech.geowave.core.store.query.gwql.statement.Statement;

public class SelectStatementTest extends AbstractGWQLTest {
  @Test
  public void testInvalidStatements() {
    final DataStore dataStore = createDataStore();
    // Missing from
    assertInvalidStatement(dataStore, "SELECT *", "expecting FROM");
    // Missing store and type name
    assertInvalidStatement(dataStore, "SELECT * FROM", "missing IDENTIFIER");
    // Missing everything
    assertInvalidStatement(dataStore, "SELECT", "expecting {'*', IDENTIFIER}");
    // All columns and single selector
    assertInvalidStatement(dataStore, "SELECT *, pop FROM type", "expecting FROM");
    // All columns and aggregation selector
    assertInvalidStatement(dataStore, "SELECT *, agg(column) FROM type", "expecting FROM");
    // Nonexistent type
    assertInvalidStatement(dataStore, "SELECT * FROM nonexistent", "No type named nonexistent");
    // No selectors
    assertInvalidStatement(dataStore, "SELECT FROM type", "expecting {'*', IDENTIFIER}");
    // Aggregation and non aggregation selectors
    assertInvalidStatement(dataStore, "SELECT agg(*), pop FROM type", "expecting '('");
    // No where filter
    assertInvalidStatement(dataStore, "SELECT * FROM type WHERE", "mismatched input '<EOF>'");
    // No limit count
    assertInvalidStatement(dataStore, "SELECT * FROM type LIMIT", "missing INTEGER");
    // Non-integer limit count
    assertInvalidStatement(dataStore, "SELECT * FROM type LIMIT 1.5", "expecting INTEGER");
    // Missing column alias
    assertInvalidStatement(dataStore, "SELECT pop AS FROM type", "expecting IDENTIFIER");
  }

  @Test
  public void testValidStatements() {
    final DataStore dataStore = createDataStore();
    GWQLParser.parseStatement(dataStore, "SELECT * FROM type");
    GWQLParser.parseStatement(dataStore, "SELECT * FROM type LIMIT 1");
    GWQLParser.parseStatement(dataStore, "SELECT * FROM type WHERE pop < 1");
    GWQLParser.parseStatement(dataStore, "SELECT * FROM type WHERE pop > 1 LIMIT 1");
    GWQLParser.parseStatement(dataStore, "SELECT a, b FROM type");
    GWQLParser.parseStatement(dataStore, "SELECT a, b FROM type LIMIT 1");
    GWQLParser.parseStatement(dataStore, "SELECT a, b FROM type WHERE pop < 1");
    GWQLParser.parseStatement(dataStore, "SELECT a, b FROM type WHERE pop > 1 LIMIT 2");
    GWQLParser.parseStatement(dataStore, "SELECT a AS a_alt, b FROM type");
    GWQLParser.parseStatement(dataStore, "SELECT a AS a_alt, b FROM type LIMIT 1");
    GWQLParser.parseStatement(dataStore, "SELECT a AS a_alt, b FROM type WHERE pop < 1");
    GWQLParser.parseStatement(dataStore, "SELECT a AS a_alt, b FROM type WHERE pop > 1 LIMIT 2");
    GWQLParser.parseStatement(dataStore, "SELECT SUM(a) FROM type");
    GWQLParser.parseStatement(dataStore, "SELECT SUM(a) FROM type LIMIT 1");
    GWQLParser.parseStatement(dataStore, "SELECT SUM(a) FROM type WHERE pop < 1");
    GWQLParser.parseStatement(dataStore, "SELECT SUM(a) FROM type WHERE pop > 1 LIMIT 3");
    GWQLParser.parseStatement(dataStore, "SELECT SUM(a) AS sum FROM type");
    GWQLParser.parseStatement(dataStore, "SELECT SUM(a) AS sum FROM type LIMIT 1");
    GWQLParser.parseStatement(dataStore, "SELECT SUM(a) AS sum FROM type WHERE pop < 1");
    GWQLParser.parseStatement(dataStore, "SELECT SUM(a) AS sum FROM type WHERE pop > 1 LIMIT 3");
    GWQLParser.parseStatement(dataStore, "SELECT COUNT(*) FROM type");
    GWQLParser.parseStatement(dataStore, "SELECT COUNT(*) FROM type LIMIT 1");
    GWQLParser.parseStatement(dataStore, "SELECT COUNT(*) FROM type WHERE pop < 1");
    GWQLParser.parseStatement(dataStore, "SELECT COUNT(*) FROM type WHERE pop > 1 LIMIT 4");
    GWQLParser.parseStatement(dataStore, "SELECT SUM(a), COUNT(*) FROM type");
    GWQLParser.parseStatement(dataStore, "SELECT SUM(a), COUNT(*) FROM type LIMIT 1");
    GWQLParser.parseStatement(dataStore, "SELECT SUM(a), COUNT(*) FROM type WHERE pop < 1");
    GWQLParser.parseStatement(dataStore, "SELECT SUM(a), COUNT(*) FROM type WHERE pop > 1 LIMIT 4");
  }


  @Test
  public void testAllColumns() throws ParseException, IOException {
    final DataStore dataStore = createDataStore();
    final String statement = "SELECT * FROM type";
    final Statement gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    final SelectStatement<?> selectStatement = (SelectStatement<?>) gwStatement;
    assertFalse(selectStatement.isAggregation());
    assertNotNull(selectStatement.getAdapter());
    assertEquals("type", selectStatement.getAdapter().getTypeName());
    assertNull(selectStatement.getFilter());
  }

  @Test
  public void testAllColumnsWithFilter() throws ParseException, IOException {
    final DataStore dataStore = createDataStore();
    final String statement = "SELECT * FROM type WHERE pop BETWEEN 1000 AND 2000 and pid > 'abc'";
    final Statement gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    final SelectStatement<?> selectStatement = (SelectStatement<?>) gwStatement;
    assertFalse(selectStatement.isAggregation());
    assertNotNull(selectStatement.getAdapter());
    assertEquals("type", selectStatement.getAdapter().getTypeName());
    assertNotNull(selectStatement.getFilter());
    Filter filter = selectStatement.getFilter();
    assertTrue(filter instanceof And);
    And andFilter = (And) filter;
    assertTrue(andFilter.getChildren().length == 2);
    assertTrue(andFilter.getChildren()[0] instanceof NumericBetween);
    assertTrue(andFilter.getChildren()[1] instanceof TextComparisonOperator);
    assertNull(selectStatement.getLimit());
  }

  @Test
  public void testAllColumnsWithFilterAndLimit() throws ParseException, IOException {
    final DataStore dataStore = createDataStore();
    final String statement =
        "SELECT * FROM type WHERE pop BETWEEN 1000 AND 2000 and pid > 'abc' LIMIT 1";
    final Statement gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    final SelectStatement<?> selectStatement = (SelectStatement<?>) gwStatement;
    assertFalse(selectStatement.isAggregation());
    assertNotNull(selectStatement.getAdapter());
    assertEquals("type", selectStatement.getAdapter().getTypeName());
    assertNotNull(selectStatement.getFilter());
    Filter filter = selectStatement.getFilter();
    assertTrue(filter instanceof And);
    And andFilter = (And) filter;
    assertTrue(andFilter.getChildren().length == 2);
    assertTrue(andFilter.getChildren()[0] instanceof NumericBetween);
    assertTrue(andFilter.getChildren()[1] instanceof TextComparisonOperator);
    assertNotNull(selectStatement.getLimit());
    assertEquals(1, selectStatement.getLimit().intValue());
  }

  @Test
  public void testAggregation() {
    final DataStore dataStore = createDataStore();
    final String statement = "SELECT sum(pop) FROM type";
    final Statement gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    final SelectStatement<?> selectStatement = (SelectStatement<?>) gwStatement;
    assertTrue(selectStatement.isAggregation());
    assertNotNull(selectStatement.getAdapter());
    assertEquals("type", selectStatement.getAdapter().getTypeName());
    assertNotNull(selectStatement.getSelectors());
    assertTrue(selectStatement.getSelectors().size() == 1);
    assertTrue(selectStatement.getSelectors().get(0) instanceof AggregationSelector);
    AggregationSelector selector = (AggregationSelector) selectStatement.getSelectors().get(0);
    assertNull(selector.alias());
    assertEquals("sum", selector.functionName());
    assertEquals(1, selector.functionArgs().length);
    assertEquals("pop", selector.functionArgs()[0]);
    assertNull(selectStatement.getFilter());
  }

  @Test
  public void testAggregationAlias() {
    final DataStore dataStore = createDataStore();
    final String statement = "SELECT sum(pop) AS total FROM type";
    final Statement gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    final SelectStatement<?> selectStatement = (SelectStatement<?>) gwStatement;
    assertTrue(selectStatement.isAggregation());
    assertNotNull(selectStatement.getAdapter());
    assertEquals("type", selectStatement.getAdapter().getTypeName());
    assertNotNull(selectStatement.getSelectors());
    assertTrue(selectStatement.getSelectors().size() == 1);
    assertTrue(selectStatement.getSelectors().get(0) instanceof AggregationSelector);
    AggregationSelector selector = (AggregationSelector) selectStatement.getSelectors().get(0);
    assertEquals("total", selector.alias());
    assertEquals("sum", selector.functionName());
    assertEquals(1, selector.functionArgs().length);
    assertEquals("pop", selector.functionArgs()[0]);
    assertNull(selectStatement.getFilter());
  }

  @Test
  public void testColumnSubset() {
    final DataStore dataStore = createDataStore();
    final String statement = "SELECT pop, start, end FROM type";
    final Statement gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    final SelectStatement<?> selectStatement = (SelectStatement<?>) gwStatement;
    assertFalse(selectStatement.isAggregation());
    assertNotNull(selectStatement.getAdapter());
    assertEquals("type", selectStatement.getAdapter().getTypeName());
    assertNotNull(selectStatement.getSelectors());
    assertTrue(selectStatement.getSelectors().size() == 3);
    assertTrue(selectStatement.getSelectors().get(0) instanceof ColumnSelector);
    ColumnSelector selector = (ColumnSelector) selectStatement.getSelectors().get(0);
    assertNull(selector.alias());
    assertEquals("pop", selector.columnName());
    assertTrue(selectStatement.getSelectors().get(1) instanceof ColumnSelector);
    selector = (ColumnSelector) selectStatement.getSelectors().get(1);
    assertNull(selector.alias());
    assertEquals("start", selector.columnName());
    assertTrue(selectStatement.getSelectors().get(2) instanceof ColumnSelector);
    selector = (ColumnSelector) selectStatement.getSelectors().get(2);
    assertNull(selector.alias());
    assertEquals("end", selector.columnName());
    assertNull(selectStatement.getFilter());
  }

  @Test
  public void testColumnSubsetWithAliases() {
    final DataStore dataStore = createDataStore();
    final String statement = "SELECT pop AS pop_alt, start, end AS end_alt FROM type";
    final Statement gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    final SelectStatement<?> selectStatement = (SelectStatement<?>) gwStatement;
    assertFalse(selectStatement.isAggregation());
    assertNotNull(selectStatement.getAdapter());
    assertEquals("type", selectStatement.getAdapter().getTypeName());
    assertNotNull(selectStatement.getSelectors());
    assertTrue(selectStatement.getSelectors().size() == 3);
    assertTrue(selectStatement.getSelectors().get(0) instanceof ColumnSelector);
    ColumnSelector selector = (ColumnSelector) selectStatement.getSelectors().get(0);
    assertEquals("pop_alt", selector.alias());
    assertEquals("pop", selector.columnName());
    assertTrue(selectStatement.getSelectors().get(1) instanceof ColumnSelector);
    selector = (ColumnSelector) selectStatement.getSelectors().get(1);
    assertNull(selector.alias());
    assertEquals("start", selector.columnName());
    assertTrue(selectStatement.getSelectors().get(2) instanceof ColumnSelector);
    selector = (ColumnSelector) selectStatement.getSelectors().get(2);
    assertEquals("end_alt", selector.alias());
    assertEquals("end", selector.columnName());
    assertNull(selectStatement.getFilter());
  }

  @Test
  public void testUnconventionalNaming() {
    final DataStore dataStore =
        createDataStore(
            BasicDataTypeAdapter.newAdapter("ty-p3", UnconventionalNameType.class, "pid"),
            "a-1");
    final String statement = "SELECT [a-1], `b-2`, \"c-3\" FROM [ty-p3]";
    final Statement gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    final SelectStatement<?> selectStatement = (SelectStatement<?>) gwStatement;
    assertFalse(selectStatement.isAggregation());
    assertNotNull(selectStatement.getAdapter());
    assertEquals("ty-p3", selectStatement.getAdapter().getTypeName());
    assertNotNull(selectStatement.getSelectors());
    assertTrue(selectStatement.getSelectors().size() == 3);
    assertTrue(selectStatement.getSelectors().get(0) instanceof ColumnSelector);
    ColumnSelector selector = (ColumnSelector) selectStatement.getSelectors().get(0);
    assertNull(selector.alias());
    assertEquals("a-1", selector.columnName());
    assertTrue(selectStatement.getSelectors().get(1) instanceof ColumnSelector);
    selector = (ColumnSelector) selectStatement.getSelectors().get(1);
    assertNull(selector.alias());
    assertEquals("b-2", selector.columnName());
    assertTrue(selectStatement.getSelectors().get(2) instanceof ColumnSelector);
    selector = (ColumnSelector) selectStatement.getSelectors().get(2);
    assertNull(selector.alias());
    assertEquals("c-3", selector.columnName());
    assertNull(selectStatement.getFilter());
  }

  @GeoWaveDataType
  protected static class UnconventionalNameType {
    @GeoWaveField(name = "pid")
    private String pid;

    @GeoWaveField(name = "a-1")
    private Long a1;

    @GeoWaveField(name = "b-2")
    private Long b2;

    @GeoWaveField(name = "c-3")
    private Long c3;

    public UnconventionalNameType() {}

    public UnconventionalNameType(final String pid, final Long a1, final Long b2, final Long c3) {
      this.pid = pid;
      this.a1 = a1;
      this.b2 = b2;
      this.c3 = c3;
    }
  }
}

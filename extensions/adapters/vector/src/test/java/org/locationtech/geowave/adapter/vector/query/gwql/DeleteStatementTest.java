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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.text.ParseException;
import org.junit.Test;
import org.locationtech.geowave.adapter.vector.query.gwql.parse.GWQLParser;
import org.locationtech.geowave.adapter.vector.query.gwql.statement.DeleteStatement;
import org.locationtech.geowave.adapter.vector.query.gwql.statement.Statement;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.spatial.BBox;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.During;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalExpression;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalFieldValue;
import org.locationtech.geowave.core.geotime.store.query.filter.expression.temporal.TemporalLiteral;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.query.filter.expression.And;
import org.locationtech.geowave.core.store.query.filter.expression.ComparisonOperator.CompareOp;
import org.locationtech.geowave.core.store.query.filter.expression.Filter;
import org.locationtech.geowave.core.store.query.filter.expression.Or;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericBetween;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericComparisonOperator;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericFieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericLiteral;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextComparisonOperator;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextFieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextLiteral;

public class DeleteStatementTest extends AbstractGWQLTest {
  @Test
  public void testInvalidStatements() {
    final DataStore dataStore = createDataStore();
    // Missing from
    assertInvalidStatement(dataStore, "DELETE", "expecting FROM");
    // Missing type name
    assertInvalidStatement(dataStore, "DELETE FROM", "missing IDENTIFIER");
    // Missing from
    assertInvalidStatement(dataStore, "DELETE type", "missing FROM");
    // Nonexistent type
    assertInvalidStatement(dataStore, "DELETE FROM nonexistent", "No type named nonexistent");
    // Missing filter
    assertInvalidStatement(dataStore, "DELETE FROM type WHERE", "mismatched input '<EOF>'");
  }

  @Test
  public void testValidStatements() {
    final DataStore dataStore = createDataStore();
    GWQLParser.parseStatement(dataStore, "DELETE FROM type");
    GWQLParser.parseStatement(dataStore, "DELETE FROM type WHERE pop < 1");
    GWQLParser.parseStatement(dataStore, "DELETE FROM type WHERE pid BETWEEN 'a' AND 'b'");
    GWQLParser.parseStatement(dataStore, "DELETE FROM type WHERE pop::date AFTER '2020-01-01'");
    GWQLParser.parseStatement(dataStore, "DELETE FROM type WHERE ((((pop < 1))))");
  }


  @Test
  public void testDelete() throws ParseException, IOException {
    final DataStore dataStore = createDataStore();
    final String statement = "DELETE FROM type";
    final Statement gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof DeleteStatement);
    final DeleteStatement<?> deleteStatement = (DeleteStatement<?>) gwStatement;
    assertNotNull(deleteStatement.getAdapter());
    assertEquals("type", deleteStatement.getAdapter().getTypeName());
    assertNull(deleteStatement.getFilter());
  }

  @Test
  public void testComplexStatement() {
    final DataStore dataStore = createDataStore();
    final Statement statement =
        GWQLParser.parseStatement(
            dataStore,
            "DELETE FROM type "
                + "WHERE (pop < 1) "
                + "AND ((pop::date DURING '2020-01-01/2020-01-02' OR pid > 'a') AND (pop BETWEEN 0 AND 10 OR pid <= 'b'))");
    assertTrue(statement instanceof DeleteStatement);
    final DeleteStatement<?> deleteStatement = (DeleteStatement<?>) statement;
    assertNotNull(deleteStatement.getAdapter());
    assertEquals("type", deleteStatement.getAdapter().getTypeName());
    assertNotNull(deleteStatement.getFilter());
    final Filter filter = deleteStatement.getFilter();
    assertTrue(filter instanceof And);
    And andFilter = (And) filter;
    assertTrue(andFilter.getChildren().length == 2);
    assertTrue(andFilter.getChildren()[0] instanceof NumericComparisonOperator);
    NumericComparisonOperator compareOp = (NumericComparisonOperator) andFilter.getChildren()[0];
    assertTrue(compareOp.getCompareOp().equals(CompareOp.LESS_THAN));
    assertTrue(compareOp.getExpression1() instanceof NumericFieldValue);
    assertEquals("pop", ((NumericFieldValue) compareOp.getExpression1()).getFieldName());
    assertTrue(compareOp.getExpression2() instanceof NumericLiteral);
    assertEquals(1, ((NumericLiteral) compareOp.getExpression2()).getValue(), 0.00001);
    assertTrue(andFilter.getChildren()[1] instanceof And);
    andFilter = (And) andFilter.getChildren()[1];
    assertTrue(andFilter.getChildren().length == 2);
    assertTrue(andFilter.getChildren()[0] instanceof Or);
    Or orFilter = (Or) andFilter.getChildren()[0];
    assertTrue(orFilter.getChildren().length == 2);
    assertTrue(orFilter.getChildren()[0] instanceof During);
    final During during = (During) orFilter.getChildren()[0];
    assertTrue(during.getExpression1() instanceof TemporalFieldValue);
    assertEquals("pop", ((TemporalFieldValue) during.getExpression1()).getFieldName());
    assertTrue(during.getExpression2() instanceof TemporalLiteral);
    assertEquals(
        TemporalExpression.stringToDate("2020-01-01").getTime(),
        ((TemporalLiteral) during.getExpression2()).evaluateValue(null).getStart().toEpochMilli());
    assertEquals(
        TemporalExpression.stringToDate("2020-01-02").getTime(),
        ((TemporalLiteral) during.getExpression2()).evaluateValue(null).getEnd().toEpochMilli());
    assertTrue(orFilter.getChildren()[1] instanceof TextComparisonOperator);
    TextComparisonOperator textCompareOp = (TextComparisonOperator) orFilter.getChildren()[1];
    assertTrue(textCompareOp.getCompareOp().equals(CompareOp.GREATER_THAN));
    assertTrue(textCompareOp.getExpression1() instanceof TextFieldValue);
    assertEquals("pid", ((TextFieldValue) textCompareOp.getExpression1()).getFieldName());
    assertTrue(textCompareOp.getExpression2() instanceof TextLiteral);
    assertEquals("a", ((TextLiteral) textCompareOp.getExpression2()).getValue());
    assertTrue(andFilter.getChildren()[1] instanceof Or);
    orFilter = (Or) andFilter.getChildren()[1];
    assertTrue(orFilter.getChildren().length == 2);
    assertTrue(orFilter.getChildren()[0] instanceof NumericBetween);
    NumericBetween between = (NumericBetween) orFilter.getChildren()[0];
    assertTrue(between.getValue() instanceof NumericFieldValue);
    assertEquals("pop", ((NumericFieldValue) between.getValue()).getFieldName());
    assertTrue(between.getLowerBound() instanceof NumericLiteral);
    assertEquals(0, ((NumericLiteral) between.getLowerBound()).getValue(), 0.00001);
    assertTrue(between.getUpperBound() instanceof NumericLiteral);
    assertEquals(10, ((NumericLiteral) between.getUpperBound()).getValue(), 0.00001);
    assertTrue(orFilter.getChildren()[1] instanceof TextComparisonOperator);
    textCompareOp = (TextComparisonOperator) orFilter.getChildren()[1];
    assertTrue(textCompareOp.getCompareOp().equals(CompareOp.LESS_THAN_OR_EQUAL));
    assertTrue(textCompareOp.getExpression1() instanceof TextFieldValue);
    assertEquals("pid", ((TextFieldValue) textCompareOp.getExpression1()).getFieldName());
    assertTrue(textCompareOp.getExpression2() instanceof TextLiteral);
    assertEquals("b", ((TextLiteral) textCompareOp.getExpression2()).getValue());


  }

  @Test
  public void testDeleteWithFilter() throws ParseException, IOException {
    final DataStore dataStore = createDataStore();
    final String statement =
        "DELETE FROM type WHERE BBOX(geometry,27.20,41.30,27.30,41.20) and start during '2005-05-19T20:32:56Z/2005-05-19T21:32:56Z'";
    final Statement gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof DeleteStatement);
    final DeleteStatement<?> deleteStatement = (DeleteStatement<?>) gwStatement;
    assertNotNull(deleteStatement.getAdapter());
    assertEquals("type", deleteStatement.getAdapter().getTypeName());
    assertNotNull(deleteStatement.getFilter());
    final Filter filter = deleteStatement.getFilter();
    assertTrue(filter instanceof And);
    final And andFilter = (And) filter;
    assertTrue(andFilter.getChildren().length == 2);
    assertTrue(andFilter.getChildren()[0] instanceof BBox);
    assertTrue(andFilter.getChildren()[1] instanceof During);
  }
}

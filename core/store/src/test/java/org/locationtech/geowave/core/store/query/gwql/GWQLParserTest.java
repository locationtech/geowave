/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
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
import static org.junit.Assert.assertTrue;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStream;
import org.junit.Test;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.query.filter.expression.And;
import org.locationtech.geowave.core.store.query.filter.expression.ComparisonOperator.CompareOp;
import org.locationtech.geowave.core.store.query.filter.expression.Filter;
import org.locationtech.geowave.core.store.query.filter.expression.IsNotNull;
import org.locationtech.geowave.core.store.query.filter.expression.IsNull;
import org.locationtech.geowave.core.store.query.filter.expression.Not;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.Abs;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.Add;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.Divide;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.Multiply;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericComparisonOperator;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericFieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.NumericLiteral;
import org.locationtech.geowave.core.store.query.filter.expression.numeric.Subtract;
import org.locationtech.geowave.core.store.query.filter.expression.text.Concat;
import org.locationtech.geowave.core.store.query.filter.expression.text.Contains;
import org.locationtech.geowave.core.store.query.filter.expression.text.EndsWith;
import org.locationtech.geowave.core.store.query.filter.expression.text.StartsWith;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextBinaryPredicate;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextComparisonOperator;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextFieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextLiteral;
import org.locationtech.geowave.core.store.query.gwql.parse.GWQLLexer;
import org.locationtech.geowave.core.store.query.gwql.parse.GWQLParser;
import org.locationtech.geowave.core.store.query.gwql.statement.SelectStatement;
import org.locationtech.geowave.core.store.query.gwql.statement.Statement;

public class GWQLParserTest extends AbstractGWQLTest {

  @Test
  public void testFilters() {
    final DataStore dataStore = createDataStore();
    String statement = "SELECT * FROM type WHERE pop IS NULL";
    Statement gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    SelectStatement<?> selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    Filter filter = selectStatement.getFilter();
    assertTrue(filter instanceof IsNull);
    assertTrue(((IsNull) filter).getExpression() instanceof NumericFieldValue);
    assertEquals("pop", ((NumericFieldValue) ((IsNull) filter).getExpression()).getFieldName());

    statement = "SELECT * FROM type WHERE pop IS NOT NULL";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof IsNotNull);
    assertTrue(((IsNotNull) filter).getExpression() instanceof NumericFieldValue);
    assertEquals("pop", ((NumericFieldValue) ((IsNotNull) filter).getExpression()).getFieldName());

    statement = "SELECT * FROM type WHERE NOT pop IS NOT NULL";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof Not);
    final Not not = (Not) filter;
    assertTrue(not.getFilter() instanceof IsNotNull);
    filter = not.getFilter();
    assertTrue(((IsNotNull) filter).getExpression() instanceof NumericFieldValue);
    assertEquals("pop", ((NumericFieldValue) ((IsNotNull) filter).getExpression()).getFieldName());
  }

  @Test
  public void testInvalidFilters() {
    final DataStore dataStore = createDataStore();
    assertInvalidStatement(
        dataStore,
        "SELECT * FROM type WHERE pop > pid",
        "Comparison operators can only be used on comparable expressions");
    assertInvalidStatement(
        dataStore,
        "SELECT * FROM type WHERE pop < pid",
        "Comparison operators can only be used on comparable expressions");
    assertInvalidStatement(
        dataStore,
        "SELECT * FROM type WHERE pop >= pid",
        "Comparison operators can only be used on comparable expressions");
    assertInvalidStatement(
        dataStore,
        "SELECT * FROM type WHERE pop <= pid",
        "Comparison operators can only be used on comparable expressions");
    assertInvalidStatement(
        dataStore,
        "SELECT * FROM type WHERE pop BETWEEN pid AND comment",
        "The BETWEEN operation is only supported for comparable expressions");
    assertInvalidStatement(
        dataStore,
        "SELECT * FROM type WHERE nonexistent > 5",
        "Field nonexistent did not exist in the specified type");
    assertInvalidStatement(
        dataStore,
        "SELECT * FROM type WHERE pid + pid > 5",
        "Math operations require numeric expressions");
    assertInvalidStatement(
        dataStore,
        "SELECT * FROM type WHERE pid - pid > 5",
        "Math operations require numeric expressions");
    assertInvalidStatement(
        dataStore,
        "SELECT * FROM type WHERE pid * pid > 5",
        "Math operations require numeric expressions");
    assertInvalidStatement(
        dataStore,
        "SELECT * FROM type WHERE pid / pid > 5",
        "Math operations require numeric expressions");
    assertInvalidStatement(
        dataStore,
        "SELECT * FROM type WHERE nonexistent(pid) > 5",
        "No expression function was found with the name: nonexistent");
    assertInvalidStatement(
        dataStore,
        "SELECT * FROM type WHERE nonexistent(pid)",
        "No predicate function was found with the name: nonexistent");
    assertInvalidStatement(
        dataStore,
        "SELECT * FROM type WHERE pid nonexistent pid",
        "No 'nonexistent' operator was found");
    assertInvalidStatement(
        dataStore,
        "SELECT * FROM type WHERE pid::nonexistent > 5",
        "Type 'nonexistent' is undefined");
  }

  @Test
  public void testExpressionFunctions() {
    final DataStore dataStore = createDataStore();
    final String statement =
        "SELECT * FROM type WHERE abs(pop) > 10 AND strStartsWith(concat(pid, 'value'), 'abc')";
    final Statement gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    final SelectStatement<?> selectStatement = (SelectStatement<?>) gwStatement;
    assertFalse(selectStatement.isAggregation());
    assertNotNull(selectStatement.getAdapter());
    assertEquals("type", selectStatement.getAdapter().getTypeName());
    assertNotNull(selectStatement.getFilter());
    final Filter filter = selectStatement.getFilter();
    assertTrue(filter instanceof And);
    final And and = (And) filter;
    assertEquals(2, and.getChildren().length);
    assertTrue(and.getChildren()[0] instanceof NumericComparisonOperator);
    final NumericComparisonOperator compareOp = (NumericComparisonOperator) and.getChildren()[0];
    assertTrue(compareOp.getCompareOp().equals(CompareOp.GREATER_THAN));
    assertTrue(compareOp.getExpression1() instanceof Abs);
    assertTrue(((Abs) compareOp.getExpression1()).getExpression() instanceof NumericFieldValue);
    assertEquals(
        "pop",
        ((NumericFieldValue) ((Abs) compareOp.getExpression1()).getExpression()).getFieldName());
    assertTrue(compareOp.getExpression2() instanceof NumericLiteral);
    assertEquals(10.0, ((NumericLiteral) compareOp.getExpression2()).getValue(), 0.00001);
    assertTrue(and.getChildren()[1] instanceof StartsWith);
    final StartsWith startsWith = (StartsWith) and.getChildren()[1];
    assertTrue(startsWith.getExpression1() instanceof Concat);
    assertTrue(((Concat) startsWith.getExpression1()).getExpression1() instanceof TextFieldValue);
    assertEquals(
        "pid",
        ((TextFieldValue) ((Concat) startsWith.getExpression1()).getExpression1()).getFieldName());
    assertTrue(((Concat) startsWith.getExpression1()).getExpression2() instanceof TextLiteral);
    assertEquals(
        "value",
        ((TextLiteral) ((Concat) startsWith.getExpression1()).getExpression2()).getValue());
    assertTrue(startsWith.getExpression2() instanceof TextLiteral);
    assertEquals("abc", ((TextLiteral) startsWith.getExpression2()).getValue());
  }

  @Test
  public void testTextLiterals() {
    assertEquals("POINT(1 1)", parseTextLiteral("'POINT(1 1)'").getValue());
    assertEquals("can't brea'k", parseTextLiteral("'can''t brea''k'").getValue());
    assertEquals("can't break", parseTextLiteral("'can\\'t break'").getValue());
    assertEquals("can''t break", parseTextLiteral("'can\\'''t break'").getValue());
    assertEquals("can't\tbreak\n", parseTextLiteral("'can''t\tbreak\n'").getValue());
    assertEquals("can't\\break", parseTextLiteral("'can''t\\\\break'").getValue());
  }

  private TextLiteral parseTextLiteral(final String text) {
    final GWQLLexer lexer = new GWQLLexer(CharStreams.fromString(text));
    final TokenStream tokenStream = new CommonTokenStream(lexer);
    final GWQLParser parser = new GWQLParser(tokenStream);
    return parser.textLiteral().value;
  }

  @Test
  public void testTextPredicateFunctions() {
    final DataStore dataStore = createDataStore();
    String statement = "SELECT * FROM type WHERE strStartsWith(pid, 'val')";
    Statement gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    SelectStatement<?> selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    Filter filter = selectStatement.getFilter();
    assertTrue(filter instanceof StartsWith);
    TextBinaryPredicate predicate = (TextBinaryPredicate) filter;
    assertFalse(predicate.isIgnoreCase());
    assertTrue(predicate.getExpression1() instanceof TextFieldValue);
    assertEquals("pid", ((TextFieldValue) predicate.getExpression1()).getFieldName());
    assertTrue(predicate.getExpression2() instanceof TextLiteral);
    assertEquals("val", ((TextLiteral) predicate.getExpression2()).getValue());

    statement = "SELECT * FROM type WHERE strStartsWith(pid, 'val', true)";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof StartsWith);
    predicate = (TextBinaryPredicate) filter;
    assertTrue(predicate.isIgnoreCase());
    assertTrue(predicate.getExpression1() instanceof TextFieldValue);
    assertEquals("pid", ((TextFieldValue) predicate.getExpression1()).getFieldName());
    assertTrue(predicate.getExpression2() instanceof TextLiteral);
    assertEquals("val", ((TextLiteral) predicate.getExpression2()).getValue());

    statement = "SELECT * FROM type WHERE strEndsWith(pid, 'val')";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof EndsWith);
    predicate = (TextBinaryPredicate) filter;
    assertFalse(predicate.isIgnoreCase());
    assertTrue(predicate.getExpression1() instanceof TextFieldValue);
    assertEquals("pid", ((TextFieldValue) predicate.getExpression1()).getFieldName());
    assertTrue(predicate.getExpression2() instanceof TextLiteral);
    assertEquals("val", ((TextLiteral) predicate.getExpression2()).getValue());

    statement = "SELECT * FROM type WHERE strEndsWith(pid, 'val', true)";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof EndsWith);
    predicate = (TextBinaryPredicate) filter;
    assertTrue(predicate.isIgnoreCase());
    assertTrue(predicate.getExpression1() instanceof TextFieldValue);
    assertEquals("pid", ((TextFieldValue) predicate.getExpression1()).getFieldName());
    assertTrue(predicate.getExpression2() instanceof TextLiteral);
    assertEquals("val", ((TextLiteral) predicate.getExpression2()).getValue());

    statement = "SELECT * FROM type WHERE strContains(pid, 'val')";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof Contains);
    predicate = (TextBinaryPredicate) filter;
    assertFalse(predicate.isIgnoreCase());
    assertTrue(predicate.getExpression1() instanceof TextFieldValue);
    assertEquals("pid", ((TextFieldValue) predicate.getExpression1()).getFieldName());
    assertTrue(predicate.getExpression2() instanceof TextLiteral);
    assertEquals("val", ((TextLiteral) predicate.getExpression2()).getValue());

    statement = "SELECT * FROM type WHERE strContains(pid, 'val', true)";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof Contains);
    predicate = (TextBinaryPredicate) filter;
    assertTrue(predicate.isIgnoreCase());
    assertTrue(predicate.getExpression1() instanceof TextFieldValue);
    assertEquals("pid", ((TextFieldValue) predicate.getExpression1()).getFieldName());
    assertTrue(predicate.getExpression2() instanceof TextLiteral);
    assertEquals("val", ((TextLiteral) predicate.getExpression2()).getValue());
  }

  @Test
  public void testMathExpression() {
    final DataStore dataStore = createDataStore();
    String statement = "SELECT * FROM type WHERE pop + 5 > 25";
    Statement gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    SelectStatement<?> selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    Filter filter = selectStatement.getFilter();
    assertTrue(filter instanceof NumericComparisonOperator);
    NumericComparisonOperator compare = (NumericComparisonOperator) filter;
    assertEquals(CompareOp.GREATER_THAN, compare.getCompareOp());
    assertTrue(compare.getExpression1() instanceof Add);
    Add add = (Add) compare.getExpression1();
    assertTrue(add.getExpression1() instanceof NumericFieldValue);
    assertEquals("pop", ((NumericFieldValue) add.getExpression1()).getFieldName());
    assertTrue(add.getExpression2() instanceof NumericLiteral);
    assertEquals(5, ((NumericLiteral) add.getExpression2()).getValue(), 0.000001);
    assertTrue(compare.getExpression2() instanceof NumericLiteral);
    assertEquals(25, ((NumericLiteral) compare.getExpression2()).getValue(), 0.000001);

    statement = "SELECT * FROM type WHERE pop - 5 > 25";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof NumericComparisonOperator);
    compare = (NumericComparisonOperator) filter;
    assertEquals(CompareOp.GREATER_THAN, compare.getCompareOp());
    assertTrue(compare.getExpression1() instanceof Subtract);
    Subtract subtract = (Subtract) compare.getExpression1();
    assertTrue(subtract.getExpression1() instanceof NumericFieldValue);
    assertEquals("pop", ((NumericFieldValue) subtract.getExpression1()).getFieldName());
    assertTrue(subtract.getExpression2() instanceof NumericLiteral);
    assertEquals(5, ((NumericLiteral) subtract.getExpression2()).getValue(), 0.000001);
    assertTrue(compare.getExpression2() instanceof NumericLiteral);
    assertEquals(25, ((NumericLiteral) compare.getExpression2()).getValue(), 0.000001);

    statement = "SELECT * FROM type WHERE pop * 5 > 25";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof NumericComparisonOperator);
    compare = (NumericComparisonOperator) filter;
    assertEquals(CompareOp.GREATER_THAN, compare.getCompareOp());
    assertTrue(compare.getExpression1() instanceof Multiply);
    Multiply multiply = (Multiply) compare.getExpression1();
    assertTrue(multiply.getExpression1() instanceof NumericFieldValue);
    assertEquals("pop", ((NumericFieldValue) multiply.getExpression1()).getFieldName());
    assertTrue(multiply.getExpression2() instanceof NumericLiteral);
    assertEquals(5, ((NumericLiteral) multiply.getExpression2()).getValue(), 0.000001);
    assertTrue(compare.getExpression2() instanceof NumericLiteral);
    assertEquals(25, ((NumericLiteral) compare.getExpression2()).getValue(), 0.000001);

    statement = "SELECT * FROM type WHERE pop / 5 > 25";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof NumericComparisonOperator);
    compare = (NumericComparisonOperator) filter;
    assertEquals(CompareOp.GREATER_THAN, compare.getCompareOp());
    assertTrue(compare.getExpression1() instanceof Divide);
    Divide divide = (Divide) compare.getExpression1();
    assertTrue(divide.getExpression1() instanceof NumericFieldValue);
    assertEquals("pop", ((NumericFieldValue) divide.getExpression1()).getFieldName());
    assertTrue(divide.getExpression2() instanceof NumericLiteral);
    assertEquals(5, ((NumericLiteral) divide.getExpression2()).getValue(), 0.000001);
    assertTrue(compare.getExpression2() instanceof NumericLiteral);
    assertEquals(25, ((NumericLiteral) compare.getExpression2()).getValue(), 0.000001);

    // Test order of operations
    // (pop + ((5 * (pop - 8)) / 6))
    statement = "SELECT * FROM type WHERE pop + 5 * (pop - 8) / 6 > 25";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof NumericComparisonOperator);
    compare = (NumericComparisonOperator) filter;
    assertEquals(CompareOp.GREATER_THAN, compare.getCompareOp());
    assertTrue(compare.getExpression1() instanceof Add);
    add = (Add) compare.getExpression1();
    assertTrue(add.getExpression1() instanceof NumericFieldValue);
    assertEquals("pop", ((NumericFieldValue) add.getExpression1()).getFieldName());
    assertTrue(add.getExpression2() instanceof Divide);
    divide = (Divide) add.getExpression2();
    assertTrue(divide.getExpression1() instanceof Multiply);
    multiply = (Multiply) divide.getExpression1();
    assertTrue(multiply.getExpression1() instanceof NumericLiteral);
    assertEquals(5, ((NumericLiteral) multiply.getExpression1()).getValue(), 0.000001);
    assertTrue(multiply.getExpression2() instanceof Subtract);
    subtract = (Subtract) multiply.getExpression2();
    assertTrue(subtract.getExpression1() instanceof NumericFieldValue);
    assertEquals("pop", ((NumericFieldValue) subtract.getExpression1()).getFieldName());
    assertTrue(subtract.getExpression2() instanceof NumericLiteral);
    assertEquals(8, ((NumericLiteral) subtract.getExpression2()).getValue(), 0.000001);
    assertTrue(divide.getExpression2() instanceof NumericLiteral);
    assertEquals(6, ((NumericLiteral) divide.getExpression2()).getValue(), 0.000001);
    assertTrue(compare.getExpression2() instanceof NumericLiteral);
    assertEquals(25, ((NumericLiteral) compare.getExpression2()).getValue(), 0.000001);
  }

  @Test
  public void testComparisonOperators() {
    final DataStore dataStore = createDataStore();
    String statement = "SELECT * FROM type WHERE pop > 25";
    Statement gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    SelectStatement<?> selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    Filter filter = selectStatement.getFilter();
    assertTrue(filter instanceof NumericComparisonOperator);
    NumericComparisonOperator compare = (NumericComparisonOperator) filter;
    assertEquals(CompareOp.GREATER_THAN, compare.getCompareOp());
    assertTrue(compare.getExpression1() instanceof NumericFieldValue);
    assertEquals("pop", ((NumericFieldValue) compare.getExpression1()).getFieldName());
    assertTrue(compare.getExpression2() instanceof NumericLiteral);
    assertEquals(25, ((NumericLiteral) compare.getExpression2()).getValue(), 0.000001);

    statement = "SELECT * FROM type WHERE pop >= 25";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof NumericComparisonOperator);
    compare = (NumericComparisonOperator) filter;
    assertEquals(CompareOp.GREATER_THAN_OR_EQUAL, compare.getCompareOp());
    assertTrue(compare.getExpression1() instanceof NumericFieldValue);
    assertEquals("pop", ((NumericFieldValue) compare.getExpression1()).getFieldName());
    assertTrue(compare.getExpression2() instanceof NumericLiteral);
    assertEquals(25, ((NumericLiteral) compare.getExpression2()).getValue(), 0.000001);

    statement = "SELECT * FROM type WHERE pop < 25";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof NumericComparisonOperator);
    compare = (NumericComparisonOperator) filter;
    assertEquals(CompareOp.LESS_THAN, compare.getCompareOp());
    assertTrue(compare.getExpression1() instanceof NumericFieldValue);
    assertEquals("pop", ((NumericFieldValue) compare.getExpression1()).getFieldName());
    assertTrue(compare.getExpression2() instanceof NumericLiteral);
    assertEquals(25, ((NumericLiteral) compare.getExpression2()).getValue(), 0.000001);

    statement = "SELECT * FROM type WHERE pop <= 25";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof NumericComparisonOperator);
    compare = (NumericComparisonOperator) filter;
    assertEquals(CompareOp.LESS_THAN_OR_EQUAL, compare.getCompareOp());
    assertTrue(compare.getExpression1() instanceof NumericFieldValue);
    assertEquals("pop", ((NumericFieldValue) compare.getExpression1()).getFieldName());
    assertTrue(compare.getExpression2() instanceof NumericLiteral);
    assertEquals(25, ((NumericLiteral) compare.getExpression2()).getValue(), 0.000001);

    statement = "SELECT * FROM type WHERE pop = 25";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof NumericComparisonOperator);
    compare = (NumericComparisonOperator) filter;
    assertEquals(CompareOp.EQUAL_TO, compare.getCompareOp());
    assertTrue(compare.getExpression1() instanceof NumericFieldValue);
    assertEquals("pop", ((NumericFieldValue) compare.getExpression1()).getFieldName());
    assertTrue(compare.getExpression2() instanceof NumericLiteral);
    assertEquals(25, ((NumericLiteral) compare.getExpression2()).getValue(), 0.000001);

    statement = "SELECT * FROM type WHERE pop <> 25";
    gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    filter = selectStatement.getFilter();
    assertTrue(filter instanceof NumericComparisonOperator);
    compare = (NumericComparisonOperator) filter;
    assertEquals(CompareOp.NOT_EQUAL_TO, compare.getCompareOp());
    assertTrue(compare.getExpression1() instanceof NumericFieldValue);
    assertEquals("pop", ((NumericFieldValue) compare.getExpression1()).getFieldName());
    assertTrue(compare.getExpression2() instanceof NumericLiteral);
    assertEquals(25, ((NumericLiteral) compare.getExpression2()).getValue(), 0.000001);
  }

  @Test
  public void testCasting() {
    final DataStore dataStore = createDataStore();

    String statement = "SELECT * FROM type WHERE pop::text = '15'";
    Statement gwStatement = GWQLParser.parseStatement(dataStore, statement);
    assertTrue(gwStatement instanceof SelectStatement);
    SelectStatement<?> selectStatement = (SelectStatement<?>) gwStatement;
    assertNotNull(selectStatement.getFilter());
    Filter filter = selectStatement.getFilter();
    assertTrue(filter instanceof TextComparisonOperator);
    final TextComparisonOperator textCompare = (TextComparisonOperator) filter;
    assertEquals(CompareOp.EQUAL_TO, textCompare.getCompareOp());
    assertTrue(textCompare.getExpression1() instanceof TextFieldValue);
    assertEquals("pop", ((TextFieldValue) textCompare.getExpression1()).getFieldName());
    assertTrue(textCompare.getExpression2() instanceof TextLiteral);
  }

}

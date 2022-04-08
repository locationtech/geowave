/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import org.junit.Test;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.adapter.AbstractDataTypeAdapterTest.TestType;
import org.locationtech.geowave.core.store.adapter.AbstractDataTypeAdapterTest.TestTypeBasicDataAdapter;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.query.filter.expression.ComparisonOperator.CompareOp;
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
import org.locationtech.geowave.core.store.query.filter.expression.text.TextFieldValue;
import org.locationtech.geowave.core.store.query.filter.expression.text.TextLiteral;
import com.google.common.collect.Lists;

public class FilterExpressionTest {

  private static final double EPSILON = 0.0000001;

  @Test
  public void testNumericExpressions() {
    final DataTypeAdapter<TestType> adapter = new TestTypeBasicDataAdapter();
    final TestType entry = new TestType("test", 1.3, 5, true);
    final TestType entryNulls = new TestType(null, null, null, null);
    final NumericLiteral doubleLit = NumericLiteral.of(0.5);
    final NumericLiteral integerLit = NumericLiteral.of(1);
    final NumericFieldValue doubleField = NumericFieldValue.of("doubleField");
    final NumericFieldValue intField = NumericFieldValue.of("intField");

    assertEquals(1.3, (double) doubleField.evaluateValue(adapter, entry), EPSILON);
    assertEquals(5, intField.evaluateValue(adapter, entry).intValue());

    // Test comparisons
    assertTrue(doubleLit.isLessThan(integerLit).evaluate(adapter, entry));
    assertFalse(integerLit.isLessThan(doubleLit).evaluate(adapter, entry));
    assertTrue(doubleField.isLessThan(1.5).evaluate(adapter, entry));
    assertFalse(doubleField.isLessThan(doubleLit).evaluate(adapter, entry));
    assertFalse(doubleField.isLessThan(integerLit).evaluate(adapter, entry));
    assertTrue(doubleField.isLessThan(intField).evaluate(adapter, entry));
    assertFalse(intField.isLessThan(doubleField).evaluate(adapter, entry));
    assertTrue(doubleLit.isGreaterThan(0).evaluate(adapter, entry));
    assertFalse(doubleLit.isGreaterThan(1).evaluate(adapter, entry));
    assertTrue(intField.isGreaterThan(1.0).evaluate(adapter, entry));
    assertTrue(intField.isGreaterThan(doubleLit).evaluate(adapter, entry));
    assertTrue(intField.isGreaterThan(integerLit).evaluate(adapter, entry));
    assertFalse(intField.isGreaterThan(6).evaluate(adapter, entry));
    assertTrue(intField.isGreaterThan(doubleField).evaluate(adapter, entry));
    assertFalse(doubleField.isGreaterThan(intField).evaluate(adapter, entry));
    assertTrue(integerLit.isGreaterThanOrEqualTo(0).evaluate(adapter, entry));
    assertTrue(integerLit.isGreaterThanOrEqualTo(integerLit).evaluate(adapter, entry));
    assertFalse(integerLit.isGreaterThanOrEqualTo(2).evaluate(adapter, entry));
    assertTrue(doubleLit.isLessThanOrEqualTo(1).evaluate(adapter, entry));
    assertTrue(doubleLit.isLessThanOrEqualTo(0.5).evaluate(adapter, entry));
    assertFalse(doubleLit.isLessThanOrEqualTo(0).evaluate(adapter, entry));
    assertTrue(doubleLit.isEqualTo(0.5).evaluate(adapter, entry));
    assertFalse(doubleLit.isEqualTo(0.4).evaluate(adapter, entry));
    assertTrue(doubleLit.isNotEqualTo(0.4).evaluate(adapter, entry));
    assertFalse(doubleLit.isNotEqualTo(0.5).evaluate(adapter, entry));
    assertFalse(doubleLit.isNull().evaluate(adapter, entry));
    assertFalse(integerLit.isNull().evaluate(adapter, entry));
    assertFalse(doubleField.isNull().evaluate(adapter, entry));
    assertFalse(intField.isNull().evaluate(adapter, entry));
    assertTrue(doubleField.isNull().evaluate(adapter, entryNulls));
    assertTrue(intField.isNull().evaluate(adapter, entryNulls));
    assertTrue(doubleLit.isNotNull().evaluate(adapter, entry));
    assertTrue(integerLit.isNotNull().evaluate(adapter, entry));
    assertTrue(doubleField.isNotNull().evaluate(adapter, entry));
    assertTrue(intField.isNotNull().evaluate(adapter, entry));
    assertFalse(doubleField.isNotNull().evaluate(adapter, entryNulls));
    assertFalse(intField.isNotNull().evaluate(adapter, entryNulls));
    assertFalse(doubleField.isLessThan(null).evaluate(adapter, entry));
    assertFalse(doubleField.isGreaterThan(null).evaluate(adapter, entry));
    assertFalse(doubleField.isLessThanOrEqualTo(null).evaluate(adapter, entry));
    assertFalse(doubleField.isGreaterThanOrEqualTo(null).evaluate(adapter, entry));
    assertFalse(doubleField.isEqualTo(null).evaluate(adapter, entry));
    assertTrue(doubleField.isNotEqualTo(null).evaluate(adapter, entry));
    assertTrue(doubleField.isEqualTo(intField).evaluate(adapter, entryNulls));
    assertFalse(doubleField.isEqualTo(doubleLit).evaluate(adapter, entryNulls));
    assertFalse(doubleField.isNotEqualTo(null).evaluate(adapter, entryNulls));
    assertTrue(doubleField.isNotEqualTo(doubleLit).evaluate(adapter, entryNulls));
    assertTrue(doubleLit.isBetween(0, 1).evaluate(adapter, entry));
    assertFalse(doubleLit.isBetween(integerLit, intField).evaluate(adapter, entry));
    assertTrue(doubleField.isBetween(doubleLit, intField).evaluate(adapter, entry));
    assertFalse(doubleField.isBetween(doubleLit, intField).evaluate(adapter, entryNulls));
    assertFalse(doubleLit.isBetween(integerLit, intField).evaluate(adapter, entryNulls));
    assertFalse(doubleLit.isBetween(intField, integerLit).evaluate(adapter, entryNulls));
    assertFalse(intField.isBetween(doubleLit, integerLit).evaluate(adapter, entry));

    assertTrue(integerLit.add(1).isLiteral());
    assertFalse(intField.add(1).isLiteral());
    assertTrue(integerLit.add(doubleLit).isLiteral());
    assertFalse(integerLit.add(doubleField).isLiteral());
    assertTrue(doubleLit.abs().isLiteral());
    assertFalse(doubleField.abs().isLiteral());

    // Test math
    assertNull(doubleField.abs().evaluateValue(adapter, entryNulls));
    assertEquals(5.3, (double) NumericLiteral.of(-5.3).abs().evaluateValue(null, null), EPSILON);
    assertEquals(5.3, (double) NumericLiteral.of(5.3).abs().evaluateValue(null, null), EPSILON);
    assertEquals(
        2.7,
        (double) doubleField.abs().evaluateValue(adapter, new TestType("test", -2.7, 5, true)),
        EPSILON);
    assertEquals(
        5,
        (double) intField.abs().evaluateValue(adapter, new TestType("test", -2.7, 5, true)),
        EPSILON);
    assertEquals(
        28,
        (double) NumericLiteral.of(5).add(15).divideBy(4).multiplyBy(8).subtract(12).evaluateValue(
            null,
            null),
        EPSILON);
    assertNull(doubleField.add(1).evaluateValue(adapter, entryNulls));
    assertNull(doubleLit.add(intField).evaluateValue(adapter, entryNulls));
    assertNull(doubleField.add(intField).evaluateValue(adapter, entryNulls));

    // Test complex
    // ((1.3 + 0.8) * (5 - 1)) / 3.2
    assertEquals(
        2.625,
        (double) doubleField.add(0.8).multiplyBy(intField.subtract(integerLit)).divideBy(
            3.2).evaluateValue(adapter, entry),
        EPSILON);

    try {
      integerLit.add("test");
      fail();
    } catch (RuntimeException e) {
      // Expected
    }

    // Test serialization
    byte[] bytes = PersistenceUtils.toBinary(doubleField.add(5));
    final Add add = (Add) PersistenceUtils.fromBinary(bytes);
    assertTrue(add.getExpression1() instanceof NumericFieldValue);
    assertEquals("doubleField", ((NumericFieldValue) add.getExpression1()).getFieldName());
    assertTrue(add.getExpression2() instanceof NumericLiteral);
    assertEquals(5L, ((Number) ((NumericLiteral) add.getExpression2()).getValue()).longValue());

    bytes = PersistenceUtils.toBinary(doubleField.subtract(5));
    final Subtract subtract = (Subtract) PersistenceUtils.fromBinary(bytes);
    assertTrue(subtract.getExpression1() instanceof NumericFieldValue);
    assertEquals("doubleField", ((NumericFieldValue) subtract.getExpression1()).getFieldName());
    assertTrue(subtract.getExpression2() instanceof NumericLiteral);
    assertEquals(
        5L,
        ((Number) ((NumericLiteral) subtract.getExpression2()).getValue()).longValue());

    bytes = PersistenceUtils.toBinary(doubleField.multiplyBy(5));
    final Multiply multiply = (Multiply) PersistenceUtils.fromBinary(bytes);
    assertTrue(multiply.getExpression1() instanceof NumericFieldValue);
    assertEquals("doubleField", ((NumericFieldValue) multiply.getExpression1()).getFieldName());
    assertTrue(multiply.getExpression2() instanceof NumericLiteral);
    assertEquals(
        5L,
        ((Number) ((NumericLiteral) multiply.getExpression2()).getValue()).longValue());

    bytes = PersistenceUtils.toBinary(doubleField.divideBy(null));
    final Divide divide = (Divide) PersistenceUtils.fromBinary(bytes);
    assertTrue(divide.getExpression1() instanceof NumericFieldValue);
    assertEquals("doubleField", ((NumericFieldValue) divide.getExpression1()).getFieldName());
    assertTrue(divide.getExpression2() instanceof NumericLiteral);
    assertNull(((NumericLiteral) divide.getExpression2()).getValue());

    bytes = PersistenceUtils.toBinary(doubleField.abs());
    final Abs abs = (Abs) PersistenceUtils.fromBinary(bytes);
    assertTrue(abs.getExpression() instanceof NumericFieldValue);
    assertEquals("doubleField", ((NumericFieldValue) abs.getExpression()).getFieldName());

    bytes = PersistenceUtils.toBinary(doubleField.isLessThan(5));
    NumericComparisonOperator compareOp =
        (NumericComparisonOperator) PersistenceUtils.fromBinary(bytes);
    assertEquals(CompareOp.LESS_THAN, compareOp.getCompareOp());
    assertTrue(compareOp.getExpression1() instanceof NumericFieldValue);
    assertEquals("doubleField", ((NumericFieldValue) compareOp.getExpression1()).getFieldName());
    assertTrue(compareOp.getExpression2() instanceof NumericLiteral);
    assertEquals(
        5L,
        ((Number) ((NumericLiteral) compareOp.getExpression2()).getValue()).longValue());

    bytes = PersistenceUtils.toBinary(doubleField.isLessThanOrEqualTo(5));
    compareOp = (NumericComparisonOperator) PersistenceUtils.fromBinary(bytes);
    assertEquals(CompareOp.LESS_THAN_OR_EQUAL, compareOp.getCompareOp());
    assertTrue(compareOp.getExpression1() instanceof NumericFieldValue);
    assertEquals("doubleField", ((NumericFieldValue) compareOp.getExpression1()).getFieldName());
    assertTrue(compareOp.getExpression2() instanceof NumericLiteral);
    assertEquals(
        5L,
        ((Number) ((NumericLiteral) compareOp.getExpression2()).getValue()).longValue());

    bytes = PersistenceUtils.toBinary(doubleField.isGreaterThan(5));
    compareOp = (NumericComparisonOperator) PersistenceUtils.fromBinary(bytes);
    assertEquals(CompareOp.GREATER_THAN, compareOp.getCompareOp());
    assertTrue(compareOp.getExpression1() instanceof NumericFieldValue);
    assertEquals("doubleField", ((NumericFieldValue) compareOp.getExpression1()).getFieldName());
    assertTrue(compareOp.getExpression2() instanceof NumericLiteral);
    assertEquals(
        5L,
        ((Number) ((NumericLiteral) compareOp.getExpression2()).getValue()).longValue());

    bytes = PersistenceUtils.toBinary(doubleField.isGreaterThanOrEqualTo(5));
    compareOp = (NumericComparisonOperator) PersistenceUtils.fromBinary(bytes);
    assertEquals(CompareOp.GREATER_THAN_OR_EQUAL, compareOp.getCompareOp());
    assertTrue(compareOp.getExpression1() instanceof NumericFieldValue);
    assertEquals("doubleField", ((NumericFieldValue) compareOp.getExpression1()).getFieldName());
    assertTrue(compareOp.getExpression2() instanceof NumericLiteral);
    assertEquals(
        5L,
        ((Number) ((NumericLiteral) compareOp.getExpression2()).getValue()).longValue());

    bytes = PersistenceUtils.toBinary(doubleField.isEqualTo(5));
    compareOp = (NumericComparisonOperator) PersistenceUtils.fromBinary(bytes);
    assertEquals(CompareOp.EQUAL_TO, compareOp.getCompareOp());
    assertTrue(compareOp.getExpression1() instanceof NumericFieldValue);
    assertEquals("doubleField", ((NumericFieldValue) compareOp.getExpression1()).getFieldName());
    assertTrue(compareOp.getExpression2() instanceof NumericLiteral);
    assertEquals(
        5L,
        ((Number) ((NumericLiteral) compareOp.getExpression2()).getValue()).longValue());

    bytes = PersistenceUtils.toBinary(doubleField.isNotEqualTo(5));
    compareOp = (NumericComparisonOperator) PersistenceUtils.fromBinary(bytes);
    assertEquals(CompareOp.NOT_EQUAL_TO, compareOp.getCompareOp());
    assertTrue(compareOp.getExpression1() instanceof NumericFieldValue);
    assertEquals("doubleField", ((NumericFieldValue) compareOp.getExpression1()).getFieldName());
    assertTrue(compareOp.getExpression2() instanceof NumericLiteral);
    assertEquals(
        5L,
        ((Number) ((NumericLiteral) compareOp.getExpression2()).getValue()).longValue());

    bytes = PersistenceUtils.toBinary(doubleField.isBetween(5, 10));
    final Between<?, ?> between = (Between<?, ?>) PersistenceUtils.fromBinary(bytes);
    assertTrue(between.getValue() instanceof NumericFieldValue);
    assertEquals("doubleField", ((NumericFieldValue) between.getValue()).getFieldName());
    assertTrue(between.getLowerBound() instanceof NumericLiteral);
    assertEquals(5L, ((Number) ((NumericLiteral) between.getLowerBound()).getValue()).longValue());
    assertTrue(between.getUpperBound() instanceof NumericLiteral);
    assertEquals(10L, ((Number) ((NumericLiteral) between.getUpperBound()).getValue()).longValue());

    bytes = PersistenceUtils.toBinary(doubleField.isNull());
    final IsNull isNull = (IsNull) PersistenceUtils.fromBinary(bytes);
    assertTrue(isNull.getExpression() instanceof NumericFieldValue);
    assertEquals("doubleField", ((NumericFieldValue) isNull.getExpression()).getFieldName());

    bytes = PersistenceUtils.toBinary(doubleField.isNotNull());
    final IsNotNull isNotNull = (IsNotNull) PersistenceUtils.fromBinary(bytes);
    assertTrue(isNotNull.getExpression() instanceof NumericFieldValue);
    assertEquals("doubleField", ((NumericFieldValue) isNotNull.getExpression()).getFieldName());

    try {
      NumericFieldValue.of("name").evaluateValue(adapter, entry);
      fail();
    } catch (RuntimeException e) {
      // expected
    }
  }

  @Test
  public void testTextExpressions() {
    final DataTypeAdapter<TestType> adapter = new TestTypeBasicDataAdapter();
    final TestType entry = new TestType("test", 1.3, 5, true);
    final TestType entryNulls = new TestType(null, null, null, null);
    final TextLiteral textLit = TextLiteral.of("text");
    final TextLiteral valueLit = TextLiteral.of("value");
    final TextFieldValue textField = TextFieldValue.of("name");

    assertEquals("test", textField.evaluateValue(adapter, entry));
    assertNull(textField.evaluateValue(adapter, entryNulls));

    // Test comparisons
    assertTrue(textLit.isLessThan(valueLit).evaluate(adapter, entry));
    assertFalse(valueLit.isLessThan(textLit).evaluate(adapter, entry));
    assertTrue(textLit.isLessThan("tfxt").evaluate(adapter, entry));
    assertFalse(textLit.isLessThan("text").evaluate(adapter, entry));
    assertTrue(textField.isLessThan(textLit).evaluate(adapter, entry));
    assertFalse(valueLit.isLessThan(TextFieldValue.of("name")).evaluate(adapter, entry));
    assertFalse(textField.isLessThan(textLit).evaluate(adapter, entryNulls));
    assertFalse(textLit.isGreaterThan(valueLit).evaluate(adapter, entry));
    assertTrue(valueLit.isGreaterThan(textLit).evaluate(adapter, entry));
    assertFalse(textLit.isGreaterThan("text").evaluate(adapter, entry));
    assertTrue(textLit.isGreaterThan("tdxt").evaluate(adapter, entry));
    assertFalse(textField.isGreaterThan(textLit).evaluate(adapter, entry));
    assertTrue(valueLit.isGreaterThan(TextFieldValue.of("name")).evaluate(adapter, entry));
    assertFalse(textField.isGreaterThan(textLit).evaluate(adapter, entryNulls));
    assertTrue(textLit.isLessThanOrEqualTo(valueLit).evaluate(adapter, entry));
    assertFalse(valueLit.isLessThanOrEqualTo(textLit).evaluate(adapter, entry));
    assertTrue(textLit.isLessThanOrEqualTo("tfxt").evaluate(adapter, entry));
    assertFalse(textLit.isLessThanOrEqualTo("test").evaluate(adapter, entry));
    assertTrue(textField.isLessThanOrEqualTo(textLit).evaluate(adapter, entry));
    assertFalse(valueLit.isLessThanOrEqualTo(textField).evaluate(adapter, entry));
    assertTrue(valueLit.isLessThanOrEqualTo("value").evaluate(adapter, entry));
    assertFalse(textLit.isGreaterThanOrEqualTo(valueLit).evaluate(adapter, entry));
    assertTrue(valueLit.isGreaterThanOrEqualTo(textLit).evaluate(adapter, entry));
    assertTrue(textLit.isGreaterThanOrEqualTo("text").evaluate(adapter, entry));
    assertTrue(textLit.isGreaterThanOrEqualTo("tdxt").evaluate(adapter, entry));
    assertFalse(textField.isGreaterThanOrEqualTo(textLit).evaluate(adapter, entry));
    assertTrue(valueLit.isGreaterThanOrEqualTo(textField).evaluate(adapter, entry));
    assertTrue(textField.isGreaterThanOrEqualTo("test").evaluate(adapter, entry));
    assertTrue(textField.isEqualTo("test").evaluate(adapter, entry));
    assertFalse(textField.isEqualTo("TEST").evaluate(adapter, entry));
    assertTrue(textField.isEqualTo("TEST", true).evaluate(adapter, entry));
    assertFalse(textField.isEqualTo(textLit).evaluate(adapter, entry));
    assertFalse(textField.isNotEqualTo("test").evaluate(adapter, entry));
    assertTrue(textField.isNotEqualTo("TEST").evaluate(adapter, entry));
    assertFalse(textField.isNotEqualTo("TEST", true).evaluate(adapter, entry));
    assertTrue(textField.isNotEqualTo("TFST", true).evaluate(adapter, entry));
    assertTrue(textField.isNotEqualTo(textLit).evaluate(adapter, entry));
    assertFalse(textLit.isNull().evaluate(adapter, entry));
    assertFalse(valueLit.isNull().evaluate(adapter, entry));
    assertFalse(textField.isNull().evaluate(adapter, entry));
    assertTrue(textField.isNull().evaluate(adapter, entryNulls));
    assertTrue(textLit.isNotNull().evaluate(adapter, entry));
    assertTrue(valueLit.isNotNull().evaluate(adapter, entry));
    assertTrue(textField.isNotNull().evaluate(adapter, entry));
    assertFalse(textField.isNotNull().evaluate(adapter, entryNulls));
    assertFalse(textField.isLessThan(null).evaluate(adapter, entry));
    assertFalse(textField.isGreaterThan(null).evaluate(adapter, entry));
    assertFalse(textField.isLessThanOrEqualTo(null).evaluate(adapter, entry));
    assertFalse(textField.isGreaterThanOrEqualTo(null).evaluate(adapter, entry));
    assertFalse(textField.isEqualTo(null).evaluate(adapter, entry));
    assertTrue(textField.isNotEqualTo(null).evaluate(adapter, entry));
    assertTrue(textField.isEqualTo(textField).evaluate(adapter, entryNulls));
    assertFalse(textField.isEqualTo(textLit).evaluate(adapter, entryNulls));
    assertFalse(textField.isNotEqualTo(null).evaluate(adapter, entryNulls));
    assertTrue(textField.isNotEqualTo(valueLit).evaluate(adapter, entryNulls));
    assertTrue(textField.isBetween("a", "z").evaluate(adapter, entry));
    assertFalse(textLit.isBetween("u", "z").evaluate(adapter, entry));
    assertTrue(textLit.isBetween(textField, valueLit).evaluate(adapter, entry));
    assertFalse(textField.isBetween(textLit, valueLit).evaluate(adapter, entryNulls));
    assertFalse(textLit.isBetween(valueLit, textField).evaluate(adapter, entryNulls));
    assertFalse(textLit.isBetween(textField, valueLit).evaluate(adapter, entryNulls));
    assertFalse(valueLit.isBetween(textLit, textField).evaluate(adapter, entry));

    assertTrue(textLit.isLiteral());
    assertFalse(textField.isLiteral());
    assertTrue(textLit.concat(valueLit).isLiteral());
    assertFalse(textLit.concat(textField).isLiteral());
    assertFalse(textField.concat(textLit).isLiteral());

    // Test functions
    assertEquals("textvalue", textLit.concat(valueLit).evaluateValue(adapter, entry));
    assertEquals("text", textLit.concat(textField).evaluateValue(adapter, entryNulls));
    assertEquals("text", textField.concat(textLit).evaluateValue(adapter, entryNulls));
    assertEquals("text", textLit.concat(null).evaluateValue(adapter, entry));
    assertEquals("text1.5", textLit.concat(1.5).evaluateValue(adapter, entry));
    assertTrue(textLit.contains("ex").evaluate(adapter, entry));
    assertFalse(textLit.contains("EX").evaluate(adapter, entry));
    assertTrue(textLit.contains("EX", true).evaluate(adapter, entry));
    assertFalse(textField.contains(null).evaluate(adapter, entry));
    assertFalse(textField.contains("es").evaluate(adapter, entryNulls));
    assertTrue(textField.contains("test").evaluate(adapter, entry));
    assertTrue(textLit.startsWith("tex").evaluate(adapter, entry));
    assertFalse(textLit.startsWith("TEX").evaluate(adapter, entry));
    assertTrue(textLit.startsWith("TEX", true).evaluate(adapter, entry));
    assertFalse(textField.startsWith(null).evaluate(adapter, entry));
    assertFalse(textField.startsWith("tes").evaluate(adapter, entryNulls));
    assertTrue(textField.startsWith("test").evaluate(adapter, entry));
    assertTrue(textLit.endsWith("xt").evaluate(adapter, entry));
    assertFalse(textLit.endsWith("XT").evaluate(adapter, entry));
    assertTrue(textLit.endsWith("XT", true).evaluate(adapter, entry));
    assertFalse(textField.endsWith(null).evaluate(adapter, entry));
    assertFalse(textField.endsWith("st").evaluate(adapter, entryNulls));
    assertTrue(textField.endsWith("test").evaluate(adapter, entry));

    // Test serialization
    byte[] bytes = PersistenceUtils.toBinary(textField.concat("test"));
    final Concat concat = (Concat) PersistenceUtils.fromBinary(bytes);
    assertTrue(concat.getExpression1() instanceof TextFieldValue);
    assertEquals("name", ((TextFieldValue) concat.getExpression1()).getFieldName());
    assertTrue(concat.getExpression2() instanceof TextLiteral);
    assertEquals("test", (String) ((TextLiteral) concat.getExpression2()).getValue());

    bytes = PersistenceUtils.toBinary(textField.contains("test", true));
    final Contains contains = (Contains) PersistenceUtils.fromBinary(bytes);
    assertTrue(contains.isIgnoreCase());
    assertTrue(contains.getExpression1() instanceof TextFieldValue);
    assertEquals("name", ((TextFieldValue) contains.getExpression1()).getFieldName());
    assertTrue(contains.getExpression2() instanceof TextLiteral);
    assertEquals("test", (String) ((TextLiteral) contains.getExpression2()).getValue());

    bytes = PersistenceUtils.toBinary(textField.endsWith("test"));
    final EndsWith endsWith = (EndsWith) PersistenceUtils.fromBinary(bytes);
    assertFalse(endsWith.isIgnoreCase());
    assertTrue(endsWith.getExpression1() instanceof TextFieldValue);
    assertEquals("name", ((TextFieldValue) endsWith.getExpression1()).getFieldName());
    assertTrue(endsWith.getExpression2() instanceof TextLiteral);
    assertEquals("test", (String) ((TextLiteral) endsWith.getExpression2()).getValue());

    bytes = PersistenceUtils.toBinary(textField.startsWith(null));
    final StartsWith startsWith = (StartsWith) PersistenceUtils.fromBinary(bytes);
    assertFalse(startsWith.isIgnoreCase());
    assertTrue(startsWith.getExpression1() instanceof TextFieldValue);
    assertEquals("name", ((TextFieldValue) startsWith.getExpression1()).getFieldName());
    assertTrue(startsWith.getExpression2() instanceof TextLiteral);
    assertNull(((TextLiteral) startsWith.getExpression2()).getValue());
  }

  @Test
  public void testBooleanExpressions() {
    final DataTypeAdapter<TestType> adapter = new TestTypeBasicDataAdapter();
    final TestType entry = new TestType("test", 1.3, 5, true);
    final TestType entryFalse = new TestType("test", 1.3, 0, false);
    final TestType entryNulls = new TestType(null, null, null, null);
    final BooleanLiteral trueLit = BooleanLiteral.of(true);
    final BooleanLiteral falseLit = BooleanLiteral.of(false);
    final BooleanLiteral stringLit = BooleanLiteral.of("test");
    final BooleanLiteral nullLit = BooleanLiteral.of(null);
    final BooleanLiteral numberTrueLit = BooleanLiteral.of(1);
    final BooleanLiteral numberFalseLit = BooleanLiteral.of(0);
    final BooleanFieldValue booleanField = BooleanFieldValue.of("boolField");
    final BooleanFieldValue booleanIntField = BooleanFieldValue.of("intField");
    final BooleanFieldValue booleanStrField = BooleanFieldValue.of("name");

    assertTrue(trueLit.evaluate(adapter, entry));
    assertFalse(falseLit.evaluate(adapter, entry));
    assertTrue(stringLit.evaluate(adapter, entry));
    assertFalse(nullLit.evaluate(adapter, entry));
    assertTrue(numberTrueLit.evaluate(adapter, entry));
    assertFalse(numberFalseLit.evaluate(adapter, entry));
    assertTrue(booleanField.evaluate(adapter, entry));
    assertFalse(booleanField.evaluate(adapter, entryNulls));
    assertTrue(trueLit.and(stringLit).evaluate(adapter, entry));
    assertFalse(falseLit.and(trueLit).evaluate(adapter, entry));
    assertTrue(falseLit.or(trueLit).evaluate(adapter, entry));
    assertTrue(trueLit.isEqualTo(true).evaluate(adapter, entry));
    assertFalse(trueLit.isEqualTo(false).evaluate(adapter, entry));
    assertTrue(falseLit.isNotEqualTo(true).evaluate(adapter, entry));
    assertFalse(falseLit.isNotEqualTo(false).evaluate(adapter, entry));
    assertTrue(booleanStrField.evaluate(adapter, entry));
    assertFalse(booleanStrField.evaluate(adapter, entryNulls));
    assertFalse(booleanField.evaluate(adapter, entryFalse));
    assertTrue(booleanIntField.evaluate(adapter, entry));
    assertFalse(booleanIntField.evaluate(adapter, entryFalse));
    assertFalse(booleanIntField.evaluate(adapter, entryNulls));
  }

  @Test
  public void testFilters() {
    final DataTypeAdapter<TestType> adapter = new TestTypeBasicDataAdapter();
    final TestType entry = new TestType("test", 1.3, 5, true);
    final NumericFieldValue doubleField = NumericFieldValue.of("doubleField");
    final NumericFieldValue intField = NumericFieldValue.of("intField");
    final TextFieldValue textField = TextFieldValue.of("name");

    // Test And
    assertTrue(
        doubleField.isLessThan(2).and(textField.concat("oreo").contains("store")).evaluate(
            adapter,
            entry));
    assertFalse(
        intField.isGreaterThan(doubleField).and(intField.isGreaterThan(10)).evaluate(
            adapter,
            entry));
    assertFalse(doubleField.isEqualTo(intField).and(intField.isNotNull()).evaluate(adapter, entry));
    assertFalse(textField.contains("val").and(intField.isLessThan(0)).evaluate(adapter, entry));

    // Test Or
    assertTrue(
        doubleField.isLessThan(2).or(textField.concat("oreo").contains("store")).evaluate(
            adapter,
            entry));
    assertTrue(
        intField.isGreaterThan(doubleField).or(intField.isGreaterThan(10)).evaluate(
            adapter,
            entry));
    assertTrue(doubleField.isEqualTo(intField).or(intField.isNotNull()).evaluate(adapter, entry));
    assertFalse(textField.contains("val").or(intField.isLessThan(0)).evaluate(adapter, entry));

    // Test Not
    assertFalse(Filter.not(doubleField.isLessThan(2)).evaluate(adapter, entry));
    assertFalse(
        Filter.not(
            doubleField.isLessThan(2).and(textField.concat("oreo").contains("store"))).evaluate(
                adapter,
                entry));
    assertTrue(
        Filter.not(intField.isGreaterThan(doubleField).and(intField.isGreaterThan(10))).evaluate(
            adapter,
            entry));
    assertTrue(
        Filter.not(doubleField.isEqualTo(intField).and(intField.isNotNull())).evaluate(
            adapter,
            entry));
    assertTrue(
        Filter.not(textField.contains("val").and(intField.isLessThan(0))).evaluate(adapter, entry));

    // Test include/exclude
    assertTrue(Filter.include().evaluate(null, null));
    assertFalse(Filter.exclude().evaluate(null, null));

    // Test serialization
    byte[] bytes =
        PersistenceUtils.toBinary(textField.contains("test").and(intField.isLessThan(1L)));
    final And and = (And) PersistenceUtils.fromBinary(bytes);
    assertEquals(2, and.getChildren().length);
    assertTrue(and.getChildren()[0] instanceof Contains);
    assertTrue(((Contains) and.getChildren()[0]).getExpression1() instanceof TextFieldValue);
    assertEquals(
        "name",
        ((TextFieldValue) ((Contains) and.getChildren()[0]).getExpression1()).getFieldName());
    assertTrue(((Contains) and.getChildren()[0]).getExpression2() instanceof TextLiteral);
    assertEquals(
        "test",
        (String) ((TextLiteral) ((Contains) and.getChildren()[0]).getExpression2()).getValue());
    assertTrue(and.getChildren()[1] instanceof NumericComparisonOperator);
    assertEquals(
        CompareOp.LESS_THAN,
        ((NumericComparisonOperator) and.getChildren()[1]).getCompareOp());
    assertTrue(
        ((NumericComparisonOperator) and.getChildren()[1]).getExpression1() instanceof NumericFieldValue);
    assertEquals(
        "intField",
        ((NumericFieldValue) ((NumericComparisonOperator) and.getChildren()[1]).getExpression1()).getFieldName());
    assertTrue(
        ((NumericComparisonOperator) and.getChildren()[1]).getExpression2() instanceof NumericLiteral);
    assertEquals(
        1.0,
        (double) ((NumericLiteral) ((NumericComparisonOperator) and.getChildren()[1]).getExpression2()).getValue(),
        EPSILON);

    bytes = PersistenceUtils.toBinary(textField.contains("test").or(intField.isLessThan(1L)));
    final Or or = (Or) PersistenceUtils.fromBinary(bytes);
    assertEquals(2, or.getChildren().length);
    assertTrue(or.getChildren()[0] instanceof Contains);
    assertTrue(((Contains) or.getChildren()[0]).getExpression1() instanceof TextFieldValue);
    assertEquals(
        "name",
        ((TextFieldValue) ((Contains) or.getChildren()[0]).getExpression1()).getFieldName());
    assertTrue(((Contains) or.getChildren()[0]).getExpression2() instanceof TextLiteral);
    assertEquals(
        "test",
        (String) ((TextLiteral) ((Contains) or.getChildren()[0]).getExpression2()).getValue());
    assertTrue(or.getChildren()[1] instanceof NumericComparisonOperator);
    assertEquals(
        CompareOp.LESS_THAN,
        ((NumericComparisonOperator) or.getChildren()[1]).getCompareOp());
    assertTrue(
        ((NumericComparisonOperator) or.getChildren()[1]).getExpression1() instanceof NumericFieldValue);
    assertEquals(
        "intField",
        ((NumericFieldValue) ((NumericComparisonOperator) or.getChildren()[1]).getExpression1()).getFieldName());
    assertTrue(
        ((NumericComparisonOperator) or.getChildren()[1]).getExpression2() instanceof NumericLiteral);
    assertEquals(
        1.0,
        (double) ((NumericLiteral) ((NumericComparisonOperator) and.getChildren()[1]).getExpression2()).getValue(),
        EPSILON);

    bytes = PersistenceUtils.toBinary(Filter.include());
    assertTrue(PersistenceUtils.fromBinary(bytes) instanceof Include);

    bytes = PersistenceUtils.toBinary(Filter.exclude());
    assertTrue(PersistenceUtils.fromBinary(bytes) instanceof Exclude);

    bytes = PersistenceUtils.toBinary(Filter.not(textField.contains("test")));
    final Not not = (Not) PersistenceUtils.fromBinary(bytes);
    assertTrue(not.getFilter() instanceof Contains);
    assertTrue(((Contains) not.getFilter()).getExpression1() instanceof TextFieldValue);
    assertEquals(
        "name",
        ((TextFieldValue) ((Contains) not.getFilter()).getExpression1()).getFieldName());
    assertTrue(((Contains) not.getFilter()).getExpression2() instanceof TextLiteral);
    assertEquals(
        "test",
        (String) ((TextLiteral) ((Contains) not.getFilter()).getExpression2()).getValue());
  }

  @Test
  public void testInvalidComparisons() throws URISyntaxException {
    final TextLiteral textLit = TextLiteral.of("text");
    final NumericLiteral doubleLit = NumericLiteral.of(0.5);
    final NumericLiteral integerLit = NumericLiteral.of(1);
    final GenericLiteral dateLit = GenericLiteral.of(new Date(100));
    final GenericLiteral dateLit2 = GenericLiteral.of(new Date(500));
    final GenericLiteral uriLit = GenericLiteral.of(new URI("test"));
    final GenericLiteral nonComparable = GenericLiteral.of(Lists.newArrayList());

    try {
      doubleLit.isGreaterThan(textLit).evaluate(null, null);
      fail();
    } catch (RuntimeException e) {
      // Expected
    }
    try {
      textLit.isGreaterThan(doubleLit).evaluate(null, null);
    } catch (RuntimeException e) {
      // Expected
    }
    try {
      textLit.isLessThan(dateLit).evaluate(null, null);
    } catch (RuntimeException e) {
      // Expected
    }
    try {
      doubleLit.isBetween("test", 1).evaluate(null, null);
      fail();
    } catch (RuntimeException e) {
      // Expected
    }
    try {
      doubleLit.isBetween(0, "test").evaluate(null, null);
      fail();
    } catch (RuntimeException e) {
      // Expected
    }
    try {
      integerLit.isBetween("test", "test2").evaluate(null, null);
      fail();
    } catch (RuntimeException e) {
      // Expected
    }
    try {
      doubleLit.isBetween(dateLit2, uriLit).evaluate(null, null);
      fail();
    } catch (RuntimeException e) {
      // Expected
    }
  }

}

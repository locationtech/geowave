/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data.visibility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.text.ParseException;
import org.junit.Test;
import com.google.common.collect.Sets;

public class VisibilityExpressionTest {
  @Test
  public void testValidVisibilityExpressions() {
    // Basic expression
    final String EXPRESSION1 = "(a&b)|c";

    assertTrue(VisibilityExpression.evaluate(EXPRESSION1, Sets.newHashSet("a", "b")));
    assertTrue(VisibilityExpression.evaluate(EXPRESSION1, Sets.newHashSet("a", "b", "c")));
    assertTrue(VisibilityExpression.evaluate(EXPRESSION1, Sets.newHashSet("c")));
    assertFalse(VisibilityExpression.evaluate(EXPRESSION1, Sets.newHashSet("a")));
    assertFalse(VisibilityExpression.evaluate(EXPRESSION1, Sets.newHashSet("b")));
    assertFalse(VisibilityExpression.evaluate(EXPRESSION1, Sets.newHashSet("d")));
    assertFalse(VisibilityExpression.evaluate(EXPRESSION1, Sets.newHashSet()));

    // More complex expression with white space
    final String EXPRESSION2 = "((a & b) | c) & (d | e)";
    assertTrue(VisibilityExpression.evaluate(EXPRESSION2, Sets.newHashSet("a", "b", "d")));
    assertTrue(VisibilityExpression.evaluate(EXPRESSION2, Sets.newHashSet("a", "b", "e")));
    assertTrue(VisibilityExpression.evaluate(EXPRESSION2, Sets.newHashSet("c", "d")));
    assertTrue(VisibilityExpression.evaluate(EXPRESSION2, Sets.newHashSet("c", "e")));
    assertTrue(VisibilityExpression.evaluate(EXPRESSION2, Sets.newHashSet("a", "c", "d")));
    assertTrue(VisibilityExpression.evaluate(EXPRESSION2, Sets.newHashSet("b", "c", "e")));
    assertTrue(
        VisibilityExpression.evaluate(EXPRESSION2, Sets.newHashSet("a", "b", "c", "d", "e")));
    assertFalse(VisibilityExpression.evaluate(EXPRESSION2, Sets.newHashSet("a")));
    assertFalse(VisibilityExpression.evaluate(EXPRESSION2, Sets.newHashSet("b")));
    assertFalse(VisibilityExpression.evaluate(EXPRESSION2, Sets.newHashSet("c")));
    assertFalse(VisibilityExpression.evaluate(EXPRESSION2, Sets.newHashSet("d")));
    assertFalse(VisibilityExpression.evaluate(EXPRESSION2, Sets.newHashSet("e")));
    assertFalse(VisibilityExpression.evaluate(EXPRESSION2, Sets.newHashSet("a", "b")));
    assertFalse(VisibilityExpression.evaluate(EXPRESSION2, Sets.newHashSet("a", "d")));
    assertFalse(VisibilityExpression.evaluate(EXPRESSION2, Sets.newHashSet("a", "e")));
    assertFalse(VisibilityExpression.evaluate(EXPRESSION2, Sets.newHashSet("a", "b", "c")));
    assertFalse(VisibilityExpression.evaluate(EXPRESSION2, Sets.newHashSet()));

    // Chained operators
    final String EXPRESSION3 = "(a&b&c)|d|e";
    assertTrue(VisibilityExpression.evaluate(EXPRESSION3, Sets.newHashSet("a", "b", "c")));
    assertTrue(VisibilityExpression.evaluate(EXPRESSION3, Sets.newHashSet("a", "b", "e")));
    assertTrue(VisibilityExpression.evaluate(EXPRESSION3, Sets.newHashSet("c", "d")));
    assertTrue(VisibilityExpression.evaluate(EXPRESSION3, Sets.newHashSet("c", "e")));
    assertTrue(VisibilityExpression.evaluate(EXPRESSION3, Sets.newHashSet("a", "c", "d")));
    assertTrue(VisibilityExpression.evaluate(EXPRESSION3, Sets.newHashSet("d")));
    assertTrue(VisibilityExpression.evaluate(EXPRESSION3, Sets.newHashSet("e")));
    assertTrue(
        VisibilityExpression.evaluate(EXPRESSION3, Sets.newHashSet("a", "b", "c", "d", "e")));
    assertFalse(VisibilityExpression.evaluate(EXPRESSION3, Sets.newHashSet("a")));
    assertFalse(VisibilityExpression.evaluate(EXPRESSION3, Sets.newHashSet("b")));
    assertFalse(VisibilityExpression.evaluate(EXPRESSION3, Sets.newHashSet("c")));
    assertFalse(VisibilityExpression.evaluate(EXPRESSION3, Sets.newHashSet("a", "b")));
    assertFalse(VisibilityExpression.evaluate(EXPRESSION3, Sets.newHashSet("a", "c")));
    assertFalse(VisibilityExpression.evaluate(EXPRESSION3, Sets.newHashSet()));

    // Empty expression
    final String EMPTY_EXPRESSION = "";

    assertTrue(VisibilityExpression.evaluate(EMPTY_EXPRESSION, Sets.newHashSet("a", "b")));
    assertTrue(VisibilityExpression.evaluate(EMPTY_EXPRESSION, Sets.newHashSet("a", "b", "c")));
    assertTrue(VisibilityExpression.evaluate(EMPTY_EXPRESSION, Sets.newHashSet("c")));
    assertTrue(VisibilityExpression.evaluate(EMPTY_EXPRESSION, Sets.newHashSet("a")));
    assertTrue(VisibilityExpression.evaluate(EMPTY_EXPRESSION, Sets.newHashSet("b")));
    assertTrue(VisibilityExpression.evaluate(EMPTY_EXPRESSION, Sets.newHashSet("d")));
    assertTrue(VisibilityExpression.evaluate(EMPTY_EXPRESSION, Sets.newHashSet()));
  }

  @Test
  public void testInvalidVisibilityExpressions() {
    // No matching right paren
    final String EXPRESSION1 = "(a&b|c";
    // No matching left paren
    final String EXPRESSION2 = "a&b)|c";
    // Multiple sequential oeprators
    final String EXPRESSION3 = "a&|b";
    // Multiple sequential operands
    final String EXPRESSION4 = "(a)(b)";
    // No left operand
    final String EXPRESSION5 = "&b";
    // No right operand
    final String EXPRESSION6 = "a&";

    try {
      VisibilityExpression.evaluate(EXPRESSION1, Sets.newHashSet("a"));
      fail();
    } catch (final Exception e) {
      // Expected
      assertTrue(e.getCause() instanceof ParseException);
      assertEquals(
          "Left parenthesis found with no matching right parenthesis.",
          e.getCause().getMessage());
    }

    try {
      VisibilityExpression.evaluate(EXPRESSION2, Sets.newHashSet("a"));
      fail();
    } catch (final Exception e) {
      // Expected
      assertTrue(e.getCause() instanceof ParseException);
      assertEquals(
          "Right parenthesis found with no matching left parenthesis.",
          e.getCause().getMessage());
    }

    try {
      VisibilityExpression.evaluate(EXPRESSION3, Sets.newHashSet("a"));
      fail();
    } catch (final Exception e) {
      // Expected
      assertTrue(e.getCause() instanceof ParseException);
      assertEquals("Multiple sequential operators.", e.getCause().getMessage());
    }

    try {
      VisibilityExpression.evaluate(EXPRESSION4, Sets.newHashSet("a"));
      fail();
    } catch (final Exception e) {
      // Expected
      assertTrue(e.getCause() instanceof ParseException);
      assertEquals("Multiple sequential operands with no operator.", e.getCause().getMessage());
    }

    try {
      VisibilityExpression.evaluate(EXPRESSION5, Sets.newHashSet("a"));
      fail();
    } catch (final Exception e) {
      // Expected
      assertTrue(e.getCause() instanceof ParseException);
      assertEquals("Operator found with no left operand.", e.getCause().getMessage());
    }

    try {
      VisibilityExpression.evaluate(EXPRESSION6, Sets.newHashSet("a"));
      fail();
    } catch (final Exception e) {
      // Expected
      assertTrue(e.getCause() instanceof ParseException);
      assertEquals("Operator found with no right operand.", e.getCause().getMessage());
    }
  }

  @Test
  public void testVisibiltyComposer() {
    VisibilityComposer composer = new VisibilityComposer();
    composer.addVisibility("a&b");
    assertEquals("a&b", composer.composeVisibility());

    // Adding "a" or "b" to the visibility shouldn't change it
    composer.addVisibility("a");
    assertEquals("a&b", composer.composeVisibility());

    composer.addVisibility("b");
    assertEquals("a&b", composer.composeVisibility());

    composer.addVisibility("a&b");
    assertEquals("a&b", composer.composeVisibility());

    // Adding "c" should update it
    composer.addVisibility("c");
    assertEquals("a&b&c", composer.composeVisibility());

    // Adding a complex visibility should duplicate any
    composer.addVisibility("(a&b)&(c&d)");
    assertEquals("a&b&c&d", composer.composeVisibility());

    // Any expression with an OR operator should be isolated
    composer.addVisibility("a&(e|(f&b))");
    assertEquals("(e|(f&b))&a&b&c&d", composer.composeVisibility());

    composer = new VisibilityComposer();

    // Adding a complex visibility that only uses AND operators should simplify the expression
    composer.addVisibility("a&((b&e)&(c&d))");
    assertEquals("a&b&c&d&e", composer.composeVisibility());

    composer = new VisibilityComposer();
    composer.addVisibility("a&b");
    assertEquals("a&b", composer.composeVisibility());

    final VisibilityComposer copy = new VisibilityComposer(composer);
    assertEquals("a&b", copy.composeVisibility());

    // Adding to the copy does not affect the original
    copy.addVisibility("c&d");
    assertEquals("a&b&c&d", copy.composeVisibility());
    assertEquals("a&b", composer.composeVisibility());

  }
}

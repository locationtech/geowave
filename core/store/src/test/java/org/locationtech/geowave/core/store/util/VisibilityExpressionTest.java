/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.core.store.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.text.ParseException;

import org.junit.Test;
import org.locationtech.geowave.core.store.util.VisibilityExpression;

import com.google.common.collect.Sets;

public class VisibilityExpressionTest
{
	@Test
	public void testValidVisibilityExpressions() {
		// Basic expression
		final String EXPRESSION1 = "(a&b)|c";

		assertTrue(VisibilityExpression.evaluate(
				EXPRESSION1,
				Sets.newHashSet(
						"a",
						"b")));
		assertTrue(VisibilityExpression.evaluate(
				EXPRESSION1,
				Sets.newHashSet(
						"a",
						"b",
						"c")));
		assertTrue(VisibilityExpression.evaluate(
				EXPRESSION1,
				Sets.newHashSet("c")));
		assertFalse(VisibilityExpression.evaluate(
				EXPRESSION1,
				Sets.newHashSet("a")));
		assertFalse(VisibilityExpression.evaluate(
				EXPRESSION1,
				Sets.newHashSet("b")));
		assertFalse(VisibilityExpression.evaluate(
				EXPRESSION1,
				Sets.newHashSet("d")));
		assertFalse(VisibilityExpression.evaluate(
				EXPRESSION1,
				Sets.newHashSet()));

		// More complex expression with white space
		final String EXPRESSION2 = "((a & b) | c) & (d | e)";
		assertTrue(VisibilityExpression.evaluate(
				EXPRESSION2,
				Sets.newHashSet(
						"a",
						"b",
						"d")));
		assertTrue(VisibilityExpression.evaluate(
				EXPRESSION2,
				Sets.newHashSet(
						"a",
						"b",
						"e")));
		assertTrue(VisibilityExpression.evaluate(
				EXPRESSION2,
				Sets.newHashSet(
						"c",
						"d")));
		assertTrue(VisibilityExpression.evaluate(
				EXPRESSION2,
				Sets.newHashSet(
						"c",
						"e")));
		assertTrue(VisibilityExpression.evaluate(
				EXPRESSION2,
				Sets.newHashSet(
						"a",
						"c",
						"d")));
		assertTrue(VisibilityExpression.evaluate(
				EXPRESSION2,
				Sets.newHashSet(
						"b",
						"c",
						"e")));
		assertTrue(VisibilityExpression.evaluate(
				EXPRESSION2,
				Sets.newHashSet(
						"a",
						"b",
						"c",
						"d",
						"e")));
		assertFalse(VisibilityExpression.evaluate(
				EXPRESSION2,
				Sets.newHashSet("a")));
		assertFalse(VisibilityExpression.evaluate(
				EXPRESSION2,
				Sets.newHashSet("b")));
		assertFalse(VisibilityExpression.evaluate(
				EXPRESSION2,
				Sets.newHashSet("c")));
		assertFalse(VisibilityExpression.evaluate(
				EXPRESSION2,
				Sets.newHashSet("d")));
		assertFalse(VisibilityExpression.evaluate(
				EXPRESSION2,
				Sets.newHashSet("e")));
		assertFalse(VisibilityExpression.evaluate(
				EXPRESSION2,
				Sets.newHashSet(
						"a",
						"b")));
		assertFalse(VisibilityExpression.evaluate(
				EXPRESSION2,
				Sets.newHashSet(
						"a",
						"d")));
		assertFalse(VisibilityExpression.evaluate(
				EXPRESSION2,
				Sets.newHashSet(
						"a",
						"e")));
		assertFalse(VisibilityExpression.evaluate(
				EXPRESSION2,
				Sets.newHashSet(
						"a",
						"b",
						"c")));
		assertFalse(VisibilityExpression.evaluate(
				EXPRESSION2,
				Sets.newHashSet()));

		// Chained operators
		final String EXPRESSION3 = "(a&b&c)|d|e";
		assertTrue(VisibilityExpression.evaluate(
				EXPRESSION3,
				Sets.newHashSet(
						"a",
						"b",
						"c")));
		assertTrue(VisibilityExpression.evaluate(
				EXPRESSION3,
				Sets.newHashSet(
						"a",
						"b",
						"e")));
		assertTrue(VisibilityExpression.evaluate(
				EXPRESSION3,
				Sets.newHashSet(
						"c",
						"d")));
		assertTrue(VisibilityExpression.evaluate(
				EXPRESSION3,
				Sets.newHashSet(
						"c",
						"e")));
		assertTrue(VisibilityExpression.evaluate(
				EXPRESSION3,
				Sets.newHashSet(
						"a",
						"c",
						"d")));
		assertTrue(VisibilityExpression.evaluate(
				EXPRESSION3,
				Sets.newHashSet("d")));
		assertTrue(VisibilityExpression.evaluate(
				EXPRESSION3,
				Sets.newHashSet("e")));
		assertTrue(VisibilityExpression.evaluate(
				EXPRESSION3,
				Sets.newHashSet(
						"a",
						"b",
						"c",
						"d",
						"e")));
		assertFalse(VisibilityExpression.evaluate(
				EXPRESSION3,
				Sets.newHashSet("a")));
		assertFalse(VisibilityExpression.evaluate(
				EXPRESSION3,
				Sets.newHashSet("b")));
		assertFalse(VisibilityExpression.evaluate(
				EXPRESSION3,
				Sets.newHashSet("c")));
		assertFalse(VisibilityExpression.evaluate(
				EXPRESSION3,
				Sets.newHashSet(
						"a",
						"b")));
		assertFalse(VisibilityExpression.evaluate(
				EXPRESSION3,
				Sets.newHashSet(
						"a",
						"c")));
		assertFalse(VisibilityExpression.evaluate(
				EXPRESSION3,
				Sets.newHashSet()));

		// Empty expression
		final String EMPTY_EXPRESSION = "";

		assertTrue(VisibilityExpression.evaluate(
				EMPTY_EXPRESSION,
				Sets.newHashSet(
						"a",
						"b")));
		assertTrue(VisibilityExpression.evaluate(
				EMPTY_EXPRESSION,
				Sets.newHashSet(
						"a",
						"b",
						"c")));
		assertTrue(VisibilityExpression.evaluate(
				EMPTY_EXPRESSION,
				Sets.newHashSet("c")));
		assertTrue(VisibilityExpression.evaluate(
				EMPTY_EXPRESSION,
				Sets.newHashSet("a")));
		assertTrue(VisibilityExpression.evaluate(
				EMPTY_EXPRESSION,
				Sets.newHashSet("b")));
		assertTrue(VisibilityExpression.evaluate(
				EMPTY_EXPRESSION,
				Sets.newHashSet("d")));
		assertTrue(VisibilityExpression.evaluate(
				EMPTY_EXPRESSION,
				Sets.newHashSet()));

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
			VisibilityExpression.evaluate(
					EXPRESSION1,
					Sets.newHashSet("a"));
			fail();
		}
		catch (Exception e) {
			// Expected
			assertTrue(e.getCause() instanceof ParseException);
			assertEquals(
					"Left parenthesis found with no matching right parenthesis.",
					e.getCause().getMessage());
		}

		try {
			VisibilityExpression.evaluate(
					EXPRESSION2,
					Sets.newHashSet("a"));
			fail();
		}
		catch (Exception e) {
			// Expected
			assertTrue(e.getCause() instanceof ParseException);
			assertEquals(
					"Right parenthesis found with no matching left parenthesis.",
					e.getCause().getMessage());
		}

		try {
			VisibilityExpression.evaluate(
					EXPRESSION3,
					Sets.newHashSet("a"));
			fail();
		}
		catch (Exception e) {
			// Expected
			assertTrue(e.getCause() instanceof ParseException);
			assertEquals(
					"Multiple sequential operators.",
					e.getCause().getMessage());
		}

		try {
			VisibilityExpression.evaluate(
					EXPRESSION4,
					Sets.newHashSet("a"));
			fail();
		}
		catch (Exception e) {
			// Expected
			assertTrue(e.getCause() instanceof ParseException);
			assertEquals(
					"Multiple sequential operands with no operator.",
					e.getCause().getMessage());
		}

		try {
			VisibilityExpression.evaluate(
					EXPRESSION5,
					Sets.newHashSet("a"));
			fail();
		}
		catch (Exception e) {
			// Expected
			assertTrue(e.getCause() instanceof ParseException);
			assertEquals(
					"Operator found with no left operand.",
					e.getCause().getMessage());
		}

		try {
			VisibilityExpression.evaluate(
					EXPRESSION6,
					Sets.newHashSet("a"));
			fail();
		}
		catch (Exception e) {
			// Expected
			assertTrue(e.getCause() instanceof ParseException);
			assertEquals(
					"Operator found with no right operand.",
					e.getCause().getMessage());
		}
	}

}

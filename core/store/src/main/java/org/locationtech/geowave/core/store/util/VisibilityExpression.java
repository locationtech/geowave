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

import java.text.ParseException;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class VisibilityExpression
{
	// Split before and after the delimiter character so that it gets
	// included in the token list
	private static final String SPLIT_DELIMITER = "((?<=%1$s)|(?=%1$s))";
	private static final String TOKEN_SPLIT;
	static {
		StringBuilder sb = new StringBuilder();
		sb.append(
				String.format(
						SPLIT_DELIMITER,
						"\\(")).append(
				"|");
		sb.append(
				String.format(
						SPLIT_DELIMITER,
						"\\)")).append(
				"|");
		sb.append(
				String.format(
						SPLIT_DELIMITER,
						"\\&")).append(
				"|");
		sb.append(String.format(
				SPLIT_DELIMITER,
				"\\|"));
		TOKEN_SPLIT = sb.toString();
	}

	private static LoadingCache<String, VisibilityNode> expressionCache = CacheBuilder.newBuilder().maximumSize(
			50).build(
			new VisibilityCacheLoader());

	public static boolean evaluate(
			String expression,
			Set<String> auths ) {
		String trimmed = expression.replaceAll(
				"\\s+",
				"");
		try {
			return expressionCache.get(
					trimmed).evalutate(
					auths);
		}
		catch (ExecutionException e) {
			throw new RuntimeException(
					e.getCause());
		}
	}

	private static class VisibilityCacheLoader extends
			CacheLoader<String, VisibilityNode>
	{

		@Override
		public VisibilityNode load(
				String key )
				throws Exception {
			String[] tokens = key.split(TOKEN_SPLIT);
			if (tokens.length == 0 || (tokens.length == 1 && tokens[0].length() == 0)) {
				return new NoAuthNode();
			}
			return parseTokens(
					0,
					tokens.length - 1,
					tokens);
		}

	}

	private static VisibilityNode parseTokens(
			int startIndex,
			int endIndex,
			String[] tokens )
			throws ParseException {
		VisibilityNode left = null;
		String operator = null;
		for (int i = startIndex; i <= endIndex; i++) {
			VisibilityNode newNode = null;
			if (tokens[i].equals("(")) {
				int matchingParen = findMatchingParen(
						i,
						tokens);
				if (matchingParen < 0) {
					throw new ParseException(
							"Left parenthesis found with no matching right parenthesis.",
							i);
				}
				newNode = parseTokens(
						i + 1,
						matchingParen - 1,
						tokens);
				i = matchingParen;
			}
			else if (tokens[i].equals(")")) {
				throw new ParseException(
						"Right parenthesis found with no matching left parenthesis.",
						i);
			}
			else if ("&|".indexOf(tokens[i]) > -1) {
				if (left == null) {
					throw new ParseException(
							"Operator found with no left operand.",
							i);
				}
				else if (operator != null) {
					throw new ParseException(
							"Multiple sequential operators.",
							i);
				}
				else {
					operator = tokens[i];
				}
			}
			else {
				newNode = new ValueNode(
						tokens[i]);
			}
			if (newNode != null) {
				if (left == null) {
					left = newNode;
				}
				else if (operator == null) {
					throw new ParseException(
							"Multiple sequential operands with no operator.",
							i);
				}
				else if (operator.equals("&")) {
					left = new AndNode(
							left,
							newNode);
					operator = null;
				}
				else {
					left = new OrNode(
							left,
							newNode);
					operator = null;
				}
			}
		}
		if (left == null) {
			return new NoAuthNode();
		}
		else if (operator != null) {
			throw new ParseException(
					"Operator found with no right operand.",
					endIndex);
		}
		return left;
	}

	private static int findMatchingParen(
			int start,
			String[] tokens ) {
		int match = -1;
		int parenDepth = 1;
		for (int i = start + 1; i < tokens.length; i++) {
			if (tokens[i].equals(")")) {
				parenDepth--;
				if (parenDepth == 0) {
					match = i;
					break;
				}
			}
			else if (tokens[i].equals("(")) {
				parenDepth++;
			}
		}
		return match;
	}

	private abstract static class VisibilityNode
	{
		public abstract boolean evalutate(
				Set<String> auths );
	}

	private static class NoAuthNode extends
			VisibilityNode
	{

		@Override
		public boolean evalutate(
				Set<String> auths ) {
			return true;
		}

	}

	private static class ValueNode extends
			VisibilityNode
	{
		private final String value;

		public ValueNode(
				String value ) {
			this.value = value;
		}

		@Override
		public boolean evalutate(
				Set<String> auths ) {
			return auths.contains(value);
		}

	}

	private static class AndNode extends
			VisibilityNode
	{
		private final VisibilityNode left;
		private final VisibilityNode right;

		public AndNode(
				VisibilityNode left,
				VisibilityNode right ) {
			this.left = left;
			this.right = right;
		}

		@Override
		public boolean evalutate(
				Set<String> auths ) {
			return left.evalutate(auths) && right.evalutate(auths);
		}
	}

	private static class OrNode extends
			VisibilityNode
	{
		private final VisibilityNode left;
		private final VisibilityNode right;

		public OrNode(
				VisibilityNode left,
				VisibilityNode right ) {
			this.left = left;
			this.right = right;
		}

		@Override
		public boolean evalutate(
				Set<String> auths ) {
			return left.evalutate(auths) || right.evalutate(auths);
		}
	}
}

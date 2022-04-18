/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data.visibility;

import java.text.ParseException;
import java.util.Set;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class VisibilityExpression {
  public static final String OR_TOKEN = "|";
  public static final String AND_TOKEN = "&";
  // Split before and after the delimiter character so that it gets
  // included in the token list
  private static final String SPLIT_DELIMITER = "((?<=%1$s)|(?=%1$s))";
  private static final String TOKEN_SPLIT;

  static {
    final StringBuilder sb = new StringBuilder();
    sb.append(String.format(SPLIT_DELIMITER, "\\(")).append("|");
    sb.append(String.format(SPLIT_DELIMITER, "\\)")).append("|");
    sb.append(String.format(SPLIT_DELIMITER, "\\" + AND_TOKEN)).append("|");
    sb.append(String.format(SPLIT_DELIMITER, "\\" + OR_TOKEN));
    TOKEN_SPLIT = sb.toString();
  }

  private static LoadingCache<String, VisibilityNode> expressionCache =
      Caffeine.newBuilder().maximumSize(50).build(new VisibilityCacheLoader());

  private static class VisibilityCacheLoader implements CacheLoader<String, VisibilityNode> {
    @Override
    public VisibilityNode load(final String key) throws Exception {
      final String[] tokens = key.split(TOKEN_SPLIT);
      if ((tokens.length == 0) || ((tokens.length == 1) && (tokens[0].length() == 0))) {
        return new NoAuthNode();
      }
      return parseTokens(0, tokens.length - 1, tokens);
    }
  }

  private static VisibilityNode getCached(final String expression) {
    final String trimmed = expression.replaceAll("\\s+", "");
    return expressionCache.get(trimmed);
  }

  public static boolean evaluate(final String expression, final Set<String> auths) {
    if (expression.isEmpty()) {
      return true;
    }
    return getCached(expression).evaluate(auths);
  }

  public static void addMinimalTokens(final String expression, final Set<String> tokens) {
    addMinimalTokens(getCached(expression), tokens);
  }

  private static void addMinimalTokens(final VisibilityNode parsed, final Set<String> tokens) {
    if (parsed instanceof ValueNode) {
      tokens.add(((ValueNode) parsed).toString());
    } else if (parsed instanceof AndNode) {
      addMinimalTokens(((AndNode) parsed).getLeft(), tokens);
      addMinimalTokens(((AndNode) parsed).getRight(), tokens);
    } else if (parsed instanceof OrNode) {
      tokens.add("(" + parsed.toString() + ")");
    }
  }

  private static VisibilityNode parseTokens(
      final int startIndex,
      final int endIndex,
      final String[] tokens) throws ParseException {
    VisibilityNode left = null;
    String operator = null;
    for (int i = startIndex; i <= endIndex; i++) {
      VisibilityNode newNode = null;
      if (tokens[i].equals("(")) {
        final int matchingParen = findMatchingParen(i, tokens);
        if (matchingParen < 0) {
          throw new ParseException("Left parenthesis found with no matching right parenthesis.", i);
        }
        newNode = parseTokens(i + 1, matchingParen - 1, tokens);
        i = matchingParen;
      } else if (tokens[i].equals(")")) {
        throw new ParseException("Right parenthesis found with no matching left parenthesis.", i);
      } else if (AND_TOKEN.equals(tokens[i]) || OR_TOKEN.equals(tokens[i])) {
        if (left == null) {
          throw new ParseException("Operator found with no left operand.", i);
        } else if (operator != null) {
          throw new ParseException("Multiple sequential operators.", i);
        } else {
          operator = tokens[i];
        }
      } else {
        newNode = new ValueNode(tokens[i]);
      }
      if (newNode != null) {
        if (left == null) {
          left = newNode;
        } else if (operator == null) {
          throw new ParseException("Multiple sequential operands with no operator.", i);
        } else if (operator.equals(AND_TOKEN)) {
          left = new AndNode(left, newNode);
          operator = null;
        } else {
          left = new OrNode(left, newNode);
          operator = null;
        }
      }
    }
    if (left == null) {
      return new NoAuthNode();
    } else if (operator != null) {
      throw new ParseException("Operator found with no right operand.", endIndex);
    }
    return left;
  }

  private static int findMatchingParen(final int start, final String[] tokens) {
    int match = -1;
    int parenDepth = 1;
    for (int i = start + 1; i < tokens.length; i++) {
      if (tokens[i].equals(")")) {
        parenDepth--;
        if (parenDepth == 0) {
          match = i;
          break;
        }
      } else if (tokens[i].equals("(")) {
        parenDepth++;
      }
    }
    return match;
  }

  public abstract static class VisibilityNode {
    public abstract boolean evaluate(Set<String> auths);
  }

  public abstract static class OperatorNode extends VisibilityNode {
    public abstract VisibilityNode getLeft();

    public abstract VisibilityNode getRight();

    @Override
    public String toString() {
      return getExpression();
    }

    protected abstract String getOperator();

    public String getExpression() {
      final StringBuilder sb = new StringBuilder();
      return buildExpression(sb);
    }

    protected String buildExpression(final StringBuilder sb) {
      return buildExpression(sb, getOperator());
    }

    protected String buildExpression(final StringBuilder sb, final String operator) {
      if (getLeft() instanceof OperatorNode) {
        sb.append("(");
        ((OperatorNode) getLeft()).buildExpression(sb);
        sb.append(")");
      } else {
        sb.append(getLeft().toString());
      }
      sb.append(operator);
      if (getRight() instanceof OperatorNode) {
        sb.append("(");
        ((OperatorNode) getRight()).buildExpression(sb);
        sb.append(")");
      } else {
        sb.append(getRight().toString());
      }
      return sb.toString();
    }
  }

  public static class NoAuthNode extends VisibilityNode {

    @Override
    public boolean evaluate(final Set<String> auths) {
      return true;
    }

    @Override
    public String toString() {
      return "";
    }
  }

  public static class ValueNode extends VisibilityNode {
    private final String value;

    public ValueNode(final String value) {
      this.value = value;
    }

    @Override
    public boolean evaluate(final Set<String> auths) {
      return auths.contains(value);
    }

    @Override
    public String toString() {
      return value;
    }
  }

  public static class AndNode extends OperatorNode {
    private final VisibilityNode left;
    private final VisibilityNode right;

    public AndNode(final VisibilityNode left, final VisibilityNode right) {
      this.left = left;
      this.right = right;
    }

    @Override
    public boolean evaluate(final Set<String> auths) {
      return left.evaluate(auths) && right.evaluate(auths);
    }

    @Override
    public VisibilityNode getLeft() {
      return left;
    }

    @Override
    public VisibilityNode getRight() {
      return right;
    }

    @Override
    protected String getOperator() {
      return AND_TOKEN;
    }
  }

  public static class OrNode extends OperatorNode {
    private final VisibilityNode left;
    private final VisibilityNode right;

    public OrNode(final VisibilityNode left, final VisibilityNode right) {
      this.left = left;
      this.right = right;
    }

    @Override
    public boolean evaluate(final Set<String> auths) {
      return left.evaluate(auths) || right.evaluate(auths);
    }

    @Override
    public VisibilityNode getLeft() {
      return left;
    }

    @Override
    public VisibilityNode getRight() {
      return right;
    }

    @Override
    protected String getOperator() {
      return OR_TOKEN;
    }
  }
}

/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression.text;

import java.util.Set;
import com.google.common.collect.Sets;

/**
 * Predicate that passes when the first operand contains the text of the second operand.
 */
public class Contains extends TextBinaryPredicate {

  public Contains() {}

  public Contains(final TextExpression expression1, final TextExpression expression2) {
    super(expression1, expression2);
  }

  public Contains(
      final TextExpression expression1,
      final TextExpression expression2,
      final boolean ignoreCase) {
    super(expression1, expression2, ignoreCase);
  }

  @Override
  public boolean evaluateInternal(final String value1, final String value2) {
    return value1.contains(value2);
  }

  @Override
  public Set<String> getConstrainableFields() {
    return Sets.newHashSet();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(expression1.toString());
    sb.append(" CONTAINS ");
    sb.append(expression2.toString());
    return sb.toString();
  }

}

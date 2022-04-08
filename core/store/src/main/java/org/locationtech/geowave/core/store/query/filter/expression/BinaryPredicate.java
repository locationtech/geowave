/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression;

import java.util.List;
import java.util.Set;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import com.beust.jcommander.internal.Sets;

/**
 * An abstract predicate for comparing two expressions of the same type.
 *
 * @param <E> the expression class
 */
public abstract class BinaryPredicate<E extends Expression<?>> implements Predicate {

  protected E expression1;
  protected E expression2;

  public BinaryPredicate() {}

  public BinaryPredicate(final E expr1, final E expr2) {
    expression1 = expr1;
    expression2 = expr2;
  }

  public E getExpression1() {
    return expression1;
  }

  public E getExpression2() {
    return expression2;
  }


  @Override
  public Filter removePredicatesForFields(Set<String> fields) {
    final Set<String> referencedFields = Sets.newHashSet();
    expression1.addReferencedFields(referencedFields);
    expression2.addReferencedFields(referencedFields);
    if (fields.containsAll(referencedFields)) {
      return null;
    }
    return this;
  }

  @Override
  public void addReferencedFields(final Set<String> fields) {
    expression1.addReferencedFields(fields);
    expression2.addReferencedFields(fields);
  }

  @Override
  public byte[] toBinary() {
    return PersistenceUtils.toBinary(new Persistable[] {expression1, expression2});
  }

  @SuppressWarnings("unchecked")
  @Override
  public void fromBinary(final byte[] bytes) {
    final List<Persistable> expressions = PersistenceUtils.fromBinaryAsList(bytes);
    expression1 = (E) expressions.get(0);
    expression2 = (E) expressions.get(1);
  }

}

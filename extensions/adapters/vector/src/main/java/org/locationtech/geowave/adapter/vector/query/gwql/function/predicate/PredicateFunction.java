/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query.gwql.function.predicate;

import java.util.List;
import org.locationtech.geowave.adapter.vector.query.gwql.QLFunction;
import org.locationtech.geowave.core.store.query.filter.expression.Expression;
import org.locationtech.geowave.core.store.query.filter.expression.Predicate;

public interface PredicateFunction extends QLFunction<Boolean> {

  @Override
  default Class<Boolean> getReturnType() {
    return Boolean.class;
  }

  Predicate create(List<Expression<?>> arguments);

}

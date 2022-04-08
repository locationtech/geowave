/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.gwql.function.aggregation;

import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.query.gwql.QLFunction;

public interface AggregationFunction<R> extends QLFunction<R> {
  /**
   * Gets the {@link Aggregation} associated with this function.
   * 
   * @param adapter the adapter to perform the aggregation on
   * @param functionArgs the function arguments
   * @return the raw aggregation for this function
   */
  public <T> Aggregation<?, R, T> getAggregation(
      final DataTypeAdapter<T> adapter,
      final String[] functionArgs);
}

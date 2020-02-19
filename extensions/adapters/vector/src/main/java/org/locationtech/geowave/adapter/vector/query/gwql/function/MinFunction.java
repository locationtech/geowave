/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query.gwql.function;

import org.locationtech.geowave.adapter.vector.query.aggregation.VectorMinAggregation;
import org.locationtech.geowave.core.geotime.store.query.aggregate.FieldNameParam;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.opengis.feature.simple.SimpleFeature;

/**
 * Aggregation function that finds the minimum value of a given numeric column.
 */
public class MinFunction extends MathAggregationFunction {

  @Override
  protected String functionName() {
    return "MIN";
  }

  @Override
  protected Aggregation<?, ?, SimpleFeature> aggregation(final FieldNameParam columnName) {
    return new VectorMinAggregation(columnName);
  }

}

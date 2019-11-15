/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.query.gwql.function;

import org.locationtech.geowave.core.store.api.Aggregation;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * Interface for functions that perform aggregations on simple feature data.
 */
public interface QLVectorAggregationFunction extends QLFunction {
  /**
   * Gets the {@class Aggregation} associated with this function.
   * 
   * @param featureType the feature type of the features
   * @param functionArgs the function arguments
   * @return the raw aggregation for this function
   */
  public Aggregation<?, ?, SimpleFeature> getAggregation(
      final SimpleFeatureType featureType,
      final String[] functionArgs);
}

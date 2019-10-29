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

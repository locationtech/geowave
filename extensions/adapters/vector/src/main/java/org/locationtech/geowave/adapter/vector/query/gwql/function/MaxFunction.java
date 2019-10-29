package org.locationtech.geowave.adapter.vector.query.gwql.function;

import org.locationtech.geowave.adapter.vector.query.aggregation.VectorMaxAggregation;
import org.locationtech.geowave.core.geotime.store.query.aggregate.FieldNameParam;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.opengis.feature.simple.SimpleFeature;

/**
 * Aggregation function that finds the maximum value of a given numeric column.
 */
public class MaxFunction extends MathAggregationFunction {

  @Override
  protected String functionName() {
    return "MAX";
  }

  @Override
  protected Aggregation<?, ?, SimpleFeature> aggregation(FieldNameParam columnName) {
    return new VectorMaxAggregation(columnName);
  }

}

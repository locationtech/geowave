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

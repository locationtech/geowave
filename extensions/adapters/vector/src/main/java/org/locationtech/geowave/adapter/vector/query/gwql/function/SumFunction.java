package org.locationtech.geowave.adapter.vector.query.gwql.function;

import org.locationtech.geowave.adapter.vector.query.aggregation.VectorSumAggregation;
import org.locationtech.geowave.core.geotime.store.query.aggregate.FieldNameParam;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.opengis.feature.simple.SimpleFeature;

/**
 * Aggregation function that sums all non-null numeric values of a given column.
 */
public class SumFunction extends MathAggregationFunction {

  @Override
  protected String functionName() {
    return "SUM";
  }

  @Override
  protected Aggregation<?, ?, SimpleFeature> aggregation(FieldNameParam columnName) {
    return new VectorSumAggregation(columnName);
  }

}

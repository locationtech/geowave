package org.locationtech.geowave.adapter.vector.query.gwql.function;

import org.locationtech.geowave.adapter.vector.query.aggregation.VectorCountAggregation;
import org.locationtech.geowave.core.geotime.store.query.aggregate.FieldNameParam;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * Count aggregation function that accepts a single argument. If `*` is passed to the function, all
 * simple features will be counted. Otherwise, all non-null values of the given column will be
 * counted.
 */
public class CountFunction implements QLVectorAggregationFunction {
  @Override
  public Class<?> returnType() {
    return Long.class;
  }

  @Override
  public Aggregation<?, ?, SimpleFeature> getAggregation(
      final SimpleFeatureType featureType,
      final String[] functionArgs) {
    if (functionArgs == null || functionArgs.length != 1) {
      throw new RuntimeException("COUNT takes exactly 1 parameter");
    }
    final FieldNameParam columnName =
        functionArgs[0].equals("*") ? null : new FieldNameParam(functionArgs[0]);
    if (columnName != null && featureType.getDescriptor(columnName.getFieldName()) == null) {
      throw new RuntimeException(
          "No attribute called '" + columnName.getFieldName() + "' was found in the given type.");
    }
    return new VectorCountAggregation(columnName);
  }
}

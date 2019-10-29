package org.locationtech.geowave.adapter.vector.query.gwql.function;

import java.math.BigDecimal;
import org.locationtech.geowave.core.geotime.store.query.aggregate.FieldNameParam;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

/**
 * Base aggregation function for performing math aggregations on numeric columns.
 */
public abstract class MathAggregationFunction implements QLVectorAggregationFunction {
  @Override
  public Class<?> returnType() {
    return BigDecimal.class;
  }

  @Override
  public Aggregation<?, ?, SimpleFeature> getAggregation(
      final SimpleFeatureType featureType,
      final String[] functionArgs) {
    if (functionArgs == null || functionArgs.length != 1) {
      throw new RuntimeException(functionName() + " takes exactly 1 parameter");
    }
    if (functionArgs[0].equals("*")) {
      throw new RuntimeException(functionName() + " expects a numeric column.");
    }
    final FieldNameParam columnName = new FieldNameParam(functionArgs[0]);
    AttributeDescriptor descriptor = featureType.getDescriptor(columnName.getFieldName());
    if (descriptor == null) {
      throw new RuntimeException(
          "No attribute called '" + columnName.getFieldName() + "' was found in the given type.");
    }
    if (!Number.class.isAssignableFrom(descriptor.getType().getBinding())) {
      throw new RuntimeException(
          functionName()
              + " aggregation only works on numeric fields, given field was of type "
              + descriptor.getType().getBinding().getName()
              + ".");
    }
    return aggregation(columnName);
  }

  protected abstract String functionName();

  protected abstract Aggregation<?, ?, SimpleFeature> aggregation(final FieldNameParam columnName);
}

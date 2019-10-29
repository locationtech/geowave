package org.locationtech.geowave.adapter.vector.query.aggregation;

import java.math.BigDecimal;
import org.locationtech.geowave.core.geotime.store.query.aggregate.FieldNameParam;

/**
 * Calculates the sum of all value of a given numeric attribute. Ignores null attribute values.
 */
public class VectorSumAggregation extends VectorMathAggregation {

  public VectorSumAggregation() {
    this(null);
  }

  public VectorSumAggregation(final FieldNameParam fieldNameParam) {
    super(fieldNameParam);
  }

  @Override
  protected BigDecimal agg(BigDecimal a, BigDecimal b) {
    if (a == null) {
      return b;
    } else if (b == null) {
      return a;
    }
    return a.add(b);
  }

}

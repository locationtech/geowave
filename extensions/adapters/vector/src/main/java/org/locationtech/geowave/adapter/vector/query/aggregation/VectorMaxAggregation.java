package org.locationtech.geowave.adapter.vector.query.aggregation;

import java.math.BigDecimal;
import org.locationtech.geowave.core.geotime.store.query.aggregate.FieldNameParam;

/**
 * Aggregates to find the maximum value of a given numeric attribute. Ignores null attribute values.
 */
public class VectorMaxAggregation extends VectorMathAggregation {

  public VectorMaxAggregation() {
    this(null);
  }

  public VectorMaxAggregation(final FieldNameParam fieldNameParam) {
    super(fieldNameParam);
  }

  @Override
  protected BigDecimal agg(BigDecimal a, BigDecimal b) {
    if (a == null) {
      return b;
    } else if (b == null) {
      return a;
    }
    return a.max(b);
  }

}

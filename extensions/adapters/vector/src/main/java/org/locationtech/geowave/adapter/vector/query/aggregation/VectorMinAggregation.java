package org.locationtech.geowave.adapter.vector.query.aggregation;

import java.math.BigDecimal;
import org.locationtech.geowave.core.geotime.store.query.aggregate.FieldNameParam;

/**
 * Aggregates to find the minimum value of a given numeric attribute. Ignores null attribute values.
 */
public class VectorMinAggregation extends VectorMathAggregation {

  public VectorMinAggregation() {
    this(null);
  }

  public VectorMinAggregation(final FieldNameParam fieldNameParam) {
    super(fieldNameParam);
  }

  @Override
  protected BigDecimal agg(final BigDecimal a, final BigDecimal b) {
    if (a == null) {
      return b;
    } else if (b == null) {
      return a;
    }
    return a.min(b);
  }

}

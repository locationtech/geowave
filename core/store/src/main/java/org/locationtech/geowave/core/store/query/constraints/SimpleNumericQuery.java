package org.locationtech.geowave.core.store.query.constraints;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.Range;
import org.locationtech.geowave.core.index.dimension.BasicDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericRange;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class SimpleNumericQuery extends BasicQuery {
  public SimpleNumericQuery(final Range<Double> range) {
    super(createNumericConstraints(range));
  }

  public SimpleNumericQuery() {
    super();
  }

  @Override
  protected QueryFilter createQueryFilter(
      final MultiDimensionalNumericData constraints,
      final NumericDimensionField<?>[] orderedConstrainedDimensionFields,
      final NumericDimensionField<?>[] unconstrainedDimensionFields,
      final Index index) {
    // this will ignore fine grained filters and just use the row ID in the
    // index, we don't need fine-grained filtering for simple numeric queries
    return null;
  }

  private static Constraints createNumericConstraints(final Range<Double> range) {
    final List<ConstraintSet> constraints = new ArrayList<>();
    constraints.add(
        new ConstraintSet(
            BasicDimensionDefinition.class,
            new ConstraintData(new NumericRange(range.getMinimum(), range.getMaximum()), false)));
    return new Constraints(constraints);
  }
}

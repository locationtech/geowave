package org.locationtech.geowave.core.geotime.store.query;

import org.geotools.factory.CommonFactoryFinder;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.util.IndexOptimizationUtils;
import org.locationtech.geowave.core.store.api.Index;
import org.opengis.filter.Filter;

public class SpatialTemporalQuery extends AbstractVectorConstraints<ExplicitSpatialTemporalQuery> {

  public SpatialTemporalQuery() {
    super();
  }

  public SpatialTemporalQuery(final ExplicitSpatialTemporalQuery delegateConstraints) {
    super(delegateConstraints);
  }

  @Override
  protected ExplicitSpatialTemporalQuery newConstraints() {
    return new ExplicitSpatialTemporalQuery();
  }

  @Override
  protected boolean isSupported(final Index index, final GeotoolsFeatureDataAdapter adapter) {
    return IndexOptimizationUtils.hasTime(index, adapter)
        && IndexOptimizationUtils.hasAtLeastSpatial(index);
  }

  @Override
  protected Filter getFilter(final GeotoolsFeatureDataAdapter adapter) {
    return CommonFactoryFinder.getFilterFactory2().and(
        SpatialQuery.getFilter(adapter, delegateConstraints),
        TemporalQuery.getFilter(adapter, delegateConstraints));
  }

}

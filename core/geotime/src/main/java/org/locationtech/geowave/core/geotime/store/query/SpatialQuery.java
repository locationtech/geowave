package org.locationtech.geowave.core.geotime.store.query;

import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.IndexOptimizationUtils;
import org.locationtech.geowave.core.store.api.Index;
import org.opengis.filter.Filter;

public class SpatialQuery extends AbstractVectorConstraints<ExplicitSpatialQuery> {

  public SpatialQuery() {
    super();
  }

  public SpatialQuery(final ExplicitSpatialQuery delegateConstraints) {
    super(delegateConstraints);
  }

  @Override
  protected ExplicitSpatialQuery newConstraints() {
    return new ExplicitSpatialQuery();
  }

  @Override
  protected boolean isSupported(final Index index, final GeotoolsFeatureDataAdapter adapter) {
    return IndexOptimizationUtils.hasAtLeastSpatial(index);
  }

  @Override
  protected Filter getFilter(final GeotoolsFeatureDataAdapter adapter) {
    return getFilter(adapter, delegateConstraints);
  }

  protected static Filter getFilter(
      final GeotoolsFeatureDataAdapter adapter,
      final ExplicitSpatialQuery delegateConstraints) {
    return GeometryUtils.geometryToSpatialOperator(
        delegateConstraints.getQueryGeometry(),
        adapter.getFeatureType().getGeometryDescriptor().getLocalName());
  }
}

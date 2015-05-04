package mil.nga.giat.geowave.analytic.model;

import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

/**
 * 
 * Builds an index model with longitude and latitude.
 * 
 */
public class SpatialIndexModelBuilder implements
		IndexModelBuilder
{

	@Override
	public CommonIndexModel buildModel() {
		return IndexType.SPATIAL_VECTOR.getDefaultIndexModel();
	}

}

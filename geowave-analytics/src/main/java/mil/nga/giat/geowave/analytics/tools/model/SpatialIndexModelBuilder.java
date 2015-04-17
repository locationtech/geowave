package mil.nga.giat.geowave.analytics.tools.model;

import mil.nga.giat.geowave.store.index.CommonIndexModel;
import mil.nga.giat.geowave.store.index.IndexType;

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

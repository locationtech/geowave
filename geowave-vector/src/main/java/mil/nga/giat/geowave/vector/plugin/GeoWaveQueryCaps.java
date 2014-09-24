package mil.nga.giat.geowave.vector.plugin;

import org.geotools.data.QueryCapabilities;
import org.opengis.filter.sort.SortBy;

/**
 * A definition of the Query capabilities provided to GeoTools by the GeoWave
 * data store.
 * 
 */
public class GeoWaveQueryCaps extends
		QueryCapabilities
{

	public GeoWaveQueryCaps() {}

	// TODO implement sorting...
	@Override
	public boolean supportsSorting(
			final SortBy[] sortAttributes ) {
		return false;
	}

}

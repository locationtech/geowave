package mil.nga.giat.geowave.adapter.vector.plugin;

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
		// called for every WFS-T operation. Without sorting requests, the
		// argument is empty or null
		// returning false fails the operation, disabling any capability of
		// writing.
		return sortAttributes == null || sortAttributes.length == 0;
	}

}

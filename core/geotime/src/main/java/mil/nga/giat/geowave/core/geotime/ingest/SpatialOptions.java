package mil.nga.giat.geowave.core.geotime.ingest;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.store.spi.DimensionalityTypeOptions;

import com.beust.jcommander.Parameter;

public class SpatialOptions implements
		DimensionalityTypeOptions
{
	@Parameter(names = {
		"--storeTime"
	}, required = false, description = "The index will store temporal values.  This allows it to slightly more efficiently run spatial-temporal queries although if spatial-temporal queries are a common use case, a separate spatial-temporal index is recommended.")
	protected boolean storeTime = false;

	@Parameter(names = {
		"-c",
		"--crs"
	}, required = false, description = "The native Coordinate Reference System used within the index.  All spatial data will be projected into this CRS for appropriate indexing as needed.")
	protected String crs = GeometryUtils.DEFAULT_CRS_STR;

	public void setCrs(
			String crs ) {
		this.crs = crs;
	}
}

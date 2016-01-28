package mil.nga.giat.geowave.adapter.raster.resize;

import mil.nga.giat.geowave.core.cli.CustomOperationCategory;

public class RasterOperationCategory extends
		CustomOperationCategory
{

	public RasterOperationCategory() {
		super(
				"raster",
				"Raster Operations",
				"Operations to perform transformations on raster data in GeoWave");
	}

}

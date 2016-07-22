package mil.nga.giat.geowave.format.geotools.raster;

import mil.nga.giat.geowave.adapter.raster.adapter.merge.RasterTileMergeStrategy;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata.NoDataMergeStrategy;

public class NoDataMergeStrategyProvider implements
		RasterMergeStrategyProviderSpi
{
	public static String NAME = "no-data";

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public RasterTileMergeStrategy<?> getStrategy() {
		return new NoDataMergeStrategy();
	}

}

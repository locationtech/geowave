package mil.nga.giat.geowave.format.geotools.raster;

import mil.nga.giat.geowave.adapter.raster.adapter.merge.RasterTileMergeStrategy;

public class NoMergeStrategyProvider implements
		RasterMergeStrategyProviderSpi
{
	public static String NAME = "none";

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public RasterTileMergeStrategy<?> getStrategy() {
		return null;
	}

}

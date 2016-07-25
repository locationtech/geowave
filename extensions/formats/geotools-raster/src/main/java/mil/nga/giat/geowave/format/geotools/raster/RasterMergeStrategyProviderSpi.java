package mil.nga.giat.geowave.format.geotools.raster;

import mil.nga.giat.geowave.adapter.raster.adapter.merge.RasterTileMergeStrategy;

public interface RasterMergeStrategyProviderSpi
{
	public String getName();

	public RasterTileMergeStrategy<?> getStrategy();
}

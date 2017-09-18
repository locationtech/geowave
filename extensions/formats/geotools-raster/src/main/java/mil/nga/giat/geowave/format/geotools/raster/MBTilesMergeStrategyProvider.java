package mil.nga.giat.geowave.format.geotools.raster;

import mil.nga.giat.geowave.adapter.raster.adapter.merge.RasterTileMergeStrategy;
import mil.nga.giat.geowave.adapter.raster.adapter.merge.mbtiles.MBTilesMergeStrategy;

public class MBTilesMergeStrategyProvider implements
		RasterMergeStrategyProviderSpi
{

	public static final String NAME = "mbtiles";

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public RasterTileMergeStrategy<?> getStrategy() {
		return new MBTilesMergeStrategy();
	}

}

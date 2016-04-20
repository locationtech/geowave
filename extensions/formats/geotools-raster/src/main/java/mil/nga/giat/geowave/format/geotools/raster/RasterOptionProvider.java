package mil.nga.giat.geowave.format.geotools.raster;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.core.ingest.spi.IngestFormatOptionProvider;

public class RasterOptionProvider implements
		IngestFormatOptionProvider
{

	@Parameter(names = "--pyramid", description = "Build an image pyramid on ingest for quick reduced resolution query")
	private boolean buildPyramid = RasterDataAdapter.DEFAULT_BUILD_PYRAMID;

	@Parameter(names = "--tileSize", description = "Optional parameter to set the tile size stored (default is 256)")
	private int tileSize = RasterDataAdapter.DEFAULT_TILE_SIZE;

	public RasterOptionProvider() {}

	public boolean isBuildPyramid() {
		return buildPyramid;
	}

	public int getTileSize() {
		return tileSize;
	}

	public void setBuildPyramid(
			boolean buildPyramid ) {
		this.buildPyramid = buildPyramid;
	}

	public void setTileSize(
			int tileSize ) {
		this.tileSize = tileSize;
	}
}

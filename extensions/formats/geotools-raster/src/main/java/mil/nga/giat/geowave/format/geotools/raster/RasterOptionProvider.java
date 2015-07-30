package mil.nga.giat.geowave.format.geotools.raster;

import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.core.ingest.IngestFormatOptionProvider;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class RasterOptionProvider implements
		IngestFormatOptionProvider
{
	private final static String BUILD_PYRAMID = "pyramid";
	private final static String TILE_SIZE = "tileSize";

	private boolean buildPyramid = RasterDataAdapter.DEFAULT_BUILD_PYRAMID;
	private int tileSize = RasterDataAdapter.DEFAULT_TILE_SIZE;

	public RasterOptionProvider() {}

	@Override
	public void applyOptions(
			final Options allOptions ) {
		allOptions.addOption(new Option(
				BUILD_PYRAMID,
				false,
				"Build an image pyramid on ingest for quick reduced resolution query"));
		allOptions.addOption(new Option(
				TILE_SIZE,
				true,
				"Optional parameter to set the tile size stored (default is 256)"));
	}

	@Override
	public void parseOptions(
			final CommandLine commandLine ) {
		buildPyramid = commandLine.hasOption(BUILD_PYRAMID);
		if (commandLine.hasOption(TILE_SIZE)) {
			tileSize = Integer.parseInt(commandLine.getOptionValue(TILE_SIZE));
		}
	}

	public boolean isBuildPyramid() {
		return buildPyramid;
	}

	public int getTileSize() {
		return tileSize;
	}
}

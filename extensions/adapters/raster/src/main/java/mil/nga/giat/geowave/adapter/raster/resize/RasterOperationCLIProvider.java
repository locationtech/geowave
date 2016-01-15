package mil.nga.giat.geowave.adapter.raster.resize;

import mil.nga.giat.geowave.core.cli.CLIOperation;
import mil.nga.giat.geowave.core.cli.CLIOperationCategory;
import mil.nga.giat.geowave.core.cli.CLIOperationProviderSpi;

public class RasterOperationCLIProvider implements
		CLIOperationProviderSpi
{
	/**
	 * This identifies the set of operations supported and which driver to
	 * execute based on the operation selected.
	 */
	private static final CLIOperation[] RASTER_OPERATIONS = new CLIOperation[] {
		new CLIOperation(
				"raster-resize",
				"Resize Raster Tiles",
				new RasterTileResizeJobRunner())
	};
	private static final CLIOperationCategory CATEGORY = new RasterOperationCategory();

	@Override
	public CLIOperation[] getOperations() {
		return RASTER_OPERATIONS;
	}

	@Override
	public CLIOperationCategory getCategory() {
		return CATEGORY;
	}
}

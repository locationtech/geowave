package mil.nga.giat.geowave.format.landsat8;

import mil.nga.giat.geowave.core.cli.spi.CLIOperationProviderSpi;

public class Landsat8OperationProvider implements
		CLIOperationProviderSpi
{
	private static final Class<?>[] OPERATIONS = new Class<?>[] {
		Landsat8Section.class,
		Landsat8AnalyzeCommand.class,
		Landsat8DownloadCommand.class,
		Landsat8IngestCommand.class,
		Landsat8IngestRasterCommand.class,
		Landsat8IngestVectorCommand.class,
	};

	@Override
	public Class<?>[] getOperations() {
		return OPERATIONS;
	}
}

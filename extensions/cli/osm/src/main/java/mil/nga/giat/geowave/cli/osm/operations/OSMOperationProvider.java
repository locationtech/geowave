package mil.nga.giat.geowave.cli.osm.operations;

import mil.nga.giat.geowave.core.cli.spi.CLIOperationProviderSpi;

public class OSMOperationProvider implements
		CLIOperationProviderSpi
{

	private static final Class<?>[] OPERATIONS = new Class<?>[] {
		OSMSection.class,
		StageOSMToHDFSCommand.class,
		IngestOSMToGeoWaveCommand.class
	};

	@Override
	public Class<?>[] getOperations() {
		return OPERATIONS;
	}

}

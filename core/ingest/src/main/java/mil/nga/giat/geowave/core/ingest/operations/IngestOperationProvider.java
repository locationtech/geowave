package mil.nga.giat.geowave.core.ingest.operations;

import mil.nga.giat.geowave.core.cli.spi.CLIOperationProviderSpi;

public class IngestOperationProvider implements
		CLIOperationProviderSpi
{

	private static final Class<?>[] OPERATIONS = new Class<?>[] {
		IngestSection.class,
		KafkaToGeowaveCommand.class,
		ListPluginsCommand.class,
		LocalToGeowaveCommand.class,
		LocalToHdfsCommand.class,
		LocalToKafkaCommand.class,
		LocalToMapReduceToGeowaveCommand.class,
		MapReduceToGeowaveCommand.class
	};

	@Override
	public Class<?>[] getOperations() {
		return OPERATIONS;
	}

}

package mil.nga.giat.geowave.ingest;

public interface CLIOperationProviderSpi
{
	public CLIOperation[] getOperations();

	public CLIOperationCategory getCategory();
}

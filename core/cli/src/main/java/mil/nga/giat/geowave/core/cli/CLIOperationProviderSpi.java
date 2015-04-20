package mil.nga.giat.geowave.core.cli;

public interface CLIOperationProviderSpi
{
	public CLIOperation[] getOperations();

	public CLIOperationCategory getCategory();
}

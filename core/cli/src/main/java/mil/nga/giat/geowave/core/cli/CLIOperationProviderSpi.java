package mil.nga.giat.geowave.core.cli;

public interface CLIOperationProviderSpi
{
	public CLIOperation[] createOperations();

	public CommandObject getOperationCategory();
}

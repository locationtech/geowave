package mil.nga.giat.geowave.core.cli.spi;

/**
 * Service provider interface for command-line-interface operations
 */
public interface CLIOperationProviderSpi
{
	/**
	 * Get the operations for this SPI
	 * 
	 * @return An array of operations
	 */
	public Class<?>[] getOperations();
}

package mil.nga.giat.geowave.core.cli.api;

import java.util.Properties;

/**
 * All plugins must provide this interface
 */
public interface PluginOptions
{
	/**
	 * Get the type of plugin
	 * 
	 * @return Type of plugin
	 */
	public String getType();

	/**
	 * Select the plugin associated with the qualifier
	 * 
	 * @param qualifier
	 *            Qualifier to lookup
	 */
	public void selectPlugin(
			String qualifier );

	/**
	 * Save the plugin options
	 * 
	 * @param properties
	 *            Properties to save
	 * @param namespace
	 *            Namespace to apply to saved properties
	 */
	public void save(
			Properties properties,
			String namespace );

	/**
	 * Load the properties and namespace into the plugin
	 * 
	 * @param properties
	 *            Properties to load
	 * @param namespace
	 *            Namespace to apply to loaded properties
	 * @return True if successfully loaded, otherwise false
	 */
	public boolean load(
			Properties properties,
			String namespace );
}

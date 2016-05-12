package mil.nga.giat.geowave.core.cli.api;

import java.util.Properties;

/**
 * All plugins must provide this interface
 */
public interface PluginOptions
{
	String getType();

	void selectPlugin(
			String qualifier );

	void save(
			Properties properties,
			String namespace );

	boolean load(
			Properties properties,
			String namespace );
}

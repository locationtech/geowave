package mil.nga.giat.geowave.core.cli.api;

import java.util.Properties;

/**
 * All plugins must provide this interface
 */
public interface PluginOptions
{
	public String getType();

	public void selectPlugin(
			String qualifier );

	public void save(
			Properties properties,
			String namespace );

	public boolean load(
			Properties properties,
			String namespace );
}

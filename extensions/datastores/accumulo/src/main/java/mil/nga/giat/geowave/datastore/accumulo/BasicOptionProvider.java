package mil.nga.giat.geowave.datastore.accumulo;

import java.util.Map;

import mil.nga.giat.geowave.datastore.accumulo.IteratorConfig.OptionProvider;

public class BasicOptionProvider implements
		OptionProvider
{
	private final Map<String, String> options;

	public BasicOptionProvider(
			final Map<String, String> options ) {
		this.options = options;
	}

	@Override
	public Map<String, String> getOptions(
			final Map<String, String> existingOptions ) {
		return options;
	}

}

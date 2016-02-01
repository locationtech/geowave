package mil.nga.giat.geowave.core.store.config;

/**
 * This is a marker so that implementations that use config options can treat
 * passwords appropriately
 * 
 * 
 */
public class PasswordConfigOption extends
		StringConfigOption
{

	public PasswordConfigOption(
			final String name,
			final String description ) {
		super(
				name,
				description);
	}

	public PasswordConfigOption(
			final String name,
			final String description,
			final boolean optional ) {
		super(
				name,
				description,
				optional);
	}

}

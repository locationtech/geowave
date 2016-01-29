package mil.nga.giat.geowave.core.store.config;

public class StringConfigOption extends
		AbstractConfigOption<String>
{

	public StringConfigOption(
			final String name,
			final String description ) {
		this(
				name,
				description,
				false);
	}

	public StringConfigOption(
			final String name,
			final String description,
			final boolean optional ) {
		super(
				name,
				description,
				optional);
	}

	@Override
	public String valueFromString(
			final String str ) {
		return str;
	}

	@Override
	public String valueToString(
			final String value ) {
		return value;
	}

}

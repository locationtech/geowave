package mil.nga.giat.geowave.core.store.config;

public class BooleanConfigOption extends
		AbstractConfigOption<Boolean>
{

	public BooleanConfigOption(
			final String name,
			final String description,
			final boolean optional ) {
		super(
				name,
				description,
				optional);
	}

	@Override
	public Boolean valueFromString(
			final String str ) {
		return Boolean.valueOf(str);
	}

	@Override
	public String valueToString(
			final Boolean value ) {
		return value.toString();
	}

}

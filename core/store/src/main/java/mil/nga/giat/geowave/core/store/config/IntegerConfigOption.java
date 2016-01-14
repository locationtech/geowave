package mil.nga.giat.geowave.core.store.config;

public class IntegerConfigOption extends
		AbstractConfigOption<Integer>
{

	public IntegerConfigOption(
			final String name,
			final String description,
			final boolean optional ) {
		super(
				name,
				description,
				optional);
	}

	@Override
	public Integer valueFromString(
			final String str ) {
		return Integer.parseInt(str);
	}

	@Override
	public String valueToString(
			final Integer value ) {
		return value.toString();
	}

}

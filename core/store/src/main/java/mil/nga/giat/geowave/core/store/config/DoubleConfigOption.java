package mil.nga.giat.geowave.core.store.config;

public class DoubleConfigOption extends
		AbstractConfigOption<Double>
{

	public DoubleConfigOption(
			String name,
			String description,
			boolean optional ) {
		super(
				name,
				description,
				optional);
	}

	@Override
	public Double valueFromString(
			final String str ) {
		return Double.parseDouble(str);
	}

	@Override
	public String valueToString(
			final Double value ) {
		return value.toString();
	}

}

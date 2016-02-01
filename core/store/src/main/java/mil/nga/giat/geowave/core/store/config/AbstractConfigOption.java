package mil.nga.giat.geowave.core.store.config;

abstract public class AbstractConfigOption<T>
{
	private final String name;
	private final String description;
	private final boolean optional;

	public AbstractConfigOption(
			final String name,
			final String description,
			final boolean optional ) {
		this.name = name;
		this.description = description;
		this.optional = optional;
	}

	public String getName() {
		return name;
	}

	public String getDescription() {
		return description;
	}

	public boolean isOptional() {
		return optional;
	}

	abstract public T valueFromString(
			String str );

	abstract public String valueToString(
			T value );

}

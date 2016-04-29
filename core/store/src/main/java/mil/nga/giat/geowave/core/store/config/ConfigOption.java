package mil.nga.giat.geowave.core.store.config;

public class ConfigOption
{
	private final String name;
	private final String description;
	private final boolean optional;
	private boolean password;
	private Class type;

	public ConfigOption(
			final String name,
			final String description,
			final boolean optional,
			final Class type ) {
		this.name = name;
		this.description = description;
		this.optional = optional;
		this.type = type;
	}

	public Class getType() {
		return type;
	}

	public void setType(
			Class type ) {
		this.type = type;
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

	public boolean isPassword() {
		return password;
	}

	public void setPassword(
			boolean password ) {
		this.password = password;
	}
}

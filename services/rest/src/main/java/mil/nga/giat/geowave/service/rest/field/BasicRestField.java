package mil.nga.giat.geowave.service.rest.field;

public class BasicRestField<T> implements
		RestField<T>
{

	private final String name;
	private final Class<T> type;
	private final String description;
	private final boolean required;

	public BasicRestField(
			final String name,
			final Class<T> type,
			final String description,
			final boolean required ) {
		this.name = name;
		this.type = type;
		this.description = description;
		this.required = required;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Class<T> getType() {
		return type;
	}

	@Override
	public String getDescription() {
		return description;
	}

	@Override
	public boolean isRequired() {
		return required;
	}
}

package mil.nga.giat.geowave.service.rest.field;

public interface RestField<T>
{
	public String getName();

	public Class<T> getType();

	public String getDescription();

	public boolean isRequired();
}

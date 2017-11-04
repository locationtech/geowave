package mil.nga.giat.geowave.service.rest.field;

public interface RestFieldValue<T> extends
		RestField<T>
{
	public void setValue(
			T value )
			throws IllegalArgumentException,
			IllegalAccessException;
}

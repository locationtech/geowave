package mil.nga.giat.geowave.core.store.data.field;

public interface FieldSerializationProviderSpi<T>
{
	public FieldReader<T> getFieldReader();

	public FieldWriter<Object, T> getFieldWriter();
}

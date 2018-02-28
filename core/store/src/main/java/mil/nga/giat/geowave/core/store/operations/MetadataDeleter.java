package mil.nga.giat.geowave.core.store.operations;

public interface MetadataDeleter extends
		AutoCloseable
{
	public boolean delete(
			MetadataQuery query );

	public void flush();
}

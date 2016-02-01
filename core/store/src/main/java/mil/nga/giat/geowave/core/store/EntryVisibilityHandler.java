package mil.nga.giat.geowave.core.store;

public interface EntryVisibilityHandler<T>
{
	public byte[] getVisibility(
			DataStoreEntryInfo entryInfo,
			T entry );
}

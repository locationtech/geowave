package mil.nga.giat.geowave.core.store;

public interface DataStoreOptions
{
	public boolean isUseAltIndex();

	public boolean isPersistIndex();

	public boolean isPersistAdapter();

	public boolean isPersistDataStatistics();

	public boolean isCreateTable();

	public boolean isEnableBlockCache();
}

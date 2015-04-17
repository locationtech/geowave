package mil.nga.giat.geowave.analytics.tools.dbops;

import mil.nga.giat.geowave.analytics.tools.ConfigurationWrapper;
import mil.nga.giat.geowave.store.index.IndexStore;

public interface IndexStoreFactory
{
	public IndexStore getIndexStore(
			ConfigurationWrapper context )
			throws InstantiationException;
}

package mil.nga.giat.geowave.analytic.db;

import mil.nga.giat.geowave.analytic.ConfigurationWrapper;
import mil.nga.giat.geowave.core.store.index.IndexStore;

public interface IndexStoreFactory
{
	public IndexStore getIndexStore(
			ConfigurationWrapper context )
			throws InstantiationException;
}

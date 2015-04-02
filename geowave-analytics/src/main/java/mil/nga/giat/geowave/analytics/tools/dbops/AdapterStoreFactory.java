package mil.nga.giat.geowave.analytics.tools.dbops;

import mil.nga.giat.geowave.analytics.tools.ConfigurationWrapper;
import mil.nga.giat.geowave.store.adapter.AdapterStore;

public interface AdapterStoreFactory
{
	public AdapterStore getAdapterStore(
			ConfigurationWrapper context )
			throws InstantiationException;
}

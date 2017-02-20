package mil.nga.giat.geowave.adapter.vector;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.geotools.data.DataStore;
import org.junit.Rule;
import org.junit.rules.TestName;

import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveGTDataStoreFactory;
import mil.nga.giat.geowave.adapter.vector.plugin.GeoWavePluginConfig;
import mil.nga.giat.geowave.adapter.vector.plugin.GeoWavePluginException;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.memory.MemoryStoreFactoryFamily;
import mil.nga.giat.geowave.core.store.query.EverythingQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

public class BaseDataStoreTest
{
	@Rule
	public TestName name = new TestName();

	protected DataStore createDataStore()
			throws IOException,
			GeoWavePluginException {
		final Map<String, Serializable> params = new HashMap<String, Serializable>();
		params.put(
				"gwNamespace",
				"test_" + getClass().getName() + "_" + name.getMethodName());
		final StoreFactoryFamilySpi storeFactoryFamily = new MemoryStoreFactoryFamily();
		// delete existing data
		new GeoWavePluginConfig(
				storeFactoryFamily,
				params).getDataStore().delete(
				new QueryOptions(),
				new EverythingQuery());

		return new GeoWaveGTDataStoreFactory(
				storeFactoryFamily).createNewDataStore(params);
	}
}

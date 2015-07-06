package mil.nga.giat.geowave.adapter.vector.plugin;

import java.awt.RenderingHints.Key;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;

/**
 * This factory is injected by GeoTools using Java SPI and is used to expose
 * GeoWave as a DataStore to GeoTools. It should be defined within a file
 * META-INF/services/org.geotools.data.DataStoreFactorySpi to inject this into
 * GeoTools.
 * 
 */
public class GeoWaveGTDataStoreFactory implements
		DataStoreFactorySpi
{
	private static class DataStoreCacheEntry
	{
		private final Map<String, Serializable> params;
		private final DataStore dataStore;

		public DataStoreCacheEntry(
				final Map<String, Serializable> params,
				final DataStore dataStore ) {
			this.params = params;
			this.dataStore = dataStore;
		}
	}

	private static final Logger LOGGER = Logger.getLogger(GeoWaveGTDataStoreFactory.class);
	private final List<DataStoreCacheEntry> dataStoreCache = new ArrayList<DataStoreCacheEntry>();

	/**
	 * Public "no argument" constructor called by Factory Service Provider (SPI)
	 * entry listed in META-INF/services/org.geotools.data.DataStoreFactorySPI
	 */
	public GeoWaveGTDataStoreFactory() {}

	// GeoServer seems to call this several times so we should cache a
	// connection if the parameters are the same, I'm not sure this is entirely
	// correct but it keeps us from making several connections for the same data
	// store
	@Override
	public DataStore createDataStore(
			final Map<String, Serializable> params )
			throws IOException {
		// iterate in reverse over the cache so the most recently added is
		// accessed first
		for (int index = dataStoreCache.size() - 1; index >= 0; index--) {
			final DataStoreCacheEntry cacheEntry = dataStoreCache.get(index);
			if (paramsEqual(
					params,
					cacheEntry.params)) {
				return cacheEntry.dataStore;
			}
		}
		return createNewDataStore(params);
	}

	private boolean paramsEqual(
			final Map<String, Serializable> params1,
			final Map<String, Serializable> params2 ) {
		if (params1.size() == params2.size()) {
			for (final Entry<String, Serializable> entry : params1.entrySet()) {
				final Serializable value = params2.get(entry.getKey());
				if (value == null) {
					if (entry.getValue() == null) {
						continue;
					}
					return false;
				}
				else if (!value.equals(entry.getValue())) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

	@Override
	public DataStore createNewDataStore(
			final Map<String, Serializable> params )
			throws IOException {
		final GeoWaveGTDataStore dataStore;
		try {
			dataStore = new GeoWaveGTDataStore(
					new GeoWavePluginConfig(
							params));
			dataStoreCache.add(new DataStoreCacheEntry(
					params,
					dataStore));
		}
		catch (final Exception ex) {
			throw new IOException(
					"Error initializing datastore",
					ex);
		}
		return dataStore;
	}

	@Override
	public String getDisplayName() {
		return "GeoWave Datastore";
	}

	@Override
	public String getDescription() {
		return "A datastore that uses the GeoWave API for spatial data persistence in the cloud";
	}

	@Override
	public Param[] getParametersInfo() {
		final List<Param> params = GeoWavePluginConfig.getPluginParams();
		return params.toArray(new Param[params.size()]);
	}

	@Override
	public boolean canProcess(
			Map<String, Serializable> params ) {
		try {
			// rely on validation in GeoWavePluginConfig's constructor
			new GeoWavePluginConfig(
					params);
			return true;
		}
		catch (GeoWavePluginException e) {
			// supplied map does not contain all necessary parameters to
			// construct GeoWaveGTDataStore
			return false;
		}
	}

	private static Boolean isAvailable = null;

	@Override
	public synchronized boolean isAvailable() {
		if (isAvailable == null) {
			try {
				Class.forName("mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveGTDataStore");
				isAvailable = true;
			}
			catch (ClassNotFoundException e) {
				isAvailable = false;
			}
		}
		return isAvailable;
	}

	@Override
	public Map<Key, ?> getImplementationHints() {
		// No implementation hints required at this time
		return Collections.emptyMap();
	}

}
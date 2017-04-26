package mil.nga.giat.geowave.adapter.vector.plugin;

import java.awt.RenderingHints.Key;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;
import org.geotools.factory.FactoryIteratorProvider;
import org.geotools.factory.GeoTools;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;

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

	public final static String DISPLAY_NAME_PREFIX = "GeoWave Datastore - ";
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGTDataStoreFactory.class);
	private final List<DataStoreCacheEntry> dataStoreCache = new ArrayList<DataStoreCacheEntry>();
	private final StoreFactoryFamilySpi geowaveStoreFactoryFamily;
	private static Boolean isAvailable = null;

	/**
	 * Public "no argument" constructor called by Factory Service Provider (SPI)
	 * entry listed in META-INF/services/org.geotools.data.DataStoreFactorySPI
	 */
	public GeoWaveGTDataStoreFactory() {
		final Collection<StoreFactoryFamilySpi> dataStoreFactories = GeoWaveStoreFinder
				.getRegisteredStoreFactoryFamilies()
				.values();

		if (dataStoreFactories.isEmpty()) {
			LOGGER.error("No GeoWave DataStore found!  Geotools datastore for GeoWave is unavailable");
			geowaveStoreFactoryFamily = null;
		}
		else {
			final Iterator<StoreFactoryFamilySpi> it = dataStoreFactories.iterator();
			geowaveStoreFactoryFamily = it.next();
			if (it.hasNext()) {
				GeoTools.addFactoryIteratorProvider(new GeoWaveGTDataStoreFactoryIteratorProvider());
			}
		}
	}

	public GeoWaveGTDataStoreFactory(
			final StoreFactoryFamilySpi geowaveStoreFactoryFamily ) {
		this.geowaveStoreFactoryFamily = geowaveStoreFactoryFamily;
	}

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
							geowaveStoreFactoryFamily,
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
		return DISPLAY_NAME_PREFIX + geowaveStoreFactoryFamily.getType().toUpperCase();
	}

	@Override
	public String getDescription() {
		return "A datastore that uses the GeoWave API for spatial data persistence in "
				+ geowaveStoreFactoryFamily.getType() + ". " + geowaveStoreFactoryFamily.getDescription();
	}

	@Override
	public Param[] getParametersInfo() {
		final List<Param> params = GeoWavePluginConfig.getPluginParams(geowaveStoreFactoryFamily);
		return params.toArray(new Param[params.size()]);
	}

	@Override
	public boolean canProcess(
			final Map<String, Serializable> params ) {
		final Map<String, String> dataStoreParams = params
				.entrySet()
				.stream()
				.filter(
						e -> !GeoWavePluginConfig.BASE_GEOWAVE_PLUGIN_PARAM_KEYS.contains(
								e.getKey()))
				.collect(
						Collectors.toMap(
								e -> e.getKey() == null ? null : e.getKey().toString(),
								e -> e.getValue() == null ? null : e.getValue().toString()));

		return GeoWaveStoreFinder.exactMatch(
				geowaveStoreFactoryFamily,
				dataStoreParams);
	}

	@Override
	public synchronized boolean isAvailable() {
		if (isAvailable == null) {
			if (geowaveStoreFactoryFamily == null) {
				isAvailable = false;
			}
			else {
				try {
					Class.forName("mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveGTDataStore");
					isAvailable = true;
				}
				catch (final ClassNotFoundException e) {
					isAvailable = false;
				}
			}
		}
		return isAvailable;
	}

	@Override
	public Map<Key, ?> getImplementationHints() {
		// No implementation hints required at this time
		return Collections.emptyMap();
	}

	private static class GeoWaveGTDataStoreFactoryIteratorProvider implements
			FactoryIteratorProvider
	{

		@Override
		public <T> Iterator<T> iterator(
				final Class<T> cls ) {
			if ((cls != null) && cls.isAssignableFrom(DataStoreFactorySpi.class)) {
				return (Iterator<T>) new GeoWaveGTDataStoreFactoryIterator();
			}
			return null;
		}

		private static class GeoWaveGTDataStoreFactoryIterator implements
				Iterator<DataStoreFactorySpi>
		{
			private final Iterator<DataStoreFactorySpi> it;

			private GeoWaveGTDataStoreFactoryIterator() {
				final Iterator<StoreFactoryFamilySpi> geowaveDataStoreIt = GeoWaveStoreFinder
						.getRegisteredStoreFactoryFamilies()
						.values()
						.iterator();
				geowaveDataStoreIt.next();
				it = Iterators.transform(
						geowaveDataStoreIt,
						new GeoWaveStoreToGeoToolsDataStore());
			}

			@Override
			public boolean hasNext() {
				return it.hasNext();
			}

			@Override
			public DataStoreFactorySpi next() {
				return it.next();
			}

			@Override
			public void remove() {}
		}
	}

	/**
	 * Below is a set of 9 additional GeoWaveGTDataStoreFactory's, its a bit of
	 * a hack, but must be done because the geotools factory registry will
	 * re-use instances of the same class, so each individual geowave data store
	 * must be registered as a different class (the alternative is dynamic
	 * compilation of classes to add to the classloader).
	 *
	 *
	 */
	private static class GeoWaveStoreToGeoToolsDataStore implements
			Function<StoreFactoryFamilySpi, DataStoreFactorySpi>
	{
		private int i = 0;

		public GeoWaveStoreToGeoToolsDataStore() {}

		@Override
		public DataStoreFactorySpi apply(
				final StoreFactoryFamilySpi input ) {
			i++;
			switch (i) {
				case 1:
					return new GeoWaveGTDataStoreFactory1(
							input);
				case 2:
					return new GeoWaveGTDataStoreFactory2(
							input);
				case 3:
					return new GeoWaveGTDataStoreFactory3(
							input);
				case 4:
					return new GeoWaveGTDataStoreFactory4(
							input);
				case 5:
					return new GeoWaveGTDataStoreFactory5(
							input);
				case 6:
					return new GeoWaveGTDataStoreFactory6(
							input);
				case 7:
					return new GeoWaveGTDataStoreFactory7(
							input);
				case 8:
					return new GeoWaveGTDataStoreFactory8(
							input);
				case 9:
					return new GeoWaveGTDataStoreFactory9(
							input);

			}
			LOGGER.error("Too many GeoWave Datastores registered for GeoTools data store");
			return new GeoWaveGTDataStoreFactory(
					input);
		}
	}

	private static class GeoWaveGTDataStoreFactory1 extends
			GeoWaveGTDataStoreFactory
	{

		public GeoWaveGTDataStoreFactory1(
				final StoreFactoryFamilySpi geowaveStoreFactoryFamily ) {
			super(
					geowaveStoreFactoryFamily);
		}
	}

	private static class GeoWaveGTDataStoreFactory2 extends
			GeoWaveGTDataStoreFactory
	{

		public GeoWaveGTDataStoreFactory2(
				final StoreFactoryFamilySpi geowaveStoreFactoryFamily ) {
			super(
					geowaveStoreFactoryFamily);
		}
	}

	private static class GeoWaveGTDataStoreFactory3 extends
			GeoWaveGTDataStoreFactory
	{

		public GeoWaveGTDataStoreFactory3(
				final StoreFactoryFamilySpi geowaveStoreFactoryFamily ) {
			super(
					geowaveStoreFactoryFamily);
		}
	}

	private static class GeoWaveGTDataStoreFactory4 extends
			GeoWaveGTDataStoreFactory
	{

		public GeoWaveGTDataStoreFactory4(
				final StoreFactoryFamilySpi geowaveStoreFactoryFamily ) {
			super(
					geowaveStoreFactoryFamily);
		}
	}

	private static class GeoWaveGTDataStoreFactory5 extends
			GeoWaveGTDataStoreFactory
	{

		public GeoWaveGTDataStoreFactory5(
				final StoreFactoryFamilySpi geowaveStoreFactoryFamily ) {
			super(
					geowaveStoreFactoryFamily);
		}
	}

	private static class GeoWaveGTDataStoreFactory6 extends
			GeoWaveGTDataStoreFactory
	{

		public GeoWaveGTDataStoreFactory6(
				final StoreFactoryFamilySpi geowaveStoreFactoryFamily ) {
			super(
					geowaveStoreFactoryFamily);
		}
	}

	private static class GeoWaveGTDataStoreFactory7 extends
			GeoWaveGTDataStoreFactory
	{

		public GeoWaveGTDataStoreFactory7(
				final StoreFactoryFamilySpi geowaveStoreFactoryFamily ) {
			super(
					geowaveStoreFactoryFamily);
		}
	}

	private static class GeoWaveGTDataStoreFactory8 extends
			GeoWaveGTDataStoreFactory
	{

		public GeoWaveGTDataStoreFactory8(
				final StoreFactoryFamilySpi geowaveStoreFactoryFamily ) {
			super(
					geowaveStoreFactoryFamily);
		}
	}

	private static class GeoWaveGTDataStoreFactory9 extends
			GeoWaveGTDataStoreFactory
	{

		public GeoWaveGTDataStoreFactory9(
				final StoreFactoryFamilySpi geowaveStoreFactoryFamily ) {
			super(
					geowaveStoreFactoryFamily);
		}
	}
}
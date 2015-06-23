package mil.nga.giat.geowave.adapter.vector.plugin;

import java.awt.RenderingHints.Key;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;

/**
 * This factory is injected by GeoTools using Java SPI and is used to expose
 * GeoWave as a DataStore to GeoTools. It should be defined within a file
 * META-INF/services/org.geotools.data.DataStoreFactorySpi to inject this into
 * GeoTools.
 * 
 */
public class GeoWaveGTMemDataStoreFactory implements
		DataStoreFactorySpi
{

	// GeoServer seems to call this several times so we should cache a
	// connection if the parameters are the same, I'm not sure this is entirely
	// correct but it keeps us from making several connections for the same data
	// store
	@Override
	public DataStore createDataStore(
			final Map<String, Serializable> params )
			throws IOException {
		return createNewDataStore(params);
	}

	@Override
	public DataStore createNewDataStore(
			final Map<String, Serializable> params )
			throws IOException {
		final GeoWaveGTMemDataStore dataStore;

		try {

			dataStore = new GeoWaveGTMemDataStore(
					GeoWavePluginConfig.getAuthorizationFactory(params),
					GeoWavePluginConfig.getAuthorizationURL(params),
					new GeoWavePluginConfig(
							params).getInstanceName());
		}
		catch (final Exception ex) {
			throw new IOException(
					"Error initializing datastore",
					ex);
		}

		return dataStore;
	}

	@Override
	public boolean canProcess(
			@SuppressWarnings("rawtypes")
			Map params ) {
		return (params.isEmpty() || params.size() == 2);
	}

	@Override
	public String getDisplayName() {
		return "GeoWave Mem Datastore";
	}

	@Override
	public String getDescription() {
		return "A datastore that uses the GeoWave API for spatial data persistence in the cloud";
	}

	@Override
	public Param[] getParametersInfo() {
		final List<Param> params = GeoWavePluginConfig.getAuthPluginParams();
		return params.toArray(new Param[params.size()]);
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

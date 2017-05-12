package mil.nga.giat.geowave.format.geotools.vector;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.Name;
import org.opengis.filter.Filter;

/**
 * This plugin is used for ingesting any GeoTools supported file data store from
 * a local file system directly into GeoWave as GeoTools' SimpleFeatures. It
 * supports the default configuration of spatial and spatial-temporal indices
 * and does NOT currently support the capability to stage intermediate data to
 * HDFS to be ingested using a map-reduce job.
 */
public class GeoToolsVectorDataStoreIngestPlugin implements
		LocalFileIngestPlugin<SimpleFeature>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoToolsVectorDataStoreIngestPlugin.class);
	private final static String PROPERTIES_EXTENSION = ".properties";

	private final RetypingVectorDataPlugin retypingPlugin;
	private final Filter filter;
	private final List<String> featureTypeNames;

	public GeoToolsVectorDataStoreIngestPlugin(
			final Filter filter ) {
		// by default inherit the types of the original file
		this(
				null,
				filter,
				new ArrayList<String>());
	}

	public GeoToolsVectorDataStoreIngestPlugin(
			final RetypingVectorDataPlugin retypingPlugin,
			final Filter filter,
			List<String> featureTypeNames ) {
		// this constructor can be used directly as an extension point for
		// retyping the original feature data, if the retyping plugin is null,
		// the data will be ingested as the original type
		this.retypingPlugin = retypingPlugin;
		this.filter = filter;
		this.featureTypeNames = featureTypeNames;
	}

	@Override
	public String[] getFileExtensionFilters() {
		return new String[] {};
	}

	@Override
	public void init(
			final File baseDirectory ) {}

	private static boolean isPropertiesFile(
			File file ) {
		return file.getName().toLowerCase(
				Locale.ENGLISH).endsWith(
				PROPERTIES_EXTENSION);
	}

	private static DataStore getDataStore(
			final File file )
			throws IOException {
		final Map<Object, Object> map = new HashMap<>();
		if (isPropertiesFile(file)) {
			try (FileInputStream fis = new FileInputStream(
					file)) {
				Properties prop = new Properties();
				prop.load(fis);
				map.putAll(prop);
				final DataStore dataStore = DataStoreFinder.getDataStore(map);
				return dataStore;
			}
		}
		map.put(
				"url",
				file.toURI().toURL());
		final DataStore dataStore = DataStoreFinder.getDataStore(map);
		return dataStore;
	}

	@Override
	public boolean supportsFile(
			final File file ) {
		DataStore dataStore = null;
		try {
			dataStore = getDataStore(file);
			dataStore.dispose();
		}
		catch (final Exception e) {
			LOGGER.info(
					"GeoTools was unable to read data source for file '" + file.getAbsolutePath() + "'",
					e);
		}
		return dataStore != null;
	}

	@Override
	public WritableDataAdapter<SimpleFeature>[] getDataAdapters(
			final String globalVisibility ) {
		return new WritableDataAdapter[] {};
	}

	@Override
	public CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveData(
			final File input,
			final Collection<ByteArrayId> primaryIndexIds,
			final String visibility ) {
		DataStore dataStore = null;
		try {
			dataStore = getDataStore(input);
		}
		catch (Exception e) {
			LOGGER.error(
					"Exception getting a datastore instance",
					e);
		}
		if (dataStore != null) {
			List<Name> names = null;
			try {
				names = dataStore.getNames();
			}
			catch (IOException e) {
				LOGGER.error(
						"Unable to get feature tpes from datastore '" + input.getAbsolutePath() + "'",
						e);
			}
			if (names == null) {
				LOGGER.error("Unable to get datatore name");
				return null;
			}
			final List<SimpleFeatureCollection> featureCollections = new ArrayList<SimpleFeatureCollection>();
			for (final Name name : names) {
				try {
					if (featureTypeNames != null && !featureTypeNames.isEmpty()
							&& !featureTypeNames.contains(name.getLocalPart())) {
						continue;
					}
					final SimpleFeatureSource source = dataStore.getFeatureSource(name);

					final SimpleFeatureCollection featureCollection;
					// we pass the filter in here so that the datastore may be
					// able to take advantage of the filter
					// but also send the filter along to be evaluated per
					// feature in case the filter is not respected by the
					// underlying store, we may want to consider relying on the
					// filtering being done by the store here
					if (filter != null) {
						featureCollection = source.getFeatures(filter);
					}
					else {
						featureCollection = source.getFeatures();
					}
					featureCollections.add(featureCollection);
				}
				catch (final Exception e) {
					LOGGER.error(
							"Unable to ingest data source for feature name '" + name + "'",
							e);
				}
			}
			return new SimpleFeatureGeoWaveWrapper(
					featureCollections,
					primaryIndexIds,
					visibility,
					dataStore,
					retypingPlugin,
					filter);
		}

		LOGGER.error("Unable to get a datastore instance, getDataStore returned null");
		return null;
	}

	@Override
	public PrimaryIndex[] getRequiredIndices() {
		return new PrimaryIndex[] {};
	}

	@Override
	public Class<? extends CommonIndexValue>[] getSupportedIndexableTypes() {
		return new Class[] {
			GeometryWrapper.class,
			Time.class
		};
	}
}

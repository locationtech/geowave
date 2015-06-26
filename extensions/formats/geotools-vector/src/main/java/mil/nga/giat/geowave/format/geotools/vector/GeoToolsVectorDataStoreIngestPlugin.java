package mil.nga.giat.geowave.format.geotools.vector;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;

import org.apache.log4j.Logger;
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
	private final static Logger LOGGER = Logger.getLogger(GeoToolsVectorDataStoreIngestPlugin.class);

	private final Index[] supportedIndices;
	private final RetypingVectorDataPlugin retypingPlugin;
	private final Filter filter;

	public GeoToolsVectorDataStoreIngestPlugin(
			final Filter filter ) {
		// by default inherit the types of the original file
		this(
				null,
				filter);
	}

	public GeoToolsVectorDataStoreIngestPlugin(
			final RetypingVectorDataPlugin retypingPlugin,
			final Filter filter ) {
		// this constructor can be used directly as an extension point for
		// retyping the original feature data, if the retyping plugin is null,
		// the data will be ingested as the original type
		supportedIndices = new Index[] {
			IndexType.SPATIAL_VECTOR.createDefaultIndex(),
			IndexType.SPATIAL_TEMPORAL_VECTOR.createDefaultIndex()
		};
		this.retypingPlugin = retypingPlugin;
		this.filter = filter;
	}

	@Override
	public String[] getFileExtensionFilters() {
		return new String[] {};
	}

	@Override
	public void init(
			final File baseDirectory ) {}

	@Override
	public boolean supportsFile(
			final File file ) {

		final Map<String, Object> map = new HashMap<String, Object>();
		try {
			map.put(
					"url",
					file.toURI().toURL());
			final DataStore dataStore = DataStoreFinder.getDataStore(map);
			return dataStore != null;
		}
		catch (final IOException e) {
			LOGGER.info(
					"GeoTools was unable to read data source for file '" + file.getAbsolutePath() + "'",
					e);
		}
		return false;
	}

	@Override
	public WritableDataAdapter<SimpleFeature>[] getDataAdapters(
			final String globalVisibility ) {
		return new WritableDataAdapter[] {};
	}

	@Override
	public CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveData(
			final File input,
			final ByteArrayId primaryIndexId,
			final String visibility ) {
		DataStore dataStore = null;
		List<Name> names = null;
		try {
			final Map<String, Object> map = new HashMap<String, Object>();
			map.put(
					"url",
					input.toURI().toURL());
			dataStore = DataStoreFinder.getDataStore(map);
			if (dataStore == null) {
				LOGGER.error("Unable to get a datastore instance, getDataStore returned null");
				return null;
			}

			names = dataStore.getNames();
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to ingest data source for file '" + input.getAbsolutePath() + "'",
					e);
		}
		if ((names == null) || (dataStore == null)) {
			LOGGER.error("Unable to get datatore name");
			return null;
		}
		final List<SimpleFeatureCollection> featureCollections = new ArrayList<SimpleFeatureCollection>();
		for (final Name name : names) {
			try {
				final SimpleFeatureSource source = dataStore.getFeatureSource(name);
				final SimpleFeatureCollection featureCollection = source.getFeatures();
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
				primaryIndexId,
				visibility,
				dataStore,
				retypingPlugin,
				filter);
	}

	@Override
	public Index[] getSupportedIndices() {
		return supportedIndices;
	}

	@Override
	public Index[] getRequiredIndices() {
		return new Index[] {};
	}
}

package mil.nga.giat.geowave.types.geotools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.ingest.GeoWaveData;
import mil.nga.giat.geowave.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;

import org.apache.log4j.Logger;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.Name;

/**
 * This plugin is used for ingesting any GeoTools supported file data store from
 * a local file system directly into GeoWave as GeoTools' SimpleFeatures. It
 * supports the default configuration of spatial and spatial-temporal indices
 * and does NOT currently support the capability to stage intermediate data to
 * HDFS to be ingested using a map-reduce job.
 */
public class GeoToolsDataStoreIngestPlugin implements
		LocalFileIngestPlugin<SimpleFeature>
{
	private final static Logger LOGGER = Logger.getLogger(GeoToolsDataStoreIngestPlugin.class);

	private final Index[] supportedIndices;

	public GeoToolsDataStoreIngestPlugin() {
		supportedIndices = new Index[] {
			IndexType.SPATIAL.createDefaultIndex(),
			IndexType.SPATIAL_TEMPORAL.createDefaultIndex()
		};
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

			names = dataStore.getNames();
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to ingest data source for file '" + input.getAbsolutePath() + "'",
					e);
		}
		if ((dataStore == null) || (names == null)) {
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
				dataStore);
	}

	@Override
	public Index[] getSupportedIndices() {
		return supportedIndices;
	}
}

package mil.nga.giat.geowave.format.geotools.vector;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfigurationSet;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.format.geotools.vector.RetypingVectorDataPlugin.RetypingVectorDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.geotools.data.DataStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

/**
 * This is a wrapper for a GeoTools SimpleFeatureCollection as a convenience to
 * ingest it into GeoWave by translating a list of SimpleFeatureCollection to a
 * closeable iterator of GeoWaveData
 */
public class SimpleFeatureGeoWaveWrapper implements
		CloseableIterator<GeoWaveData<SimpleFeature>>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SimpleFeatureGeoWaveWrapper.class);

	private class InternalIterator implements
			CloseableIterator<GeoWaveData<SimpleFeature>>
	{
		private final SimpleFeatureIterator featureIterator;
		private final WritableDataAdapter<SimpleFeature> dataAdapter;
		private RetypingVectorDataSource source = null;
		private final Filter filter;
		private SimpleFeatureBuilder builder = null;
		private GeoWaveData<SimpleFeature> currentData = null;

		public InternalIterator(
				final SimpleFeatureCollection featureCollection,
				final String visibility,
				final Filter filter ) {
			this.filter = filter;
			featureIterator = featureCollection.features();
			final SimpleFeatureType originalSchema = featureCollection.getSchema();
			SimpleFeatureType retypedSchema = SimpleFeatureUserDataConfigurationSet.configureType(originalSchema);
			if (retypingPlugin != null) {
				source = retypingPlugin.getRetypingSource(originalSchema);
				if (source != null) {
					retypedSchema = source.getRetypedSimpleFeatureType();
					builder = new SimpleFeatureBuilder(
							retypedSchema);
				}
			}
			if ((visibility == null) || visibility.isEmpty()) {
				dataAdapter = new FeatureDataAdapter(
						retypedSchema);
			}
			else {
				dataAdapter = new FeatureDataAdapter(
						retypedSchema,
						new GlobalVisibilityHandler<SimpleFeature, Object>(
								visibility));
			}
		}

		@Override
		public boolean hasNext() {
			if (currentData == null) {
				// return a flag indicating if we find more data that matches
				// the filter, essentially peeking and caching the result
				return nextData();
			}
			return true;
		}

		@Override
		public GeoWaveData<SimpleFeature> next() {
			if (currentData == null) {
				// get the next data that matches the filter
				nextData();
			}
			// return that data and set the current data to null
			final GeoWaveData<SimpleFeature> retVal = currentData;
			currentData = null;
			return retVal;
		}

		private synchronized boolean nextData() {
			SimpleFeature nextAcceptedFeature;
			do {
				if (!featureIterator.hasNext()) {
					return false;
				}
				nextAcceptedFeature = featureIterator.next();
				if (builder != null) {
					nextAcceptedFeature = source.getRetypedSimpleFeature(
							builder,
							nextAcceptedFeature);
				}
			}
			while (!filter.evaluate(nextAcceptedFeature));
			currentData = new GeoWaveData<SimpleFeature>(
					dataAdapter,
					primaryIndexIds,
					nextAcceptedFeature);
			return true;
		}

		@Override
		public void remove() {}

		@Override
		public void close()
				throws IOException {
			featureIterator.close();
		}

	}

	private final List<SimpleFeatureCollection> featureCollections;
	private final Collection<ByteArrayId> primaryIndexIds;
	private InternalIterator currentIterator = null;
	private final String visibility;
	private final DataStore dataStore;
	private final RetypingVectorDataPlugin retypingPlugin;
	private final Filter filter;

	public SimpleFeatureGeoWaveWrapper(
			final List<SimpleFeatureCollection> featureCollections,
			final Collection<ByteArrayId> primaryIndexIds,
			final String visibility,
			final DataStore dataStore,
			final RetypingVectorDataPlugin retypingPlugin,
			final Filter filter ) {
		this.featureCollections = featureCollections;
		this.visibility = visibility;
		this.primaryIndexIds = primaryIndexIds;
		this.dataStore = dataStore;
		this.retypingPlugin = retypingPlugin;
		this.filter = filter;
	}

	@Override
	public boolean hasNext() {
		if ((currentIterator == null) || !currentIterator.hasNext()) {
			// return a flag indicating if we find another iterator that hasNext
			return nextIterator();
		}
		// currentIterator has next
		return true;
	}

	private synchronized boolean nextIterator() {
		if (currentIterator != null) {
			try {
				currentIterator.close();
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Cannot close feature iterator",
						e);
			}
		}
		final Iterator<SimpleFeatureCollection> it = featureCollections.iterator();
		while (it.hasNext()) {
			final SimpleFeatureCollection collection = it.next();
			final InternalIterator featureIt = new InternalIterator(
					collection,
					visibility,
					filter);

			it.remove();
			if (!featureIt.hasNext()) {
				try {
					featureIt.close();
				}
				catch (final IOException e) {
					LOGGER.warn(
							"Cannot close feature iterator",
							e);
				}
			}
			else {
				currentIterator = featureIt;
				return true;
			}
		}
		return false;
	}

	@Override
	public GeoWaveData<SimpleFeature> next() {
		if ((currentIterator == null) || !currentIterator.hasNext()) {
			if (nextIterator()) {
				return currentIterator.next();
			}
			return null;
		}
		return currentIterator.next();
	}

	@Override
	public void remove() {
		if (currentIterator != null) {
			// this isn't really implemented anyway and should not be called
			currentIterator.remove();
		}
	}

	@Override
	public void close()
			throws IOException {
		if (currentIterator != null) {
			currentIterator.close();
		}
		if (dataStore != null) {
			dataStore.dispose();
		}
	}
}

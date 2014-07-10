package mil.nga.giat.geowave.types.geotools;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import mil.nga.giat.geowave.gt.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.ingest.GeoWaveData;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.data.field.GlobalVisibilityHandler;

import org.apache.log4j.Logger;
import org.geotools.data.DataStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.opengis.feature.simple.SimpleFeature;

/**
 * This is a wrapper for a GeoTools SimpleFeatureCollection as a convenience to
 * ingest it into GeoWave by translating a list of SimpleFeatureCollection to a
 * closeable iterator of GeoWaveData
 */
public class SimpleFeatureGeoWaveWrapper implements
		CloseableIterator<GeoWaveData<SimpleFeature>>
{
	private final static Logger LOGGER = Logger.getLogger(SimpleFeatureGeoWaveWrapper.class);

	private class InternalIterator implements
			CloseableIterator<GeoWaveData<SimpleFeature>>
	{
		private final SimpleFeatureIterator featureIterator;
		private final WritableDataAdapter<SimpleFeature> dataAdapter;

		public InternalIterator(
				final SimpleFeatureCollection featureCollection,
				final String visibility ) {
			featureIterator = featureCollection.features();
			if ((visibility == null) || visibility.isEmpty()) {
				dataAdapter = new FeatureDataAdapter(
						featureCollection.getSchema());
			}
			else {
				dataAdapter = new FeatureDataAdapter(
						featureCollection.getSchema(),
						new GlobalVisibilityHandler(
								visibility));
			}
		}

		@Override
		public boolean hasNext() {
			return featureIterator.hasNext();
		}

		@Override
		public GeoWaveData<SimpleFeature> next() {
			return new GeoWaveData<SimpleFeature>(
					dataAdapter,
					primaryIndexId,
					featureIterator.next());
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
	private final ByteArrayId primaryIndexId;
	private InternalIterator currentIterator = null;
	private final String visibility;
	private final DataStore dataStore;

	public SimpleFeatureGeoWaveWrapper(
			final List<SimpleFeatureCollection> featureCollections,
			final ByteArrayId primaryIndexId,
			final String visibility,
			final DataStore dataStore ) {
		this.featureCollections = featureCollections;
		this.visibility = visibility;
		this.primaryIndexId = primaryIndexId;
		this.dataStore = dataStore;
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
					visibility);

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

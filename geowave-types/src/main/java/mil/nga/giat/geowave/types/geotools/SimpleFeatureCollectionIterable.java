package mil.nga.giat.geowave.types.geotools;

import java.util.Iterator;

import mil.nga.giat.geowave.gt.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.ingest.GeoWaveData;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.data.field.GlobalVisibilityHandler;

import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.opengis.feature.simple.SimpleFeature;

/**
 * This is a wrapper for a GeoTools SimpleFeatureCollection as a convenience to
 * ingest it into GeoWave by translating it to a collection of GeoWaveData
 */
public class SimpleFeatureCollectionIterable implements
		Iterable<GeoWaveData<SimpleFeature>>
{
	private class InternalIterator implements
			Iterator<GeoWaveData<SimpleFeature>>
	{
		private final SimpleFeatureIterator featureIterator;

		public InternalIterator(
				final SimpleFeatureIterator featureIterator ) {
			this.featureIterator = featureIterator;
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

	}

	private final SimpleFeatureCollection featureCollection;
	private final ByteArrayId primaryIndexId;
	private final WritableDataAdapter<SimpleFeature> dataAdapter;

	public SimpleFeatureCollectionIterable(
			final SimpleFeatureCollection featureCollection,
			final ByteArrayId primaryIndexId,
			final String visibility ) {
		this.featureCollection = featureCollection;
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
		this.primaryIndexId = primaryIndexId;
	}

	@Override
	public Iterator<GeoWaveData<SimpleFeature>> iterator() {
		return new InternalIterator(
				featureCollection.features());
		// TODO these feature collection iterators are never closed
	}

}

package mil.nga.giat.geowave.ingest;

import java.util.Iterator;

import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.opengis.feature.simple.SimpleFeature;

public class SimpleFeatureCollectionIterable implements
		Iterable<SimpleFeature>
{
	private static class InternalIterator implements
			Iterator<SimpleFeature>
	{
		private final SimpleFeatureIterator featureIterator;

		public InternalIterator(
				SimpleFeatureIterator featureIterator ) {
			this.featureIterator = featureIterator;
		}

		@Override
		public boolean hasNext() {
			return featureIterator.hasNext();
		}

		@Override
		public SimpleFeature next() {
			return featureIterator.next();
		}

		@Override
		public void remove() {}

	}

	private final SimpleFeatureCollection featureCollection;

	public SimpleFeatureCollectionIterable(
			SimpleFeatureCollection featureCollection ) {
		this.featureCollection = featureCollection;
	}

	@Override
	public Iterator<SimpleFeature> iterator() {
		return new InternalIterator(
				featureCollection.features());
		// TODO these feature collection iterators are never closed
	}

}

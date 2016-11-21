package mil.nga.giat.geowave.adapter.vector.render;

import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.collection.BaseSimpleFeatureCollection;
import org.geotools.feature.collection.DelegateSimpleFeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.aol.cyclops.data.async.Queue;

public class AsyncQueueFeatureCollection extends
		BaseSimpleFeatureCollection
{
	private final Queue<SimpleFeature> asyncQueue;

	public AsyncQueueFeatureCollection(
			final SimpleFeatureType type,
			final Queue<SimpleFeature> asyncQueue ) {
		super(
				type);
		this.asyncQueue = asyncQueue;
	}

	@Override
	public SimpleFeatureIterator features() {
		return new DelegateSimpleFeatureIterator(
				asyncQueue.stream().iterator());
	}
}

package mil.nga.giat.geowave.adapter.vector.query;

import java.util.Iterator;
import java.util.Map.Entry;

import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveFeatureCollection;
import mil.nga.giat.geowave.adapter.vector.wms.accumulo.RenderedMaster;
import mil.nga.giat.geowave.core.index.PersistenceUtils;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * This wraps RenderedMaster results from the Iterator within a SimpleFeature
 * object as the GeoTools data store expects. A special purpose renderer within
 * GeoTools will have to take these features and composite it into the resultant
 * map.
 * 
 */
public class RenderIteratorWrapper implements
		Iterator<SimpleFeature>
{
	private final Iterator<Entry<Key, Value>> scannerIt;

	private SimpleFeature nextValue;
	private final SimpleFeatureType featureType;

	public RenderIteratorWrapper(
			final Iterator<Entry<Key, Value>> scannerIt ) {
		this.scannerIt = scannerIt;
		featureType = GeoWaveFeatureCollection.getDistributedRenderFeatureType();
		findNext();
	}

	private void findNext() {
		while (scannerIt.hasNext()) {
			final Entry<Key, Value> row = scannerIt.next();
			final SimpleFeature decodedValue = decodeRow(row);
			if (decodedValue != null) {
				nextValue = decodedValue;
				return;
			}
		}
		nextValue = null;
	}

	private SimpleFeature decodeRow(
			final Entry<Key, Value> row ) {
		final SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(
				featureType);
		featureBuilder.add(PersistenceUtils.fromBinary(
				row.getValue().get(),
				RenderedMaster.class));
		return featureBuilder.buildFeature(row.getKey().toString());
	}

	@Override
	public boolean hasNext() {
		return nextValue != null;
	}

	@Override
	public SimpleFeature next() {
		final SimpleFeature previousNext = nextValue;
		findNext();
		return previousNext;
	}

	@Override
	public void remove() {
		// TODO what should we do here considering the scanning iterator is
		// already past the current entry? it probably doesn't matter much as
		// this is not called in practice

		// scannerIt.remove();
	}

}

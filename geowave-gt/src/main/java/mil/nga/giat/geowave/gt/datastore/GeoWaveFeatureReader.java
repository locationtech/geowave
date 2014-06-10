package mil.nga.giat.geowave.gt.datastore;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import mil.nga.giat.geowave.gt.DistributableRenderer;
import mil.nga.giat.geowave.gt.GeoWaveDataStore;
import mil.nga.giat.geowave.gt.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.query.SpatialQuery;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import com.vividsolutions.jts.geom.Geometry;

/**
 * This class wraps a geotools data store as well as one for statistics (for
 * example to display Heatmaps) into a GeoTools FeatureReader for simple feature
 * data. It acts as a helper for GeoWave's GeoTools data store.
 * 
 */
public class GeoWaveFeatureReader implements
		FeatureReader<SimpleFeatureType, SimpleFeature>
{
	private final FeatureDataAdapter adapter;
	private final GeoWaveDataStore dataStore;
	private final GeoWaveDataStore statsDataStore;
	private final GeoWaveFeatureCollection featureCollection;

	public GeoWaveFeatureReader(
			final Query query,
			final GeoWaveDataStore dataStore,
			final GeoWaveDataStore statsDataStore,
			final FeatureDataAdapter adapter ) {
		this.adapter = adapter;
		this.dataStore = dataStore;
		this.statsDataStore = statsDataStore;
		featureCollection = new GeoWaveFeatureCollection(
				this,
				query);
	}

	@Override
	public void close()
			throws IOException {
		if (featureCollection.getOpenIterator() != null) {
			featureCollection.closeIterator(featureCollection.getOpenIterator());
		}
	}

	@Override
	public SimpleFeatureType getFeatureType() {
		if (featureCollection.isDistributedRenderQuery()) {
			return GeoWaveFeatureCollection.getDistributedRenderFeatureType();
		}
		return adapter.getType();
	}

	@Override
	public boolean hasNext()
			throws IOException {
		Iterator<SimpleFeature> it = featureCollection.getOpenIterator();
		if (it != null) {
			return it.hasNext();
		}
		it = featureCollection.openIterator();
		return it.hasNext();
	}

	@Override
	public SimpleFeature next()
			throws IOException,
			IllegalArgumentException,
			NoSuchElementException {
		Iterator<SimpleFeature> it = featureCollection.getOpenIterator();
		if (it != null) {
			return it.next();
		}
		it = featureCollection.openIterator();
		return it.next();
	}

	public CloseableIterator<SimpleFeature> getAllData(
			final Integer limit ) {
		if ((limit != null) && (limit >= 0)) {
			return dataStore.query(
					adapter,
					null,
					limit);
		}
		return dataStore.query(
				adapter,
				null);
	}

	public CloseableIterator<SimpleFeature> renderData(
			final Geometry jtsBounds,
			final Filter filter,
			final DistributableRenderer renderer ) {
		return dataStore.query(
				adapter,
				new SpatialQuery(
						jtsBounds.getGeometryN(0)),
				filter,
				renderer);
	}

	public CloseableIterator<SimpleFeature> getData(
			final Geometry jtsBounds,
			final int width,
			final int height,
			final double pixelSize,
			final Filter filter,
			final ReferencedEnvelope envelope ) {
		return dataStore.query(
				adapter,
				new SpatialQuery(
						jtsBounds.getGeometryN(0)),
				width,
				height,
				pixelSize,
				filter,
				envelope);
	}

	public CloseableIterator<SimpleFeature> getData(
			final Geometry jtsBounds,
			final Integer limit ) {
		if ((limit != null) && (limit >= 0)) {
			return dataStore.query(
					adapter,
					new SpatialQuery(
							jtsBounds),
					limit);
		}
		return dataStore.query(
				adapter,
				new SpatialQuery(
						jtsBounds));
	}

	@SuppressWarnings("unchecked")
	public CloseableIterator<SimpleFeature> getData(
			final Geometry jtsBounds,
			final int level,
			final String statsName ) {
		return (CloseableIterator<SimpleFeature>) statsDataStore.query(
				Arrays.asList(new ByteArrayId[] {
					new ByteArrayId(
							StringUtils.stringToBinary("l" + level + "_stats" + statsName))
				}),
				new SpatialQuery(
						jtsBounds));
	}

	public GeoWaveFeatureCollection getFeatureCollection() {
		return featureCollection;
	}

}

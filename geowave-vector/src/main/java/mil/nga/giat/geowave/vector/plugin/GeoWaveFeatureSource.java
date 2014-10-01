package mil.nga.giat.geowave.vector.plugin;

import java.awt.RenderingHints.Key;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.geotools.data.DataAccess;
import org.geotools.data.FeatureListener;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.QueryCapabilities;
import org.geotools.data.ResourceInfo;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.filter.Filter;
import org.opengis.geometry.BoundingBox;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * This is directly used by GeoWave's GeoTools DataStore to get a GeoTools data
 * reader or feature collection for a specific feature type (defined by a
 * GeoWave FeatureDataAdapter). This uses EPSG:4326 as the default CRS.
 * 
 */
public class GeoWaveFeatureSource implements
		SimpleFeatureSource
{
	private final GeoWaveGTDataStore store;
	private FeatureDataAdapter adapter = null;
	private final GeoWaveQueryCaps queryCaps = new GeoWaveQueryCaps();
	private final GeoWaveResourceInfo info;

	public GeoWaveFeatureSource(
			final GeoWaveGTDataStore store,
			final FeatureDataAdapter adapter ) {
		this.store = store;
		this.adapter = adapter;
		info = new GeoWaveResourceInfo(
				this);
	}

	public CoordinateReferenceSystem getCRS() {
		return GeoWaveGTDataStore.DEFAULT_CRS;
	}

	protected FeatureDataAdapter getStatsAdapter(
			final String typeName ) {
		return store.getStatsAdapter(typeName);
	}

	protected ReferencedEnvelope getBoundsInternal(
			final Query query )
			throws IOException {
		// TODO whats the most efficient way to get bounds in accumulo
		// for now just perform the query and iterate through the results

		final FeatureReader<SimpleFeatureType, SimpleFeature> reader = getReaderInternal(query);
		if (!reader.hasNext()) {
			return null;
		}
		double minx = Double.MAX_VALUE, maxx = -Double.MAX_VALUE, miny = Double.MAX_VALUE, maxy = -Double.MAX_VALUE;
		while (reader.hasNext()) {
			final BoundingBox bbox = reader.next().getBounds();
			minx = Math.min(
					bbox.getMinX(),
					minx);
			maxx = Math.max(
					bbox.getMaxX(),
					maxx);
			miny = Math.min(
					bbox.getMinY(),
					miny);
			maxy = Math.max(
					bbox.getMaxY(),
					maxy);

		}
		return new ReferencedEnvelope(
				minx,
				maxx,
				miny,
				maxy,
				GeoWaveGTDataStore.DEFAULT_CRS);
	}

	protected int getCountInternal(
			final Query query )
			throws IOException {
		// TODO whats the most efficient way to get bounds in accumulo
		// for now just iterate through results and count
		final FeatureReader<SimpleFeatureType, SimpleFeature> reader = getReaderInternal(query);
		int count = 0;
		while (reader.hasNext()) {
			reader.next();
			count++;
		}
		return count;
	}

	@Override
	public QueryCapabilities getQueryCapabilities() {
		return queryCaps;
	}

	@Override
	public ResourceInfo getInfo() {
		return info;
	}

	@Override
	public Name getName() {
		return adapter.getType().getName();
	}

	public SimpleFeatureType getFeatureType() {
		return adapter.getType();
	}

	// TODO we must implement a writer to support WFS-T
	protected GeoWaveFeatureReader getReaderInternal(
			final Query query ) {
		return new GeoWaveFeatureReader(
				query,
				store.dataStore,
				store.statsDataStore,
				adapter);
	}

	@Override
	public void addFeatureListener(
			final FeatureListener listener ) {
		store.addListener(
				this,
				listener);
	}

	@Override
	public void removeFeatureListener(
			final FeatureListener listener ) {
		store.removeListener(
				this,
				listener);
	}

	@Override
	public ReferencedEnvelope getBounds()
			throws IOException {
		return getBoundsInternal(null);
	}

	@Override
	public ReferencedEnvelope getBounds(
			final Query query )
			throws IOException {
		return getBoundsInternal(query);
	}

	@Override
	public int getCount(
			final Query query )
			throws IOException {
		return getCountInternal(query);
	}

	@Override
	public DataAccess<SimpleFeatureType, SimpleFeature> getDataStore() {
		return store;
	}

	@Override
	public SimpleFeatureType getSchema() {
		return adapter.getType();
	}

	@Override
	public Set<Key> getSupportedHints() {
		// TODO perhaps advertise hints that are used internally such as
		// decimation or density parameters
		return Collections.emptySet();
	}

	@Override
	public SimpleFeatureCollection getFeatures()
			throws IOException {
		return getReaderInternal(
				null).getFeatureCollection();
	}

	@Override
	public SimpleFeatureCollection getFeatures(
			final Filter filter )
			throws IOException {
		return getReaderInternal(
				new Query(
						getSchema().getTypeName(),
						filter)).getFeatureCollection();
	}

	@Override
	public SimpleFeatureCollection getFeatures(
			final Query query )
			throws IOException {
		return getReaderInternal(
				query).getFeatureCollection();
	}
}

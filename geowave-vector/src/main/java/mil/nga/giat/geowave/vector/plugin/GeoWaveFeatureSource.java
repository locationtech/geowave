package mil.nga.giat.geowave.vector.plugin;

import java.io.IOException;

import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.vector.plugin.transaction.GeoWaveEmptyTransaction;
import mil.nga.giat.geowave.vector.plugin.transaction.GeoWaveTransaction;

import org.geotools.data.AbstractFeatureLocking;
import org.geotools.data.DataStore;
import org.geotools.data.FeatureListener;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.QueryCapabilities;
import org.geotools.data.ResourceInfo;
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
public class GeoWaveFeatureSource extends
		AbstractFeatureLocking implements
		SimpleFeatureSource
{
	private final GeoWaveDataStoreComponents components;
	private final GeoWaveQueryCaps queryCaps = new GeoWaveQueryCaps();
	private final GeoWaveResourceInfo info;

	public GeoWaveFeatureSource(
			final GeoWaveGTDataStore store,
			final FeatureDataAdapter adapter ) {
		this.components = new GeoWaveDataStoreComponents(
				store.getDataStore(),
				store.getStatsDataStore(),
				store,
				adapter,
				store.getTransactionsAllocater());
		info = new GeoWaveResourceInfo(
				this);
	}

	public CoordinateReferenceSystem getCRS() {
		return GeoWaveGTDataStore.DEFAULT_CRS;
	}

	public GeoWaveDataStoreComponents getComponents() {
		return components;
	}

	protected FeatureDataAdapter getStatsAdapter(
			final String typeName ) {
		return components.getGTstore().getStatsAdapter(
				typeName);
	}

	protected ReferencedEnvelope getBoundsInternal(
			final Query query )
			throws IOException {
		// TODO whats the most efficient way to get bounds in accumulo
		// for now just perform the query and iterate through the results

		final FeatureReader<SimpleFeatureType, SimpleFeature> reader = new GeoWaveFeatureReader(
				query,
				new GeoWaveEmptyTransaction(
						this.components),
				this.components);
		if (!reader.hasNext()) {
			return new ReferencedEnvelope(
					-90.0,
					-180.0,
					90.0,
					180.0,
					GeoWaveGTDataStore.DEFAULT_CRS);
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
		final FeatureReader<SimpleFeatureType, SimpleFeature> reader = new GeoWaveFeatureReader(
				query,
				new GeoWaveEmptyTransaction(
						this.components),
				this.components);
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
		return components.getAdapter().getType().getName();
	}

	public SimpleFeatureType getFeatureType() {
		return components.getAdapter().getType();
	}

	protected GeoWaveFeatureReader getReaderInternal(
			final Query query,
			final GeoWaveTransaction transaction ) {
		return new GeoWaveFeatureReader(
				query,
				transaction,
				components);

	}

	protected GeoWaveFeatureWriter getWriterInternal(
			final GeoWaveTransaction transaction ) {

		return new GeoWaveFeatureWriter(
				components,
				transaction,
				null);
	}

	protected GeoWaveFeatureWriter getWriterInternal(
			final GeoWaveTransaction transaction,
			final Filter filter ) {
		String typeName = (String) components.getAdapter().getType().getTypeName();
		Query query = new Query(
				typeName,
				filter);
		final GeoWaveFeatureReader myReader = getReaderInternal(
				query,
				transaction);
		return new GeoWaveFeatureWriter(
				components,
				transaction,
				myReader);
	}

	@Override
	public ReferencedEnvelope getBounds()
			throws IOException {
		Query query = new Query(
				getSchema().getTypeName(),
				Filter.INCLUDE);
		return this.getBounds(query);
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
	public DataStore getDataStore() {
		return this.components.getGTstore();
	}

	@Override
	public SimpleFeatureType getSchema() {
		return this.components.getAdapter().getType();
	}

	@Override
	public void addFeatureListener(
			FeatureListener listener ) {
		this.components.getGTstore().getListenerManager().addFeatureListener(
				this,
				listener);
	}

	@Override
	public void removeFeatureListener(
			FeatureListener listener ) {
		this.components.getGTstore().getListenerManager().removeFeatureListener(
				this,
				listener);
	}
}

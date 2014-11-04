package mil.nga.giat.geowave.vector.plugin;

import java.io.IOException;
import java.util.Map;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.adapter.statistics.BoundingBoxDataStatistics;
import mil.nga.giat.geowave.store.adapter.statistics.CountDataStatistics;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatistics;
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
@SuppressWarnings("unchecked")
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

	@SuppressWarnings("rawtypes")
	protected ReferencedEnvelope getBoundsInternal(
			final Query query )
			throws IOException {
		double minx = -90.0, maxx = 90.0, miny = -180.0, maxy = 180.0;

		DataStatistics<SimpleFeature> bboxStats = null;
		if (query.getFilter().equals(
				Filter.INCLUDE)) {
			Map<ByteArrayId, DataStatistics<SimpleFeature>> stats = this.components.getDataStatistics(new GeoWaveEmptyTransaction(
					this.components));
			bboxStats = stats.get(BoundingBoxDataStatistics.STATS_ID);
		}
		if (bboxStats != null) {
			minx = ((BoundingBoxDataStatistics) bboxStats).getMinX();
			maxx = ((BoundingBoxDataStatistics) bboxStats).getMaxX();
			miny = ((BoundingBoxDataStatistics) bboxStats).getMinY();
			maxy = ((BoundingBoxDataStatistics) bboxStats).getMaxY();
		}
		else {

			final FeatureReader<SimpleFeatureType, SimpleFeature> reader = new GeoWaveFeatureReader(
					query,
					new GeoWaveEmptyTransaction(
							this.components),
					this.components);
			if (reader.hasNext()) {
				minx = 90.0;
				maxx = -90.0;
				miny = 180.0;
				maxy = -180.0;
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
			}
			reader.close();
		}
		return new ReferencedEnvelope(
				minx,
				maxx,
				miny,
				maxy,
				GeoWaveGTDataStore.DEFAULT_CRS);
	}

	@SuppressWarnings("rawtypes")
	protected int getCountInternal(
			final Query query )
			throws IOException {
		Map<ByteArrayId, DataStatistics<SimpleFeature>> stats = this.components.getDataStatistics(new GeoWaveEmptyTransaction(
				this.components));
		DataStatistics<SimpleFeature> countStats = stats.get(CountDataStatistics.STATS_ID);
		if (countStats != null && query.getFilter().equals(Filter.INCLUDE)) {
			return (int) ((CountDataStatistics) countStats).getCount();
		}
		else {
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
			reader.close();
			return count;
		}

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

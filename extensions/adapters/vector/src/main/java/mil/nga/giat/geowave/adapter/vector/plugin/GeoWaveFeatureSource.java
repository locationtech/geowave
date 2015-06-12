package mil.nga.giat.geowave.adapter.vector.plugin;

import java.io.IOException;
import java.util.Map;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.plugin.transaction.GeoWaveEmptyTransaction;
import mil.nga.giat.geowave.adapter.vector.plugin.transaction.GeoWaveTransactionState;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureBoundingBoxStatistics;
import mil.nga.giat.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.statistics.CountDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;

import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureStore;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.geometry.BoundingBox;

@SuppressWarnings("unchecked")
public class GeoWaveFeatureSource extends
		ContentFeatureStore
{
	private final GeoWaveDataStoreComponents components;

	public GeoWaveFeatureSource(
			final ContentEntry entry,
			final Query query,
			final FeatureDataAdapter adapter ) {
		super(
				entry,
				query);
		components = new GeoWaveDataStoreComponents(
				this.getDataStore().getDataStore(),
				this.getDataStore(),
				adapter,
				this.getDataStore().getTransactionsAllocater());
	}

	public GeoWaveDataStoreComponents getComponents() {
		return components;
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected ReferencedEnvelope getBoundsInternal(
			final Query query )
			throws IOException {
		double minx = -90.0, maxx = 90.0, miny = -180.0, maxy = 180.0;

		DataStatistics<SimpleFeature> bboxStats = null;
		if (query.getFilter().equals(
				Filter.INCLUDE)) {
			final Map<ByteArrayId, DataStatistics<SimpleFeature>> stats = components.getDataStatistics(new GeoWaveEmptyTransaction(
					components));
			bboxStats = stats.get(FeatureBoundingBoxStatistics.composeId(this.getFeatureType().getGeometryDescriptor().getLocalName()));
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
							components),
					components);
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
	@Override
	protected int getCountInternal(
			final Query query )
			throws IOException {
		final Map<ByteArrayId, DataStatistics<SimpleFeature>> stats = components.getDataStatistics(new GeoWaveEmptyTransaction(
				components));
		final DataStatistics<SimpleFeature> countStats = stats.get(CountDataStatistics.STATS_ID);
		if ((countStats != null) && query.getFilter().equals(
				Filter.INCLUDE)) {
			return (int) ((CountDataStatistics) countStats).getCount();
		}
		else {
			final FeatureReader<SimpleFeatureType, SimpleFeature> reader = new GeoWaveFeatureReader(
					query,
					new GeoWaveEmptyTransaction(
							components),
					components);
			int count = 0;
			while (reader.hasNext()) {
				reader.next();
				count++;
			}
			reader.close();
			return count;
		}

	}

	public SimpleFeatureType getFeatureType() {
		return components.getAdapter().getType();
	}

	@Override
	protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(
			final Query query )
			throws IOException {
		final GeoWaveTransactionState state = this.getDataStore().getMyTransactionState(
				transaction,
				this);
		return new GeoWaveFeatureReader(
				query,
				state.getGeoWaveTransaction(query.getTypeName()),
				components);
	}

	@Override
	protected FeatureWriter<SimpleFeatureType, SimpleFeature> getWriterInternal(
			Query query,
			int flags )
			throws IOException {
		final GeoWaveTransactionState state = this.getDataStore().getMyTransactionState(
				transaction,
				this);
		return new GeoWaveFeatureWriter(
				components,
				state.getGeoWaveTransaction(query.getTypeName()),
				(GeoWaveFeatureReader) getReaderInternal(query));
	}

	@Override
	protected SimpleFeatureType buildFeatureType()
			throws IOException {
		return components.getAdapter().getType();
	}

	@Override
	public GeoWaveGTDataStore getDataStore() {
		// type narrow this method to prevent a lot of casts resulting in more
		// readable code.
		return (GeoWaveGTDataStore) super.getDataStore();
	}

	@Override
	protected boolean canTransact() {
		// tell GeoTools that we natively handle this
		return true;
	}

	@Override
	protected boolean canLock() {
		// tell GeoTools that we natively handle this
		return true;
	}

	@Override
	protected void doLockInternal(
			String typeName,
			SimpleFeature feature )
			throws IOException {
		getDataStore().getLockingManager().lockFeatureID(
				typeName,
				feature.getID(),
				transaction,
				lock);
	}

	@Override
	protected void doUnlockInternal(
			String typeName,
			SimpleFeature feature )
			throws IOException {
		getDataStore().getLockingManager().unLockFeatureID(
				typeName,
				feature.getID(),
				transaction,
				lock);
	}

}

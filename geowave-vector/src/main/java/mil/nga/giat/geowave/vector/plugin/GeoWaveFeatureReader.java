package mil.nga.giat.geowave.vector.plugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.adapter.statistics.BoundingBoxDataStatistics;
import mil.nga.giat.geowave.store.query.BasicQuery;
import mil.nga.giat.geowave.store.query.SpatialQuery;
import mil.nga.giat.geowave.store.query.SpatialTemporalQuery;
import mil.nga.giat.geowave.store.query.TemporalConstraints;
import mil.nga.giat.geowave.store.query.TemporalQuery;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.vector.plugin.transaction.GeoWaveTransaction;
import mil.nga.giat.geowave.vector.wms.DistributableRenderer;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.filter.FidFilterImpl;
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

	private final GeoWaveDataStoreComponents components;
	private final GeoWaveFeatureCollection featureCollection;
	private final GeoWaveTransaction transaction;

	public GeoWaveFeatureReader(
			final Query query,
			final GeoWaveTransaction transaction,
			final GeoWaveDataStoreComponents components ) {
		this.components = components;
		this.transaction = transaction;
		featureCollection = new GeoWaveFeatureCollection(
				this,
				query);
	}

	public GeoWaveTransaction getTransaction() {
		return transaction;
	}

	public GeoWaveDataStoreComponents getComponents() {
		return components;
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
		return components.getAdapter().getType();
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

	public CloseableIterator<SimpleFeature> getNoData() {
		return new CloseableIterator.Empty<SimpleFeature>();
	}

	@SuppressWarnings({
		"unchecked",
		"rawtypes"
	})
	public CloseableIterator<SimpleFeature> getAllData(
			final Filter filter,
			final Integer limit ) {
		if (filter instanceof FidFilterImpl) {
			final List<SimpleFeature> retVal = new ArrayList<SimpleFeature>();
			final Set<String> fids = ((FidFilterImpl) filter).getIDs();
			for (final String fid : fids) {
				retVal.add((SimpleFeature) components.getDataStore().getEntry(
						components.getCurrentIndex(),
						new ByteArrayId(
								fid),
						components.getAdapter().getAdapterId(),
						transaction.composeAuthorizations()));
			}
			return new CloseableIterator.Wrapper(
					retVal.iterator());
		}
		if ((limit != null) && (limit >= 0)) {
			return components.getDataStore().query(
					components.getAdapter(),
					null,
					filter,
					limit,
					transaction.composeAuthorizations());
		}
		return interweaveTransaction(components.getDataStore().query(
				components.getAdapter(),
				(mil.nga.giat.geowave.store.query.Query) null,
				filter,
				(Integer) null,
				transaction.composeAuthorizations()));
	}

	public CloseableIterator<SimpleFeature> renderData(
			final Geometry jtsBounds,
			final TemporalConstraints timeBounds,
			final Filter filter,
			final DistributableRenderer renderer ) {
		return interweaveTransaction(components.getDataStore().query(
				components.getAdapter(),
				composeQuery(
						jtsBounds.getGeometryN(0),
						timeBounds),
				filter,
				renderer,
				transaction.composeAuthorizations()));
	}

	public CloseableIterator<SimpleFeature> getData(
			final Geometry jtsBounds,
			final TemporalConstraints timeBounds,
			final int width,
			final int height,
			final double pixelSize,
			final Filter filter,
			final ReferencedEnvelope envelope,
			final Integer limit ) {
		return interweaveTransaction(components.getDataStore().query(
				components.getAdapter(),
				composeQuery(
						jtsBounds.getGeometryN(0),
						timeBounds),
				width,
				height,
				pixelSize,
				filter,
				envelope,
				limit,
				transaction.composeAuthorizations()));
	}

	public CloseableIterator<SimpleFeature> getData(
			final Geometry jtsBounds,
			final TemporalConstraints timeBounds,
			final Integer limit ) {
		if ((limit != null) && (limit >= 0)) {
			return components.getDataStore().query(
					components.getAdapter(),
					composeQuery(
							jtsBounds,
							timeBounds),
					null,
					limit,
					transaction.composeAuthorizations());
		}
		return interweaveTransaction(components.getDataStore().query(
				components.getAdapter(),
				composeQuery(
						jtsBounds,
						timeBounds),
				(Filter) null,
				(Integer) null,
				transaction.composeAuthorizations()));
	}

	public CloseableIterator<SimpleFeature> getData(
			final Geometry jtsBounds,
			final TemporalConstraints timeBounds,
			final Filter filter,
			final Integer limit ) {
		if ((limit != null) && (limit >= 0)) {
			return interweaveTransaction(components.getDataStore().query(
					components.getAdapter(),
					composeQuery(
							jtsBounds,
							timeBounds),
					filter,
					limit));
		}
		return interweaveTransaction(components.getDataStore().query(
				components.getAdapter(),
				composeQuery(
						jtsBounds,
						timeBounds),
				filter,
				(Integer) null,
				transaction.composeAuthorizations()));
	}

	@SuppressWarnings("unchecked")
	public CloseableIterator<SimpleFeature> getData(
			final Geometry jtsBounds,
			final TemporalConstraints timeBounds,
			final int level,
			final String statsName ) {
		return interweaveTransaction((CloseableIterator<SimpleFeature>) components.getStatsDataStore().query(
				Arrays.asList(new ByteArrayId[] {
					new ByteArrayId(
							StringUtils.stringToBinary("l" + level + "_stats" + statsName))
				}),
				composeQuery(
						jtsBounds,
						timeBounds)));

	}

	public GeoWaveFeatureCollection getFeatureCollection() {
		return featureCollection;
	}

	private CloseableIterator<SimpleFeature> interweaveTransaction(
			final CloseableIterator<SimpleFeature> it ) {
		return transaction.interweaveTransaction(it);

	}

	private BasicQuery composeQuery(
			final Geometry jtsBounds,
			final TemporalConstraints timeBounds ) {
		if (jtsBounds == null) {
			if (timeBounds == null) {
				return new TemporalQuery(
						new TemporalConstraints());
			}
			else {
				return new TemporalQuery(
						timeBounds);
			}
		}
		else {
			if (timeBounds == null) {
				return new SpatialQuery(
						jtsBounds);
			}
			else {
				return new SpatialTemporalQuery(
						timeBounds,
						jtsBounds);
			}

		}
	}
}

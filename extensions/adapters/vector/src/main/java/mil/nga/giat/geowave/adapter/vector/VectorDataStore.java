package mil.nga.giat.geowave.adapter.vector;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import mil.nga.giat.geowave.adapter.vector.query.AccumuloCqlConstraintsQuery;
import mil.nga.giat.geowave.adapter.vector.query.DistributedRenderQuery;
import mil.nga.giat.geowave.adapter.vector.query.SpatialDecimationQuery;
import mil.nga.giat.geowave.adapter.vector.wms.DistributableRenderer;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.MemoryAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOptions;
import mil.nga.giat.geowave.datastore.accumulo.util.CloseableIteratorWrapper;

import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

import com.google.common.collect.Iterators;

/**
 * This is the most convenient way of using GeoWave as a SimpleFeature
 * persistence store backed by Accumulo. Beyond the basic functions of
 * AccumuloDataStore (general-purpose query and ingest of any data adapter type)
 * this class enables distributed rendering and decimation on SimpleFeature,
 * both with CQL filtering able to be applied to features on the tablet server.
 * 
 */
public class VectorDataStore extends
		AccumuloDataStore
{

	public VectorDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AccumuloOperations accumuloOperations ) {
		super(
				indexStore,
				adapterStore,
				statisticsStore,
				accumuloOperations);
	}

	public VectorDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AccumuloOperations accumuloOperations,
			final AccumuloOptions accumuloOptions ) {
		super(
				indexStore,
				adapterStore,
				statisticsStore,
				accumuloOperations,
				accumuloOptions);
	}

	public VectorDataStore(
			final AccumuloOperations accumuloOperations ) {
		super(
				accumuloOperations);
	}

	public VectorDataStore(
			final AccumuloOperations accumuloOperations,
			final AccumuloOptions accumuloOptions ) {
		super(
				accumuloOperations,
				accumuloOptions);
	}

	public CloseableIterator<Index> getIndices() {
		return indexStore.getIndices();
	}

	public DataStatisticsStore getStatsStore() {
		return this.statisticsStore;
	}

	@SuppressWarnings("unchecked")
	public CloseableIterator<SimpleFeature> query(
			final FeatureDataAdapter adapter,
			final Index index,
			final Query query,
			final Filter filter,
			final Integer limit,
			final String... authorizations ) {
		store(adapter);
		final List<ByteArrayId> adapterIds = Arrays.asList(new ByteArrayId[] {
			adapter.getAdapterId()
		});
		final AdapterStore adapterStore = new MemoryAdapterStore(
				new DataAdapter[] {
					adapter
				});

		if (query == null) {
			return (CloseableIterator<SimpleFeature>) new AccumuloCqlConstraintsQuery(
					adapterIds,
					index,
					filter,
					adapter,
					authorizations).query(
					accumuloOperations,
					adapterStore,
					limit);
		}
		else if (query.isSupported(index)) {
			// construct the query
			return (CloseableIterator<SimpleFeature>) new AccumuloCqlConstraintsQuery(
					adapterIds,
					index,
					query.getIndexConstraints(index.getIndexStrategy()),
					query.createFilters(index.getIndexModel()),
					filter,
					adapter,
					authorizations).query(
					accumuloOperations,
					adapterStore,
					limit);
		}

		return new CloseableIterator.Empty<SimpleFeature>();
	}

	public CloseableIterator<SimpleFeature> query(
			final FeatureDataAdapter adapter,
			final Index index,
			final Query query,
			final Filter filter,
			final DistributableRenderer distributedRenderer,
			final String... authorizations ) {

		// query the indices that are supported for this query object, and these
		// data adapter Ids
		final DistributedRenderQuery accumuloQuery;
		if (query == null) {
			accumuloQuery = new DistributedRenderQuery(
					Arrays.asList(new ByteArrayId[] {
						adapter.getAdapterId()
					}),
					index,
					filter,
					adapter,
					distributedRenderer,
					authorizations);
		}
		else if (query.isSupported(index)) {
			// construct the query
			accumuloQuery = new DistributedRenderQuery(
					Arrays.asList(new ByteArrayId[] {
						adapter.getAdapterId()
					}),
					index,
					query.getIndexConstraints(index.getIndexStrategy()),
					query.createFilters(index.getIndexModel()),
					filter,
					adapter,
					distributedRenderer,
					authorizations);
		}
		else {
			return (new CloseableIterator.Empty<SimpleFeature>());
		}

		final List<CloseableIterator<SimpleFeature>> results = accumuloQuery.queryDistributedRender(
				accumuloOperations,
				new MemoryAdapterStore(
						new DataAdapter[] {
							adapter
						}),
				distributedRenderer.isDecimationEnabled());
		return new CloseableIteratorWrapper<SimpleFeature>(
				new Closeable() {
					@Override
					public void close()
							throws IOException {
						for (final CloseableIterator<SimpleFeature> result : results) {
							result.close();
						}
					}
				},
				Iterators.concat(results.iterator()));
	}

	@SuppressWarnings("unchecked")
	public CloseableIterator<SimpleFeature> query(
			final FeatureDataAdapter adapter,
			final Index index,
			final Query query,
			final int width,
			final int height,
			final double pixelSize,
			final Filter filter,
			final ReferencedEnvelope envelope,
			final Integer limit,
			final String... authorizations ) {

		final SpatialDecimationQuery accumuloQuery = (query == null) ? new SpatialDecimationQuery(
				Arrays.asList(new ByteArrayId[] {
					adapter.getAdapterId()
				}),
				index,
				width,
				height,
				pixelSize,
				filter,
				adapter,
				envelope,
				authorizations) : new SpatialDecimationQuery(
				Arrays.asList(new ByteArrayId[] {
					adapter.getAdapterId()
				}),
				index,
				query.getIndexConstraints(index.getIndexStrategy()),
				query.createFilters(index.getIndexModel()),
				width,
				height,
				pixelSize,
				filter,
				adapter,
				envelope,
				authorizations);

		return (CloseableIterator<SimpleFeature>) accumuloQuery.query(
				accumuloOperations,
				new MemoryAdapterStore(
						new DataAdapter[] {
							adapter
						}),
				limit);

	}
}

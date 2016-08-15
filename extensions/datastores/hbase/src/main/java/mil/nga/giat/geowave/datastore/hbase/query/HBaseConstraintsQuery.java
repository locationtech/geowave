package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIterator.Wrapper;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.ConstraintsQuery;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

public class HBaseConstraintsQuery extends
		HBaseFilteredIndexQuery
{
	protected final ConstraintsQuery base;

	private final static Logger LOGGER = Logger.getLogger(HBaseConstraintsQuery.class);

	public HBaseConstraintsQuery(
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final Query query,
			final DedupeFilter clientDedupeFilter,
			final ScanCallback<?> scanCallback,
			final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final IndexMetaData[] indexMetaData,
			final DuplicateEntryCount duplicateCounts,
			final String[] authorizations ) {
		this(
				adapterIds,
				index,
				query != null ? query.getIndexConstraints(index.getIndexStrategy()) : null,
				query != null ? query.createFilters(index.getIndexModel()) : null,
				clientDedupeFilter,
				scanCallback,
				aggregation,
				indexMetaData,
				duplicateCounts,
				authorizations);
	}

	public HBaseConstraintsQuery(
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final List<MultiDimensionalNumericData> constraints,
			final List<QueryFilter> queryFilters,
			final DedupeFilter clientDedupeFilter,
			final ScanCallback<?> scanCallback,
			final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final IndexMetaData[] indexMetaData,
			final DuplicateEntryCount duplicateCounts,
			final String[] authorizations ) {

		super(
				adapterIds,
				index,
				scanCallback,
				authorizations);

		base = new ConstraintsQuery(
				constraints,
				aggregation,
				indexMetaData,
				index,
				queryFilters,
				clientDedupeFilter,
				duplicateCounts,
				this);

		if (isAggregation()) {
			// Because aggregations are done client-side make sure to set
			// the adapter ID here
			this.adapterIds = Collections.singletonList(aggregation.getLeft().getAdapterId());
		}
	}

	protected boolean isAggregation() {
		return base.isAggregation();
	}

	@Override
	protected List<ByteArrayRange> getRanges() {
		return base.getRanges();
	}

	@Override
	protected List<QueryFilter> getAllFiltersList() {
		final List<QueryFilter> filters = super.getAllFiltersList();
		for (final QueryFilter distributable : base.distributableFilters) {
			if (!filters.contains(distributable)) {
				filters.add(distributable);
			}
		}
		return filters;
	}

	@Override
	protected List<Filter> getDistributableFilter() {
		return new ArrayList<Filter>();
	}

	@Override
	public CloseableIterator<Object> query(
			final BasicHBaseOperations operations,
			final AdapterStore adapterStore,
			final Integer limit ) {
		final CloseableIterator<Object> it = super.query(
				operations,
				adapterStore,
				limit);

		if (isAggregation() && (it != null) && it.hasNext()) {
			// TODO implement aggregation as a co-processor on the server side,
			// but for now simply aggregate client-side here

			final Aggregation aggregationFunction = base.aggregation.getRight();
			synchronized (aggregationFunction) {

				aggregationFunction.clearResult();
				while (it.hasNext()) {
					final Object input = it.next();
					if (input != null) {
						aggregationFunction.aggregate(input);
					}
				}
				try {
					it.close();
				}
				catch (final IOException e) {
					LOGGER.warn(
							"Unable to close hbase scanner",
							e);
				}

				return new Wrapper(
						Iterators.singletonIterator(aggregationFunction.getResult()));
			}
		}
		// else {
		// return super.query(
		// operations,
		// adapterStore,
		// limit);
		// }

		return it;
	}
}

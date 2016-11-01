package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;
import com.google.protobuf.ByteString;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIterator.Wrapper;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.ConstraintsQuery;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;
import mil.nga.giat.geowave.core.store.query.aggregate.CommonIndexAggregation;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.query.generated.AggregationProtos;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;

public class HBaseConstraintsQuery extends
		HBaseFilteredIndexQuery
{
	protected final ConstraintsQuery base;

	private final static Logger LOGGER = Logger.getLogger(
			HBaseConstraintsQuery.class);

	public HBaseConstraintsQuery(
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final Query query,
			final DedupeFilter clientDedupeFilter,
			final ScanCallback<?> scanCallback,
			final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final IndexMetaData[] indexMetaData,
			final DuplicateEntryCount duplicateCounts,
			final Pair<List<String>, DataAdapter<?>> fieldIds,
			final String[] authorizations ) {
		this(
				adapterIds,
				index,
				query != null ? query.getIndexConstraints(
						index.getIndexStrategy()) : null,
				query != null ? query.createFilters(
						index.getIndexModel()) : null,
				clientDedupeFilter,
				scanCallback,
				aggregation,
				indexMetaData,
				duplicateCounts,
				fieldIds,
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
			final Pair<List<String>, DataAdapter<?>> fieldIds,
			final String[] authorizations ) {

		super(
				adapterIds,
				index,
				scanCallback,
				fieldIds,
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
			this.adapterIds = Collections.singletonList(
					aggregation.getLeft().getAdapterId());
		}
	}

	protected boolean isAggregation() {
		return base.isAggregation();
	}

	protected boolean isCommonIndexAggregation() {
		return base.isAggregation() && (base.aggregation.getRight() instanceof CommonIndexAggregation);
	}

	@Override
	protected List<ByteArrayRange> getRanges() {
		return base.getRanges();
	}

	@Override
	protected List<QueryFilter> getAllFiltersList() {
		final List<QueryFilter> filters = super.getAllFiltersList();

		// Since we have custom filters enabled, this list should only return
		// the client filters
		if ((options != null) && options.isEnableCustomFilters()) {
			return filters;
		}

		// Without custom filters, we need all the filters on the client side
		for (final QueryFilter distributable : base.distributableFilters) {
			if (!filters.contains(
					distributable)) {
				filters.add(
						distributable);
			}
		}
		return filters;
	}

	@Override
	protected List<DistributableQueryFilter> getDistributableFilters() {
		return base.distributableFilters;
	}

	@Override
	public CloseableIterator<Object> query(
			final BasicHBaseOperations operations,
			final AdapterStore adapterStore,
			final double[] maxResolutionSubsamplingPerDimension,
			final Integer limit ) {
		if (!isAggregation()) {
			return super.query(
					operations,
					adapterStore,
					maxResolutionSubsamplingPerDimension,
					limit);
		}

		// Aggregate without coprocessor
		if ((options == null) || !options.isEnableCoprocessors()) {
			final CloseableIterator<Object> it = super.internalQuery(
					operations,
					adapterStore,
					maxResolutionSubsamplingPerDimension,
					limit,
					!isCommonIndexAggregation());

			if ((it != null) && it.hasNext()) {
				final Aggregation aggregationFunction = base.aggregation.getRight();
				synchronized (aggregationFunction) {

					aggregationFunction.clearResult();
					while (it.hasNext()) {
						final Object input = it.next();
						if (input != null) {
							aggregationFunction.aggregate(
									input);
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
							Iterators.singletonIterator(
									aggregationFunction.getResult()));
				}
			}

			return new CloseableIterator.Empty();
		}

		// If we made it this far, we're using a coprocessor for aggregation
		return aggregateWithCoprocessor(
				operations,
				adapterStore,
				limit);
	}

	private CloseableIterator<Object> aggregateWithCoprocessor(
			final BasicHBaseOperations operations,
			final AdapterStore adapterStore,
			final Integer limit ) {
		final String tableName = StringUtils.stringFromBinary(
				index.getId().getBytes());
		Mergeable total = null;

		try {
			// Use the row count coprocessor
			if (options.isVerifyCoprocessors()) {
				operations.verifyCoprocessor(
						tableName,
						AggregationEndpoint.class.getName(),
						options.getCoprocessorJar());
			}

			final MultiRowRangeFilter multiFilter = getMultiFilter();

			final Aggregation aggregation = base.aggregation.getRight();

			final AggregationProtos.AggregationType.Builder aggregationBuilder = AggregationProtos.AggregationType
					.newBuilder();
			aggregationBuilder.setName(
					aggregation.getClass().getName());

			if (aggregation.getParameters() != null) {
				final byte[] paramBytes = PersistenceUtils.toBinary(
						aggregation.getParameters());
				aggregationBuilder.setParams(
						ByteString.copyFrom(
								paramBytes));
			}

			final AggregationProtos.AggregationRequest.Builder requestBuilder = AggregationProtos.AggregationRequest
					.newBuilder();
			requestBuilder.setAggregation(
					aggregationBuilder.build());

			final byte[] filterBytes = PersistenceUtils.toBinary(
					base.distributableFilters);
			final ByteString filterByteString = ByteString.copyFrom(
					filterBytes);

			requestBuilder.setFilter(
					filterByteString);

			requestBuilder.setModel(
					ByteString.copyFrom(
							PersistenceUtils.toBinary(
									index.getIndexModel())));

			requestBuilder.setRangefilter(
					ByteString.copyFrom(
							multiFilter.toByteArray()));
			if (base.aggregation.getLeft() != null) {
				final ByteArrayId adapterId = base.aggregation.getLeft().getAdapterId();
				if (isCommonIndexAggregation()) {
					final int binaryLength = 5 + adapterId.getBytes().length;

					final ByteBuffer buf = ByteBuffer.allocate(
							binaryLength);
					buf.put(
							(byte) 0);
					buf.putInt(
							adapterId.getBytes().length);
					buf.put(
							adapterId.getBytes());
					requestBuilder.setAdapter(
							ByteString.copyFrom(
									buf.array()));
				}
				else {
					final DataAdapter dataAdapter = adapterStore.getAdapter(
							adapterId);
					requestBuilder.setAdapter(
							ByteString.copyFrom(
									new byte[] {
										1
									}).concat(
											ByteString.copyFrom(
													PersistenceUtils.toBinary(
															dataAdapter))));
				}
			}
			final AggregationProtos.AggregationRequest request = requestBuilder.build();

			final Table table = operations.getTable(
					tableName);

			byte[] startRow = null;
			byte[] endRow = null;

			final List<ByteArrayRange> ranges = getRanges();
			if ((ranges != null) && !ranges.isEmpty()) {
				final ByteArrayRange aggRange = getRanges().get(
						0);
				startRow = aggRange.getStart().getBytes();
				endRow = aggRange.getEnd().getBytes();
			}

			final Map<byte[], ByteString> results = table.coprocessorService(
					AggregationProtos.AggregationService.class,
					startRow,
					endRow,
					new Batch.Call<AggregationProtos.AggregationService, ByteString>() {
						@Override
						public ByteString call(
								final AggregationProtos.AggregationService counter )
								throws IOException {
							final BlockingRpcCallback<AggregationProtos.AggregationResponse> rpcCallback = new BlockingRpcCallback<AggregationProtos.AggregationResponse>();
							counter.aggregate(
									null,
									request,
									rpcCallback);
							final AggregationProtos.AggregationResponse response = rpcCallback.get();
							return response.hasValue() ? response.getValue() : null;
						}
					});
			int regionCount = 0;
			for (final Map.Entry<byte[], ByteString> entry : results.entrySet()) {
				regionCount++;

				final ByteString value = entry.getValue();
				if ((value != null) && !value.isEmpty()) {
					final byte[] bvalue = value.toByteArray();
					final Mergeable mvalue = PersistenceUtils.fromBinary(
							bvalue,
							Mergeable.class);

					LOGGER.debug(
							"Value from region " + regionCount + " is " + mvalue);

					if (total == null) {
						total = mvalue;
					}
					else {
						total.merge(
								mvalue);
					}
				}
				else {
					LOGGER.debug(
							"Empty response for region " + regionCount);
				}
			}

		}
		catch (final Exception e) {
			LOGGER.error(
					"Error during aggregation." + e,
					e);
		}
		catch (final Throwable e) {
			LOGGER.error(
					"Error during aggregation." + e,
					e);
		}

		return new Wrapper(
				Iterators.singletonIterator(
						total));
	}

	protected MultiRowRangeFilter getMultiFilter() {
		// create the multi-row filter
		final List<RowRange> rowRanges = new ArrayList<RowRange>();

		final List<ByteArrayRange> ranges = base.getAllRanges();

		if ((ranges == null) || ranges.isEmpty()) {
			rowRanges.add(
					new RowRange(
							HConstants.EMPTY_BYTE_ARRAY,
							true,
							HConstants.EMPTY_BYTE_ARRAY,
							false));
		}
		else {
			for (final ByteArrayRange range : ranges) {
				if (range.getStart() != null) {
					final byte[] startRow = range.getStart().getBytes();
					byte[] stopRow;
					if (!range.isSingleValue()) {
						stopRow = HBaseUtils.getNextPrefix(
								range.getEnd().getBytes());
					}
					else {
						stopRow = HBaseUtils.getNextPrefix(
								range.getStart().getBytes());
					}

					final RowRange rowRange = new RowRange(
							startRow,
							true,
							stopRow,
							true);

					rowRanges.add(
							rowRange);
				}
			}
		}

		// Create the multi-range filter
		try {
			final MultiRowRangeFilter filter = new MultiRowRangeFilter(
					rowRanges);

			return filter;
		}
		catch (final IOException e) {
			LOGGER.error(
					"Error creating range filter." + e);
		}

		return null;
	}
}

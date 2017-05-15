package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;
import com.google.protobuf.ByteString;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRangesArray;
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
import mil.nga.giat.geowave.core.store.query.CoordinateRangeQueryFilter;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;
import mil.nga.giat.geowave.core.store.query.aggregate.CommonIndexAggregation;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.operations.HBaseBulkDeleteEndpoint;
import mil.nga.giat.geowave.datastore.hbase.operations.protobuf.HBaseBulkDeleteProtos;
import mil.nga.giat.geowave.datastore.hbase.operations.protobuf.HBaseBulkDeleteProtos.BulkDeleteResponse;
import mil.nga.giat.geowave.datastore.hbase.query.protobuf.AggregationProtos;

public class HBaseConstraintsDelete extends
		HBaseConstraintsQuery
{

	private final static Logger LOGGER = Logger.getLogger(HBaseConstraintsQuery.class);

	public HBaseConstraintsDelete(
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
		super(
				adapterIds,
				index,
				query != null ? query.getIndexConstraints(index.getIndexStrategy()) : null,
				query != null ? query.createFilters(index.getIndexModel()) : null,
				clientDedupeFilter,
				scanCallback,
				aggregation,
				indexMetaData,
				duplicateCounts,
				fieldIds,
				authorizations);
	}

	public HBaseConstraintsDelete(
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
				constraints,
				queryFilters,
				clientDedupeFilter,
				scanCallback,
				aggregation,
				indexMetaData,
				duplicateCounts,
				fieldIds,
				authorizations);

	}

	@Override
	public CloseableIterator<Object> aggregateWithCoprocessor(
			final BasicHBaseOperations operations,
			final AdapterStore adapterStore,
			final Integer limit ) {
		final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
		Mergeable total = null;

		try {
			// Use the row count coprocessor
			if (options.isVerifyCoprocessors()) {
				operations.verifyCoprocessor(
						tableName,
						HBaseBulkDeleteEndpoint.class.getName(),
						options.getCoprocessorJar());
			}

			final HBaseBulkDeleteProtos.BulkDeleteRequest.Builder requestBuilder = HBaseBulkDeleteProtos.BulkDeleteRequest
					.newBuilder();

			final HBaseBulkDeleteProtos.BulkDeleteRequest.BulkDeleteType type = HBaseBulkDeleteProtos.BulkDeleteRequest.BulkDeleteType.ROW;

			requestBuilder.setDeleteType(type);

			if ((base.distributableFilters != null) && !base.distributableFilters.isEmpty()) {
				final byte[] filterBytes = PersistenceUtils.toBinary(base.distributableFilters);
				final ByteString filterByteString = ByteString.copyFrom(filterBytes);

				requestBuilder.setFilter(filterByteString);
			}
			else {
				final List<MultiDimensionalCoordinateRangesArray> coords = base.getCoordinateRanges();
				if (!coords.isEmpty()) {
					final byte[] filterBytes = new HBaseNumericIndexStrategyFilter(
							index.getIndexStrategy(),
							coords.toArray(new MultiDimensionalCoordinateRangesArray[] {})).toByteArray();
					final ByteString filterByteString = ByteString.copyFrom(
							new byte[] {
								0
							}).concat(
							ByteString.copyFrom(filterBytes));

					requestBuilder.setNumericIndexStrategyFilter(filterByteString);
				}
			}
			requestBuilder.setModel(ByteString.copyFrom(PersistenceUtils.toBinary(index.getIndexModel())));

			final MultiRowRangeFilter multiFilter = getMultiRowRangeFilter(base.getAllRanges());
			if (multiFilter != null) {
				requestBuilder.setRangeFilter(ByteString.copyFrom(multiFilter.toByteArray()));
			}
			if (base.aggregation.getLeft() != null) {
				final ByteArrayId adapterId = base.aggregation.getLeft().getAdapterId();
				if (isCommonIndexAggregation()) {
					requestBuilder.setAdapterId(ByteString.copyFrom(adapterId.getBytes()));
				}
				else {
					final DataAdapter dataAdapter = adapterStore.getAdapter(adapterId);
					requestBuilder.setAdapter(ByteString.copyFrom(PersistenceUtils.toBinary(dataAdapter)));
				}
			}
			final HBaseBulkDeleteProtos.BulkDeleteRequest request = requestBuilder.build();

			final Table table = operations.getTable(tableName);

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
					HBaseBulkDeleteProtos.BulkDeleteService.class,
					startRow,
					endRow,
					new Batch.Call<HBaseBulkDeleteProtos.BulkDeleteService, ByteString>() {
						@Override
						public ByteString call(
								final HBaseBulkDeleteProtos.BulkDeleteService counter )
								throws IOException {
							final BlockingRpcCallback<HBaseBulkDeleteProtos.BulkDeleteResponse> rpcCallback = new BlockingRpcCallback<HBaseBulkDeleteProtos.BulkDeleteResponse>();
							counter.delete(
									null,
									request,
									rpcCallback);
							final BulkDeleteResponse response = rpcCallback.get();
							return response.hasRowsDeleted() ? response.getRowsDeleted() : null;
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

					LOGGER.debug("Value from region " + regionCount + " is " + mvalue);

					if (total == null) {
						total = mvalue;
					}
					else {
						total.merge(mvalue);
					}
				}
				else {
					LOGGER.debug("Empty response for region " + regionCount);
				}
			}

		}
		catch (final Exception e) {
			LOGGER.error(
					"Error during aggregation.",
					e);
		}
		catch (final Throwable e) {
			LOGGER.error(
					"Error during aggregation.",
					e);
		}

		return new Wrapper(
				total != null ? Iterators.singletonIterator(total) : Iterators.emptyIterator());
	}
}
